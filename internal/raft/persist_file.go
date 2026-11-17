package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// FilePersister 是 Persister 的文件实现：持久状态落在 --data 目录下的
// 三个文件里（格式见 docs/02-persistence.md）：
//
//   - raft.log：日志文件，追加写。每条 entry 编码为
//     [term varint][命令长度 varint][命令字节][CRC32 大端 4 字节]，
//     CRC 覆盖前面三段。写完调 fsync 再返回。
//   - meta：元数据文件（JSON：currentTerm、votedFor、lastApplied），
//     每次变化都原子写（临时文件 + fsync + 改名 + fsync 目录）。
//   - snapshot：快照文件（M5）。内容是状态机全量 + 快照覆盖的
//     最后一条日志的下标与任期，编码为
//     [index varint][term varint][数据长度 varint][数据字节][CRC32]，
//     CRC 覆盖前面四段；同样原子写。日志压缩后，raft.log 的首条目
//     变成快照位置的占位条目，基底下标只由“日志首条目 + 快照文件”
//     推导（见 deriveSnapshotBase），meta 里不存它。
//
// 写顺序是“先日志、后 meta”：Save 只有在两部分都落盘后才返回，
// 所以只要 Save 返回了，状态就是稳固的；中途崩溃只会丢掉“还没被
// 承认”的状态——日志里多出来的尾部（没来得及写 meta 的部分）
// 要么是未提交的、要么会在下次 Save 时被重新判定，绝不会丢掉
// 已经回复过的投票或日志。
//
// Save 的差分策略（不每条 entry 重写整个文件）：
//   - 新日志是已落盘日志的严格扩展 → 只追加新增的 entry；
//   - 日志被截断 / 冲突改写（新日志在某个下标处与已落盘的不同）→
//     整个日志文件重写（临时文件 + fsync + 改名 + fsync 目录；
//     原文件在改名前一直完好，崩溃不会留下一个写一半的日志文件）；
//   - meta 只有 term / votedFor / lastApplied 变了才重写。
//
// 比较是全量逐条比的（O(已落盘条数)，M2 的量级下可忽略；
// 日志变长后由 M5 的快照兜底，见规划）。
//
// 并发：Save 只在 raft.go 的 mu 保护下被调用（persistLocked），
// 但 FilePersister 自己也带一把锁，测试里直接调 Save 也是安全的。
type FilePersister struct {
	mu  sync.Mutex
	dir string

	// saved* 是“上次成功落盘”的状态快照，Save 靠它做差分。
	// savedKnown 为 false 表示从未成功保存过（全新目录）。
	savedLog         []LogEntry
	savedTerm        int
	savedVotedFor    string
	savedLastApplied int
	savedKnown       bool

	// logNeedsRewrite 置位时，下次 Save 不论差分结果都重写整个日志文件
	// （上次 Save 出错，磁盘上的日志内容已不可信）。
	logNeedsRewrite bool
}

const (
	// logFileName 是日志文件名，metaFileName 是元数据文件名，
	// snapshotFileName 是快照文件名。
	logFileName      = "raft.log"
	metaFileName     = "meta"
	snapshotFileName = "snapshot"
)

// metaFile 是 meta 文件的磁盘格式。
type metaFile struct {
	CurrentTerm int    `json:"currentTerm"`
	VotedFor    string `json:"votedFor"`
	LastApplied int    `json:"lastApplied"`
}

// NewFilePersister 建一个文件持久化器。dir 不存在时创建。
// 注意：这里不读文件，读由 Load() 做（NewRaft 调它）。
func NewFilePersister(dir string) (*FilePersister, error) {
	if dir == "" {
		return nil, errors.New("数据目录不能为空")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("创建数据目录 %q 失败：%w", dir, err)
	}
	return &FilePersister{dir: dir}, nil
}

// Save 把 s 落盘：先写日志（追加或重写），再写 meta（有变化才写）。
// 任何一步出错都返回错误，并且不更新对应的 saved*——
// 下次 Save 会重试；日志一旦出错就置 logNeedsRewrite，
// 下次直接重写整个日志文件（重写是幂等的，永远正确）。
func (f *FilePersister) Save(s PersistentState) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.saveLogLocked(s.Log); err != nil {
		f.logNeedsRewrite = true
		return err
	}
	if err := f.saveMetaLocked(s); err != nil {
		return err
	}
	return nil
}

// saveLogLocked 落盘日志部分。调用方持 f.mu。
func (f *FilePersister) saveLogLocked(log []LogEntry) error {
	// 差分：找新日志与已落盘日志的最长公共前缀。
	// prefix < len(savedLog) 说明日志被截断或冲突改写 → 重写整个文件；
	// 否则新日志是已落盘日志的扩展 → 只追加新增部分。
	prefix := 0
	for prefix < len(f.savedLog) && prefix < len(log) {
		if !logEntryEqual(f.savedLog[prefix], log[prefix]) {
			break
		}
		prefix++
	}
	if f.logNeedsRewrite || prefix < len(f.savedLog) {
		if err := f.rewriteLogFileLocked(log); err != nil {
			return err
		}
	} else if prefix < len(log) {
		if err := f.appendLogLocked(log[prefix:]); err != nil {
			return err
		}
	}
	f.savedLog = append([]LogEntry(nil), log...)
	f.logNeedsRewrite = false
	return nil
}

// appendLogLocked 把 entries 追加到日志文件尾并 fsync。
func (f *FilePersister) appendLogLocked(entries []LogEntry) error {
	var buf bytes.Buffer
	for _, e := range entries {
		buf.Write(encodeLogEntry(e))
	}
	path := filepath.Join(f.dir, logFileName)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("打开日志文件失败：%w", err)
	}
	if _, err := file.Write(buf.Bytes()); err != nil {
		_ = file.Close()
		return fmt.Errorf("追加日志失败：%w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("同步日志文件失败：%w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("关闭日志文件失败：%w", err)
	}
	return nil
}

// rewriteLogFileLocked 把整个日志重写一遍（临时文件 + fsync + 改名 +
// fsync 目录）。崩溃窗口里原文件一直完好：要么还是旧文件，
// 要么已经是完整的新文件，绝不会出现写一半的日志文件。
func (f *FilePersister) rewriteLogFileLocked(log []LogEntry) error {
	var buf bytes.Buffer
	for _, e := range log {
		buf.Write(encodeLogEntry(e))
	}
	path := filepath.Join(f.dir, logFileName)
	if err := atomicWriteFile(f.dir, logFileName, buf.Bytes()); err != nil {
		return fmt.Errorf("重写日志文件 %s 失败：%w", path, err)
	}
	return nil
}

// saveMetaLocked 落盘 meta 部分（有变化才写）。调用方持 f.mu。
func (f *FilePersister) saveMetaLocked(s PersistentState) error {
	if f.savedKnown && s.CurrentTerm == f.savedTerm && s.VotedFor == f.savedVotedFor && s.LastApplied == f.savedLastApplied {
		return nil
	}
	m := metaFile{CurrentTerm: s.CurrentTerm, VotedFor: s.VotedFor, LastApplied: s.LastApplied}
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("编码元数据失败：%w", err)
	}
	data = append(data, '\n')
	if err := atomicWriteFile(f.dir, metaFileName, data); err != nil {
		return fmt.Errorf("写元数据文件失败：%w", err)
	}
	f.savedTerm = s.CurrentTerm
	f.savedVotedFor = s.VotedFor
	f.savedLastApplied = s.LastApplied
	f.savedKnown = true
	return nil
}

// Load 读回持久状态。
//   - meta 文件不存在 → 全新节点，返回零值与 nil 错误。
//   - meta 文件损坏（不是合法 JSON）→ 返回错误，节点拒绝启动
//     （宁可 loud fail，也不带着猜错的状态跑）。
//   - 日志文件不存在 → 空日志（NewRaft 会补哨兵条目）。
//   - 日志尾部写一半 / CRC 不对 → 把文件截断到最后一条完整 entry 处，
//     丢弃写一半的尾巴，不报错（崩溃写一半是预期内的事）。
//   - 日志与 meta 自相矛盾（lastApplied 指向不存在的下标，
//     或第一条不是哨兵/占位条目）→ 返回错误，节点拒绝启动。
//   - 日志已压缩（首条目是占位条目）→ 读快照文件推导基底
//     （见 deriveSnapshotBase）：快照文件缺失、损坏、
//     任期对不上占位条目，都返回错误，节点拒绝启动；
//     只有快照文件是孤儿时才忽略它——日志未压缩（首条目是哨兵）
//     说明快照还没生效（写完快照文件、还没重写日志就崩了）。
//
// Load 成功后会把读到的状态记为 saved*，这样下次 Save 的差分是准的。
func (f *FilePersister) Load() (PersistentState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	meta, err := f.loadMetaLocked()
	if err != nil {
		return PersistentState{}, err
	}
	log, err := f.loadLogLocked()
	if err != nil {
		return PersistentState{}, err
	}
	snapIndex, snapTerm := 0, 0
	if len(log) > 0 {
		if len(log[0].Command) != 0 {
			return PersistentState{}, fmt.Errorf("日志文件第一条不是哨兵/占位条目（term=%d，命令长 %d），文件可能已损坏", log[0].Term, len(log[0].Command))
		}
		if log[0].Term > 0 {
			// 占位条目：日志已压缩，读快照文件推导基底。
			snap, ok, err := f.loadSnapshotLocked()
			if err != nil {
				return PersistentState{}, err
			}
			snapIndex, snapTerm, err = deriveSnapshotBase(log, snap, ok)
			if err != nil {
				return PersistentState{}, err
			}
		}
		// lastApplied 的上界：日志只覆盖到 snapIndex+len(log)-1。
		// 下界不校验：lastApplied < snapIndex 是合法的中间态——
		// 快照已落盘、但 apply 循环还没把 lastApplied 推过去就崩了，
		// NewRaft 会把它钳到 snapIndex（快照本身就是已应用的状态）。
		if meta.LastApplied > snapIndex+len(log)-1 {
			return PersistentState{}, fmt.Errorf("元数据与日志不一致：lastApplied=%d，但日志只覆盖到下标 %d", meta.LastApplied, snapIndex+len(log)-1)
		}
	}
	st := PersistentState{
		CurrentTerm:   meta.CurrentTerm,
		VotedFor:      meta.VotedFor,
		Log:           log,
		LastApplied:   meta.LastApplied,
		SnapshotIndex: snapIndex,
		SnapshotTerm:  snapTerm,
	}
	f.savedLog = append([]LogEntry(nil), log...)
	f.savedTerm = meta.CurrentTerm
	f.savedVotedFor = meta.VotedFor
	f.savedLastApplied = meta.LastApplied
	f.savedKnown = true
	f.logNeedsRewrite = false
	return st, nil
}

// SaveMeta 只写 meta 文件（日志文件不动，见 Persister.SaveMeta）。
func (f *FilePersister) SaveMeta(term int, votedFor string, lastApplied int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.saveMetaLocked(PersistentState{CurrentTerm: term, VotedFor: votedFor, LastApplied: lastApplied})
}

// SaveSnapshot 原子写一份快照文件（临时文件 + fsync + 改名 +
// fsync 目录）。调用方（TakeSnapshot / HandleInstallSnapshot）
// 保证先写快照文件、再重写日志文件——崩溃窗口的恢复规则见
// deriveSnapshotBase 与 Load 的注释。
func (f *FilePersister) SaveSnapshot(snap Snapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(snap.Data) == 0 {
		return fmt.Errorf("快照数据为空，拒绝保存（空字节在快照文件里表示‘没有快照’）")
	}
	if err := atomicWriteFile(f.dir, snapshotFileName, encodeSnapshot(snap)); err != nil {
		return fmt.Errorf("写快照文件失败：%w", err)
	}
	return nil
}

// LoadSnapshot 读回快照文件。文件不存在时返回 ok=false 与 nil 错误；
// 文件存在但损坏（长度不够、CRC 不对）时返回错误——
// 调用方（NewRaft）应拒绝启动，而不是静默用空状态跑。
func (f *FilePersister) LoadSnapshot() (Snapshot, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loadSnapshotLocked()
}

// loadSnapshotLocked 读快照文件。调用方持 f.mu。
func (f *FilePersister) loadSnapshotLocked() (Snapshot, bool, error) {
	path := filepath.Join(f.dir, snapshotFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Snapshot{}, false, nil // 从未拍过快照
		}
		return Snapshot{}, false, fmt.Errorf("读快照文件失败：%w", err)
	}
	snap, err := decodeSnapshot(data)
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("快照文件损坏：%w", err)
	}
	return snap, true, nil
}

// encodeSnapshot 把一份快照编成磁盘格式：
// [index varint][term varint][数据长度 varint][数据字节][CRC32 大端 4 字节]，
// CRC 覆盖前面四段（不含 CRC 自身）。与 encodeLogEntry 同风格。
func encodeSnapshot(snap Snapshot) []byte {
	var buf []byte
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(snap.LastIncludedIndex))
	buf = append(buf, tmp[:n]...)
	n = binary.PutUvarint(tmp[:], uint64(snap.LastIncludedTerm))
	buf = append(buf, tmp[:n]...)
	n = binary.PutUvarint(tmp[:], uint64(len(snap.Data)))
	buf = append(buf, tmp[:n]...)
	buf = append(buf, snap.Data...)
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(buf))
	buf = append(buf, crc[:]...)
	return buf
}

// decodeSnapshot 从 data 解一份快照。长度不够或 CRC 不对都返回错误
// （调用方按“文件损坏”处理：拒绝启动；原子写保证文件要么是旧的
// 完整内容、要么是新的完整内容，写一半的文件只可能来自磁盘损坏
// 或外部篡改，不像日志尾巴那样是崩溃的预期结果）。
func decodeSnapshot(data []byte) (Snapshot, error) {
	r := bytes.NewReader(data)
	index, err := binary.ReadUvarint(r)
	if err != nil {
		return Snapshot{}, io.ErrUnexpectedEOF
	}
	term, err := binary.ReadUvarint(r)
	if err != nil {
		return Snapshot{}, io.ErrUnexpectedEOF
	}
	dataLen, err := binary.ReadUvarint(r)
	if err != nil {
		return Snapshot{}, io.ErrUnexpectedEOF
	}
	consumed := len(data) - r.Len()
	remaining := len(data) - consumed
	if remaining < 4 || dataLen > uint64(remaining-4) {
		return Snapshot{}, io.ErrUnexpectedEOF
	}
	payload := make([]byte, dataLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Snapshot{}, io.ErrUnexpectedEOF
	}
	var crcBytes [4]byte
	if _, err := io.ReadFull(r, crcBytes[:]); err != nil {
		return Snapshot{}, io.ErrUnexpectedEOF
	}
	n := len(data) - r.Len()
	if n != len(data) {
		return Snapshot{}, fmt.Errorf("快照文件有多余的 %d 字节尾巴", len(data)-n)
	}
	if crc32.ChecksumIEEE(data[:n-4]) != binary.BigEndian.Uint32(crcBytes[:]) {
		return Snapshot{}, errors.New("CRC 校验失败")
	}
	return Snapshot{LastIncludedIndex: int(index), LastIncludedTerm: int(term), Data: payload}, nil
}

// loadMetaLocked 读 meta 文件。调用方持 f.mu。
func (f *FilePersister) loadMetaLocked() (metaFile, error) {
	path := filepath.Join(f.dir, metaFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return metaFile{}, nil // 全新节点
		}
		return metaFile{}, fmt.Errorf("读元数据文件失败：%w", err)
	}
	var m metaFile
	if err := json.Unmarshal(data, &m); err != nil {
		return metaFile{}, fmt.Errorf("元数据文件损坏（不是合法 JSON）：%w", err)
	}
	return m, nil
}

// loadLogLocked 逐条读日志文件。调用方持 f.mu。
// 遇到写一半的尾部（长度不够、CRC 不对）就把文件截断到最后一条
// 完整 entry 处，丢弃尾巴，不报错。
func (f *FilePersister) loadLogLocked() ([]LogEntry, error) {
	path := filepath.Join(f.dir, logFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // 全新节点
		}
		return nil, fmt.Errorf("读日志文件失败：%w", err)
	}
	var entries []LogEntry
	offset := 0
	for offset < len(data) {
		entry, n, err := decodeLogEntry(data[offset:])
		if err != nil {
			// 写一半的尾巴：截断文件，丢弃它。
			if terr := truncateFile(path, int64(offset)); terr != nil {
				return nil, fmt.Errorf("日志尾部损坏且截断失败（偏移 %d）：%w", offset, terr)
			}
			break
		}
		entries = append(entries, entry)
		offset += n
	}
	return entries, nil
}

// encodeLogEntry 把一条日志编成磁盘格式：
// [term varint][命令长度 varint][命令字节][CRC32 大端 4 字节]，
// CRC 覆盖前面三段（不含 CRC 自身）。
func encodeLogEntry(e LogEntry) []byte {
	var buf []byte
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(e.Term))
	buf = append(buf, tmp[:n]...)
	n = binary.PutUvarint(tmp[:], uint64(len(e.Command)))
	buf = append(buf, tmp[:n]...)
	buf = append(buf, e.Command...)
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(buf))
	buf = append(buf, crc[:]...)
	return buf
}

// decodeLogEntry 从 data 开头解一条日志，返回 entry、它占的字节数。
// data 不够一条完整 entry，或 CRC 不对，都返回错误
// （调用方按“写一半的尾巴”处理：截断丢弃）。
func decodeLogEntry(data []byte) (LogEntry, int, error) {
	r := bytes.NewReader(data)
	term, err := binary.ReadUvarint(r)
	if err != nil {
		return LogEntry{}, 0, io.ErrUnexpectedEOF
	}
	cmdLen, err := binary.ReadUvarint(r)
	if err != nil {
		return LogEntry{}, 0, io.ErrUnexpectedEOF
	}
	// 剩余字节必须够放命令 + 4 字节 CRC，否则就是写一半的尾巴。
	// 这里先不分配命令内存：长度不够直接报错。
	consumed := len(data) - r.Len()
	remaining := len(data) - consumed
	if remaining < 4 || cmdLen > uint64(remaining-4) {
		return LogEntry{}, 0, io.ErrUnexpectedEOF
	}
	cmd := make([]byte, cmdLen)
	if _, err := io.ReadFull(r, cmd); err != nil {
		return LogEntry{}, 0, io.ErrUnexpectedEOF
	}
	var crcBytes [4]byte
	if _, err := io.ReadFull(r, crcBytes[:]); err != nil {
		return LogEntry{}, 0, io.ErrUnexpectedEOF
	}
	n := len(data) - r.Len()
	if crc32.ChecksumIEEE(data[:n-4]) != binary.BigEndian.Uint32(crcBytes[:]) {
		return LogEntry{}, 0, errors.New("CRC 校验失败")
	}
	return LogEntry{Term: int(term), Command: cmd}, n, nil
}

// logEntryEqual 判断两条日志是否相同（Term 与命令字节都相同）。
func logEntryEqual(a, b LogEntry) bool {
	return a.Term == b.Term && bytes.Equal(a.Command, b.Command)
}

// atomicWriteFile 原子写一个小文件：写临时文件 → fsync 临时文件 →
// 改名覆盖目标 → fsync 目录。崩溃后目标文件要么是旧的完整内容，
// 要么是新的完整内容，绝不会是写一半的。
func atomicWriteFile(dir, name string, data []byte) error {
	tmpPath := filepath.Join(dir, name+".tmp")
	targetPath := filepath.Join(dir, name)
	file, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("创建临时文件失败：%w", err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("写临时文件失败：%w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("同步临时文件失败：%w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("关闭临时文件失败：%w", err)
	}
	if err := os.Rename(tmpPath, targetPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("改名覆盖 %s 失败：%w", name, err)
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("同步目录失败：%w", err)
	}
	return nil
}

// truncateFile 把 path 截断到 size 字节并 fsync。
func truncateFile(path string, size int64) error {
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := file.Truncate(size); err != nil {
		return err
	}
	return file.Sync()
}

// syncDir 把目录的元数据刷到磁盘（改名操作靠它才稳固）。
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

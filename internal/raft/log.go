// Package raft 实现 Raft 共识算法（论文图 2）。
package raft

import (
	"fmt"
	"sync"
)

// LogEntry 是 Raft 日志里的一条记录。Term 是写入这条记录时 leader 的任期，
// Command 是序列化后的状态机命令（见 internal/kv.Command 的 JSON）。
type LogEntry struct {
	Term    int    `json:"term"`
	Command []byte `json:"command"`
}

// Snapshot 是 Raft 快照：状态机在某个日志下标处的全量。
// LastIncludedIndex/LastIncludedTerm 是快照覆盖的最后一条日志
// （它本身也含在快照里）；Data 是状态机全量的序列化字节
// （KV 存的是 Store.Snapshot() 的 JSON）。
type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// PersistentState 是 Raft 论文要求“在回复 RPC 之前必须落盘”的全部状态，
// 再加一个 LastApplied：已应用到状态机的最大日志下标。
// 重启时只把 1..LastApplied 的日志重放到状态机——LastApplied 永远 ≤
// commitIndex，所以重放的都是已提交的条目，绝不会把未提交的尾部日志
// 喂给状态机；LastApplied 之后的已提交条目等新 leader 的心跳带
// LeaderCommit 过来后再由 apply 循环正常应用。
// M2 用文件实现（FilePersister，见 persist_file.go）；内存实现只留给测试。
//
// 快照（M5）：Log 不再从下标 0 开始——
// 快照之后只保留快照之后的条目，Log[0] 是快照位置的占位条目
// （Term = 快照任期、空命令），真正的基底是 SnapshotIndex：
// Log[i] 是绝对下标 SnapshotIndex+i 的条目。
// SnapshotIndex/SnapshotTerm 是 Load 推导出来的（见 deriveSnapshotBase），
// Save 不写它们——基底只由“日志首条目 + 快照文件”决定，
// 这样崩溃窗口里两者不可能各说各话（见 docs/02-persistence.md 快照一节）。
type PersistentState struct {
	CurrentTerm int        `json:"currentTerm"`
	VotedFor    string     `json:"votedFor"`
	Log         []LogEntry `json:"log"`
	LastApplied int        `json:"lastApplied"`
	// SnapshotIndex 是 Log[0] 的绝对下标（0 表示从未快照，
	// Log[0] 就是原来的哨兵条目）；SnapshotTerm 是该位置的任期。
	SnapshotIndex int `json:"snapshotIndex"`
	SnapshotTerm  int `json:"snapshotTerm"`
}

// Persister 是持久状态的存储抽象。Raft 核心只调这些方法，
// 不关心状态存在内存还是磁盘。
type Persister interface {
	// Save 原子地保存一份持久状态。调用方保证 Save 返回前状态已稳固
	// （文件实现里要写文件 + fsync；内存实现只是复制一份）。
	Save(PersistentState) error
	// Load 读回最近一次 Save 的状态。从未 Save 过时返回零值与 nil 错误。
	Load() (PersistentState, error)
	// SaveSnapshot 原子地保存一份快照（文件实现：临时文件 + fsync +
	// 改名 + fsync 目录；见 persist_file.go）。快照文件与日志文件
	// 是分开写的——崩溃窗口的恢复规则见 deriveSnapshotBase。
	SaveSnapshot(Snapshot) error
	// LoadSnapshot 读回快照文件。从未保存过快照时返回 ok=false 与 nil 错误；
	// 文件存在但损坏时返回错误（调用方应拒绝启动）。
	LoadSnapshot() (snap Snapshot, ok bool, err error)
	// SaveMeta 只持久化元数据（任期/投票/已应用位置），日志文件不动。
	// 供 apply 循环调用：应用推进只改 lastApplied，不改日志，
	// 没必要每次都把整条日志比一遍、拷一遍（M5 的 100k 测试里
	// 这是热路径：30 万次 Save 里约 20 万次来自 apply 循环）。
	// 安全性：lastApplied 永远 ≤ 已落盘日志长度
	// （条目落盘先于提交、提交先于应用），所以只写 meta 不会
	// 让 meta 指向日志里没有的条目；下次 Save（日志变了时）
	// 仍会把 meta 与日志一起写。日志变了的路径（Start、
	// AppendEntries 合入、快照）必须走 Save，不能走这里。
	SaveMeta(term int, votedFor string, lastApplied int) error
}

// deriveSnapshotBase 根据“日志首条目 + 快照文件”推导快照基底。
// 快照的触发（TakeSnapshot）与安装（HandleInstallSnapshot）都是
// “先写快照文件、再重写日志文件”，所以崩溃只可能留下以下几种状态，
// 恢复规则按“日志文件为准，快照文件为辅”定：
//
//   - 日志为空 → 全新节点，基底 0；快照文件即使存在也是孤儿，直接忽略。
//   - 首条目是哨兵（Term 0、空命令）→ 日志未压缩，基底 0；
//     快照文件即使存在也是孤儿（写完快照文件、还没重写日志就崩了），
//     直接忽略——下次快照会重写它。
//   - 首条目是占位条目（Term > 0、空命令）→ 日志已压缩：
//     必须有快照文件，且其 LastIncludedTerm == 首条目 Term，
//     否则返回错误——1..基底 的数据只在快照文件里，
//     没它就没法恢复，节点必须 loud fail，不能静默用空状态启动。
//   - 首条目带命令 → 日志文件损坏，返回错误。
func deriveSnapshotBase(log []LogEntry, snap Snapshot, hasSnap bool) (index, term int, err error) {
	if len(log) == 0 {
		return 0, 0, nil
	}
	first := log[0]
	if len(first.Command) != 0 {
		return 0, 0, fmt.Errorf("日志文件第一条不是哨兵/占位条目（term=%d，命令长 %d），文件可能已损坏", first.Term, len(first.Command))
	}
	if first.Term == 0 {
		return 0, 0, nil // 哨兵：日志未压缩
	}
	// 占位条目：日志已压缩，快照文件必须在且任期对得上。
	if !hasSnap {
		return 0, 0, fmt.Errorf("日志已压缩到占位条目（term=%d），但快照文件缺失，没法恢复", first.Term)
	}
	if snap.LastIncludedTerm != first.Term {
		return 0, 0, fmt.Errorf("快照文件与日志基底不一致：快照任期 %d，日志占位条目任期 %d", snap.LastIncludedTerm, first.Term)
	}
	return snap.LastIncludedIndex, snap.LastIncludedTerm, nil
}

// MemoryPersister 是 Persister 的内存实现，仅供测试使用：
// 进程退出即丢数据；生产路径（cmd/server）用 FilePersister。
type MemoryPersister struct {
	mu    sync.Mutex
	state PersistentState
	saved bool
	// snapshot 是最近一次 SaveSnapshot 的快照（文件实现里它在磁盘上，
	// 这里只放内存；Load 的基底推导与文件实现走同一条规则）。
	snapshot    Snapshot
	hasSnapshot bool
}

// Save 把状态复制一份存在内存里。
// 注意：这里不存 SnapshotIndex/SnapshotTerm——
// 它们由 Load 从“日志首条目 + 快照”推导出来（与文件实现同一条规则），
// 存两份只会让两者有机会各说各话。
func (m *MemoryPersister) Save(s PersistentState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = PersistentState{
		CurrentTerm: s.CurrentTerm,
		VotedFor:    s.VotedFor,
		Log:         append([]LogEntry(nil), s.Log...),
		LastApplied: s.LastApplied,
	}
	m.saved = true
	return nil
}

// Load 读回最近一次 Save 的状态（同样复制一份，防调用方改写内部数据）。
// 快照基底由 deriveSnapshotBase 从日志首条目与快照推导；
// 推导失败（日志已压缩但快照缺失/任期对不上）时返回错误。
func (m *MemoryPersister) Load() (PersistentState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.saved {
		return PersistentState{}, nil
	}
	st := PersistentState{
		CurrentTerm: m.state.CurrentTerm,
		VotedFor:    m.state.VotedFor,
		Log:         append([]LogEntry(nil), m.state.Log...),
		LastApplied: m.state.LastApplied,
	}
	index, term, err := deriveSnapshotBase(st.Log, m.snapshot, m.hasSnapshot)
	if err != nil {
		return PersistentState{}, err
	}
	st.SnapshotIndex = index
	st.SnapshotTerm = term
	return st, nil
}

// SaveMeta 只更新内存里的元数据字段（日志不动）。
func (m *MemoryPersister) SaveMeta(term int, votedFor string, lastApplied int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.CurrentTerm = term
	m.state.VotedFor = votedFor
	m.state.LastApplied = lastApplied
	m.saved = true
	return nil
}

// SaveSnapshot 把快照复制一份存在内存里。
func (m *MemoryPersister) SaveSnapshot(snap Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshot = Snapshot{
		LastIncludedIndex: snap.LastIncludedIndex,
		LastIncludedTerm:  snap.LastIncludedTerm,
		Data:              append([]byte(nil), snap.Data...),
	}
	m.hasSnapshot = true
	return nil
}

// LoadSnapshot 读回最近一次 SaveSnapshot 的快照（复制一份）。
func (m *MemoryPersister) LoadSnapshot() (Snapshot, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.hasSnapshot {
		return Snapshot{}, false, nil
	}
	return Snapshot{
		LastIncludedIndex: m.snapshot.LastIncludedIndex,
		LastIncludedTerm:  m.snapshot.LastIncludedTerm,
		Data:              append([]byte(nil), m.snapshot.Data...),
	}, true, nil
}

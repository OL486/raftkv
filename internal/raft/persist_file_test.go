package raft

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// 测试用的持久状态：哨兵 + 3 条用户日志，term 3、投过票、已应用 2 条。
func testPersistentState() PersistentState {
	return PersistentState{
		CurrentTerm: 3,
		VotedFor:    "node2",
		Log: []LogEntry{
			{Term: 0},
			{Term: 1, Command: []byte(`{"op":"put","key":"a","value":"1"}`)},
			{Term: 2, Command: []byte(`{"op":"put","key":"b","value":"2"}`)},
			{Term: 3, Command: []byte(`{"op":"put","key":"c","value":"3"}`)},
		},
		LastApplied: 2,
	}
}

func equalPersistentState(t *testing.T, want, got PersistentState) {
	t.Helper()
	if want.CurrentTerm != got.CurrentTerm || want.VotedFor != got.VotedFor || want.LastApplied != got.LastApplied {
		t.Fatalf("元数据不一致：want %+v，got %+v", want, got)
	}
	if len(want.Log) != len(got.Log) {
		t.Fatalf("日志长度不一致：want %d，got %d", len(want.Log), len(got.Log))
	}
	for i := range want.Log {
		if !logEntryEqual(want.Log[i], got.Log[i]) {
			t.Fatalf("日志 %d 不一致：want %+v，got %+v", i, want.Log[i], got.Log[i])
		}
	}
}

func TestFilePersister_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	fp, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := testPersistentState()
	if err := fp.Save(want); err != nil {
		t.Fatal(err)
	}
	// 换一个实例读（模拟重启后重新打开数据目录）。
	fp2, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fp2.Load()
	if err != nil {
		t.Fatal(err)
	}
	equalPersistentState(t, want, got)
}

func TestFilePersister_FreshDir(t *testing.T) {
	dir := t.TempDir()
	fp, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := fp.Load()
	if err != nil {
		t.Fatal(err)
	}
	if st.CurrentTerm != 0 || st.VotedFor != "" || st.LastApplied != 0 || len(st.Log) != 0 {
		t.Fatalf("全新目录应返回零值状态，got %+v", st)
	}
	// 数据目录里不应凭空多出文件（Load 不该写东西）。
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("Load 不应创建文件，目录里有 %d 个文件", len(entries))
	}
}

func TestFilePersister_MetaFile(t *testing.T) {
	dir := t.TempDir()
	fp, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := testPersistentState()
	if err := fp.Save(want); err != nil {
		t.Fatal(err)
	}
	// meta 文件是可读的 JSON，字段齐全。
	data, err := os.ReadFile(filepath.Join(dir, metaFileName))
	if err != nil {
		t.Fatal(err)
	}
	var m metaFile
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("meta 文件不是合法 JSON：%v（内容 %q）", err, data)
	}
	if m.CurrentTerm != 3 || m.VotedFor != "node2" || m.LastApplied != 2 {
		t.Fatalf("meta 内容不对：%+v", m)
	}
	// 临时文件不应残留。
	if _, err := os.Stat(filepath.Join(dir, metaFileName+".tmp")); !os.IsNotExist(err) {
		t.Fatal("meta.tmp 不应残留")
	}
}

func TestFilePersister_VotedForSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	fp, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	// 只投过票、还没日志的典型状态：term 5，投给 node3。
	if err := fp.Save(PersistentState{CurrentTerm: 5, VotedFor: "node3", Log: []LogEntry{{Term: 0}}}); err != nil {
		t.Fatal(err)
	}
	fp2, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := fp2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if st.CurrentTerm != 5 || st.VotedFor != "node3" {
		t.Fatalf("投票记录丢了：%+v", st)
	}
}

func TestFilePersister_LastAppliedOnly(t *testing.T) {
	dir := t.TempDir()
	fp, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := testPersistentState()
	if err := fp.Save(want); err != nil {
		t.Fatal(err)
	}
	logBefore, err := os.ReadFile(filepath.Join(dir, logFileName))
	if err != nil {
		t.Fatal(err)
	}
	// 只推进 lastApplied（日志不变）：meta 应更新，日志文件一个字节都不应变。
	want.LastApplied = 3
	if err := fp.Save(want); err != nil {
		t.Fatal(err)
	}
	logAfter, err := os.ReadFile(filepath.Join(dir, logFileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(logBefore, logAfter) {
		t.Fatal("只改 lastApplied 时日志文件不应被重写")
	}
	fp2, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fp2.Load()
	if err != nil {
		t.Fatal(err)
	}
	equalPersistentState(t, want, got)
}

func TestFilePersister_AppendOnly(t *testing.T) {
	dir := t.TempDir()
	fp, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	st := testPersistentState()
	if err := fp.Save(st); err != nil {
		t.Fatal(err)
	}
	infoBefore, err := os.Stat(filepath.Join(dir, logFileName))
	if err != nil {
		t.Fatal(err)
	}
	// 再追加一条：文件应恰好长出这一条的编码长度（追加路径，不是重写）。
	extra := LogEntry{Term: 3, Command: []byte(`{"op":"put","key":"d","value":"4"}`)}
	st.Log = append(append([]LogEntry(nil), st.Log...), extra)
	if err := fp.Save(st); err != nil {
		t.Fatal(err)
	}
	infoAfter, err := os.Stat(filepath.Join(dir, logFileName))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := infoAfter.Size()-infoBefore.Size(), int64(len(encodeLogEntry(extra))); got != want {
		t.Fatalf("追加应只长 %d 字节，实际长了 %d 字节（可能走了重写路径）", want, got)
	}
	fp2, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fp2.Load()
	if err != nil {
		t.Fatal(err)
	}
	equalPersistentState(t, st, got)
}

func TestFilePersister_TruncationRewrite(t *testing.T) {
	dir := t.TempDir()
	fp, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	st := testPersistentState()
	if err := fp.Save(st); err != nil {
		t.Fatal(err)
	}
	// 冲突改写：下标 2 处换成另一个任期的条目（模拟 follower 收到冲突日志被截断）。
	// 新日志：哨兵、{1,a}、{3,X}、{3,Y}——比原来短，且下标 2、3 都变了。
	conflict := PersistentState{
		CurrentTerm: 3,
		VotedFor:    "node2",
		Log: []LogEntry{
			{Term: 0},
			{Term: 1, Command: []byte(`{"op":"put","key":"a","value":"1"}`)},
			{Term: 3, Command: []byte(`{"op":"put","key":"x","value":"9"}`)},
			{Term: 3, Command: []byte(`{"op":"put","key":"y","value":"8"}`)},
		},
		LastApplied: 1,
	}
	if err := fp.Save(conflict); err != nil {
		t.Fatal(err)
	}
	fp2, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fp2.Load()
	if err != nil {
		t.Fatal(err)
	}
	equalPersistentState(t, conflict, got)

	// 再测纯截断（新日志是旧日志的严格前缀）：只剩哨兵 + 第一条。
	shorter := PersistentState{
		CurrentTerm: 3,
		VotedFor:    "node2",
		Log:         []LogEntry{{Term: 0}, {Term: 1, Command: []byte(`{"op":"put","key":"a","value":"1"}`)}},
		LastApplied: 1,
	}
	if err := fp.Save(shorter); err != nil {
		t.Fatal(err)
	}
	fp3, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err = fp3.Load()
	if err != nil {
		t.Fatal(err)
	}
	equalPersistentState(t, shorter, got)
}

func TestFilePersister_TornTail(t *testing.T) {
	// 两种“写一半”的尾巴：半条 entry、坏 CRC 的完整 entry。
	// 每种都应被 Load 截断丢弃，只留之前完整的 4 条。
	cases := map[string]func(dir string) error{
		"半条 entry": func(dir string) error {
			f, err := os.OpenFile(filepath.Join(dir, logFileName), os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				return err
			}
			defer f.Close()
			full := encodeLogEntry(LogEntry{Term: 3, Command: []byte(`{"op":"put","key":"z","value":"0"}`)})
			_, err = f.Write(full[:len(full)/2])
			return err
		},
		"坏 CRC": func(dir string) error {
			f, err := os.OpenFile(filepath.Join(dir, logFileName), os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				return err
			}
			defer f.Close()
			full := encodeLogEntry(LogEntry{Term: 3, Command: []byte(`{"op":"put","key":"z","value":"0"}`)})
			full[len(full)-1] ^= 0xFF // 弄坏 CRC 最后一个字节
			_, err = f.Write(full)
			return err
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			fp, err := NewFilePersister(dir)
			if err != nil {
				t.Fatal(err)
			}
			want := testPersistentState()
			if err := fp.Save(want); err != nil {
				t.Fatal(err)
			}
			sizeBefore, err := os.Stat(filepath.Join(dir, logFileName))
			if err != nil {
				t.Fatal(err)
			}
			if err := corrupt(dir); err != nil {
				t.Fatal(err)
			}
			fp2, err := NewFilePersister(dir)
			if err != nil {
				t.Fatal(err)
			}
			got, err := fp2.Load()
			if err != nil {
				t.Fatal(err)
			}
			equalPersistentState(t, want, got)
			// 文件应被截回损坏前的大小。
			sizeAfter, err := os.Stat(filepath.Join(dir, logFileName))
			if err != nil {
				t.Fatal(err)
			}
			if sizeAfter.Size() != sizeBefore.Size() {
				t.Fatalf("截断后文件应为 %d 字节，实际 %d 字节", sizeBefore.Size(), sizeAfter.Size())
			}
			// 再 Load 一次，结果应稳定（截断是幂等的）。
			fp3, err := NewFilePersister(dir)
			if err != nil {
				t.Fatal(err)
			}
			got, err = fp3.Load()
			if err != nil {
				t.Fatal(err)
			}
			equalPersistentState(t, want, got)
		})
	}

	// 极端情况：整个日志文件都是垃圾（模拟文件被写坏）。
	// Load 应截断到 0 条，而不是报错。
	t.Run("整个文件都是垃圾", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, logFileName), []byte{0xFF, 0xFF, 0x01, 0x02}, 0o644); err != nil {
			t.Fatal(err)
		}
		fp, err := NewFilePersister(dir)
		if err != nil {
			t.Fatal(err)
		}
		got, err := fp.Load()
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Log) != 0 {
			t.Fatalf("全是垃圾的日志应截断到 0 条，got %d 条", len(got.Log))
		}
	})
}

func TestFilePersister_CorruptMeta(t *testing.T) {
	dir := t.TempDir()
	fp, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := fp.Save(testPersistentState()); err != nil {
		t.Fatal(err)
	}
	// 把 meta 文件写坏：Load 必须报错，节点拒绝启动。
	if err := os.WriteFile(filepath.Join(dir, metaFileName), []byte("{不是 JSON"), 0o644); err != nil {
		t.Fatal(err)
	}
	fp2, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fp2.Load(); err == nil {
		t.Fatal("meta 文件损坏时 Load 应返回错误")
	}
}

func TestFilePersister_LogMetaMismatch(t *testing.T) {
	dir := t.TempDir()
	fp, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	st := testPersistentState()
	st.LastApplied = 3 // 3 条用户日志，全应用了
	if err := fp.Save(st); err != nil {
		t.Fatal(err)
	}
	// 手工把日志文件砍到只剩哨兵 + 1 条，但 meta 还说应用了 3 条：
	// 自相矛盾，Load 必须报错。
	logPath := filepath.Join(dir, logFileName)
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	// 只保留第一条 entry（哨兵）的编码。
	_, n, err := decodeLogEntry(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, data[:n], 0o644); err != nil {
		t.Fatal(err)
	}
	fp2, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fp2.Load(); err == nil {
		t.Fatal("lastApplied 指向不存在的日志下标时 Load 应返回错误")
	}
}

func TestFilePersister_BadDir(t *testing.T) {
	// 数据目录路径上已经是个文件：建目录应失败。
	f, err := os.CreateTemp(t.TempDir(), "not-a-dir")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, err := NewFilePersister(f.Name()); err == nil {
		t.Fatal("数据目录是个文件时应返回错误")
	}
	if _, err := NewFilePersister(""); err == nil {
		t.Fatal("数据目录为空时应返回错误")
	}
}

func TestFilePersister_SaveAfterFailedSave(t *testing.T) {
	// Save 出错后（这里用只读……root 下 chmod 拦不住，改用：先正常 Save，
	// 再把日志文件删了换成一个目录，让追加写失败），
	// 下次 Save 应走重写路径自我修复，而不是一直失败或写出重复 entry。
	dir := t.TempDir()
	fp, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := testPersistentState()
	if err := fp.Save(want); err != nil {
		t.Fatal(err)
	}
	// 把 raft.log 换成目录：追加新 entry 时写会失败。
	logPath := filepath.Join(dir, logFileName)
	if err := os.Remove(logPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(logPath, 0o755); err != nil {
		t.Fatal(err)
	}
	// 新状态多一条日志（逼 Save 走追加路径）+ lastApplied 推进。
	want.Log = append(append([]LogEntry(nil), want.Log...), LogEntry{Term: 3, Command: []byte(`{"op":"put","key":"d","value":"4"}`)})
	want.LastApplied = 3
	if err := fp.Save(want); err == nil {
		t.Fatal("日志文件被换成目录时 Save 应返回错误")
	}
	// 恢复成正常文件：下次 Save 应重写整个日志，自我修复。
	if err := os.Remove(logPath); err != nil {
		t.Fatal(err)
	}
	if err := fp.Save(want); err != nil {
		t.Fatalf("恢复后 Save 应成功（重写路径自我修复）：%v", err)
	}
	fp2, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fp2.Load()
	if err != nil {
		t.Fatal(err)
	}
	equalPersistentState(t, want, got)
}

func TestEncodeDecodeEntry(t *testing.T) {
	entries := []LogEntry{
		{Term: 0}, // 哨兵：空命令
		{Term: 1, Command: []byte("a")},
		{Term: 300, Command: []byte(`{"op":"put","key":"很长的键","value":"还有中文和 emoji 🎉"}`)},
		{Term: 1, Command: make([]byte, 10000)}, // 大命令（varint 长度多字节）
	}
	for i, e := range entries {
		enc := encodeLogEntry(e)
		dec, n, err := decodeLogEntry(enc)
		if err != nil {
			t.Fatalf("entry %d 解码失败：%v", i, err)
		}
		if n != len(enc) {
			t.Fatalf("entry %d 占 %d 字节，编码长 %d 字节", i, n, len(enc))
		}
		if !logEntryEqual(e, dec) {
			t.Fatalf("entry %d 编解码不一致：want %+v，got %+v", i, e, dec)
		}
	}
	// 截断的编码应报错。
	enc := encodeLogEntry(entries[1])
	if _, _, err := decodeLogEntry(enc[:len(enc)-1]); err == nil {
		t.Fatal("截断的 entry 应解码失败")
	}
	if _, _, err := decodeLogEntry(enc[:0]); err == nil {
		t.Fatal("空输入应解码失败")
	}
}

package raft

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// stubTransport 是什么都不做的 Transport（回调永远不调）：
// 单元测试里节点不 Run 或只测 handler，不需要真的发 RPC。
type stubTransport struct{}

func (s *stubTransport) RequestVote(addr string, args *RequestVoteArgs, cb func(*RequestVoteReply, error)) {
}

func (s *stubTransport) AppendEntries(addr string, args *AppendEntriesArgs, cb func(*AppendEntriesReply, error)) {
}

func (s *stubTransport) InstallSnapshot(addr string, args *InstallSnapshotArgs, cb func(*InstallSnapshotReply, error)) {
}

// newSnapshotTestRaft 建一个带指定 applyCh 的测试节点（不 Run）。
func newSnapshotTestRaft(t *testing.T, applyCh chan ApplyMsg) *Raft {
	t.Helper()
	addrs := map[string]string{"n1": "127.0.0.1:1", "n2": "127.0.0.1:1", "n3": "127.0.0.1:1"}
	r, err := NewRaft("n1", []string{"n1", "n2", "n3"}, addrs, &stubTransport{}, &MemoryPersister{}, applyCh)
	if err != nil {
		t.Fatalf("建测试节点失败：%v", err)
	}
	return r
}

// TestSnapshot_TakeSnapshot_CompactsLog：拍快照后日志被压缩到快照位置，
// 保留的条目数变小；旧下标的快照是空操作；超出 lastApplied 的快照报错。
func TestSnapshot_TakeSnapshot_CompactsLog(t *testing.T) {
	r := newSnapshotTestRaft(t, nil)
	// 日志：哨兵 + 3 条（任期 1, 1, 2），已应用 3 条。
	r.log = []LogEntry{{Term: 0}, {Term: 1, Command: []byte("A")}, {Term: 1, Command: []byte("B")}, {Term: 2, Command: []byte("C")}}
	r.lastApplied = 3
	r.commitIndex = 3

	if err := r.TakeSnapshot(2, []byte(`{"a":"1"}`)); err != nil {
		t.Fatalf("TakeSnapshot(2) 失败：%v", err)
	}
	if r.snapshotIndex != 2 || r.snapshotTerm != 1 {
		t.Fatalf("快照位置不对：index=%d term=%d，想要 (2, 1)", r.snapshotIndex, r.snapshotTerm)
	}
	if len(r.log) != 2 || r.log[0].Term != 1 || len(r.log[0].Command) != 0 {
		t.Fatalf("压缩后日志不对：%+v", r.log)
	}
	if r.log[1].Term != 2 || string(r.log[1].Command) != "C" {
		t.Fatalf("快照之后的条目应保留：%+v", r.log[1])
	}
	if got := r.Debug().LogLen; got != 1 {
		t.Fatalf("压缩后保留日志条数应为 1，实际 %d", got)
	}
	// 旧下标的快照是空操作。
	if err := r.TakeSnapshot(2, []byte(`{"a":"1"}`)); err != nil {
		t.Fatalf("重复快照应是空操作，实际报错：%v", err)
	}
	if err := r.TakeSnapshot(1, []byte(`{"a":"1"}`)); err != nil {
		t.Fatalf("更旧的快照应是空操作，实际报错：%v", err)
	}
	// 超出 lastApplied 的快照是调用方 bug，应报错。
	if err := r.TakeSnapshot(4, []byte(`{}`)); err == nil {
		t.Fatal("快照下标超出 lastApplied 应报错")
	}
	// 空数据拒绝保存。
	if err := r.TakeSnapshot(3, nil); err == nil {
		t.Fatal("空快照数据应报错")
	}
	// 持久化 round-trip：新实例 Load 回来的基底与日志一致。
	st, err := r.persister.Load()
	if err != nil {
		t.Fatalf("Load 失败：%v", err)
	}
	if st.SnapshotIndex != 2 || st.SnapshotTerm != 1 || len(st.Log) != 2 {
		t.Fatalf("Load 回来的状态不对：%+v", st)
	}
	snap, ok, err := r.persister.LoadSnapshot()
	if err != nil || !ok || string(snap.Data) != `{"a":"1"}` {
		t.Fatalf("LoadSnapshot 不对：%+v %v %v", snap, ok, err)
	}
}

// TestSnapshot_InstallSnapshot：安装新快照压缩日志、推进 commit；
// 旧快照直接忽略；低任期拒绝；保留/丢弃快照之后条目的两种情况。
func TestSnapshot_InstallSnapshot(t *testing.T) {
	newFollower := func() *Raft {
		r := newSnapshotTestRaft(t, nil)
		r.log = []LogEntry{{Term: 0}, {Term: 1, Command: []byte("A")}, {Term: 1, Command: []byte("B")}}
		r.lastApplied = 2
		r.commitIndex = 2
		return r
	}

	// (a) 安装比本地日志更远的快照：日志全丢，只剩占位条目。
	r := newFollower()
	reply := r.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term: 1, LeaderID: "n1", LastIncludedIndex: 5, LastIncludedTerm: 1, Data: []byte(`{"x":"1"}`),
	})
	if !reply.Success {
		t.Fatalf("安装新快照应成功：%+v", reply)
	}
	if r.snapshotIndex != 5 || r.snapshotTerm != 1 {
		t.Fatalf("快照位置不对：index=%d term=%d", r.snapshotIndex, r.snapshotTerm)
	}
	if len(r.log) != 1 || r.log[0].Term != 1 {
		t.Fatalf("安装后日志应只剩占位条目：%+v", r.log)
	}
	if r.commitIndex != 5 {
		t.Fatalf("commitIndex 应推进到 5，实际 %d", r.commitIndex)
	}
	if r.pendingSnap == nil || r.pendingSnap.index != 5 {
		t.Fatal("快照位置超出 lastApplied，应挂待投递快照")
	}
	// (b) 旧快照直接忽略（状态不动）。
	before := r.snapshotIndex
	reply = r.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term: 1, LeaderID: "n1", LastIncludedIndex: 3, LastIncludedTerm: 1, Data: []byte(`{}`),
	})
	if !reply.Success || r.snapshotIndex != before {
		t.Fatalf("旧快照应忽略：reply=%+v snapshotIndex=%d", reply, r.snapshotIndex)
	}
	// (c) 低任期拒绝。
	r2 := newFollower()
	r2.currentTerm = 5
	reply = r2.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term: 3, LeaderID: "n1", LastIncludedIndex: 9, LastIncludedTerm: 1, Data: []byte(`{}`),
	})
	if reply.Success || reply.Term != 5 {
		t.Fatalf("低任期快照应拒绝：%+v", reply)
	}
	// (d) 本地该下标任期与快照一致：保留快照之后的条目。
	r3 := newSnapshotTestRaft(t, nil)
	r3.log = []LogEntry{{Term: 0}, {Term: 1, Command: []byte("A")}, {Term: 2, Command: []byte("B")}, {Term: 2, Command: []byte("C")}}
	r3.lastApplied = 1
	r3.commitIndex = 1
	reply = r3.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term: 1, LeaderID: "n1", LastIncludedIndex: 2, LastIncludedTerm: 2, Data: []byte(`{}`),
	})
	if !reply.Success {
		t.Fatalf("安装快照应成功：%+v", reply)
	}
	if len(r3.log) != 2 || r3.log[1].Term != 2 || string(r3.log[1].Command) != "C" {
		t.Fatalf("任期一致时快照之后的条目应保留：%+v", r3.log)
	}
	// (e) 本地该下标任期与快照不一致：之后的全丢。
	r4 := newSnapshotTestRaft(t, nil)
	r4.log = []LogEntry{{Term: 0}, {Term: 1, Command: []byte("A")}, {Term: 1, Command: []byte("B")}, {Term: 1, Command: []byte("C")}}
	r4.lastApplied = 1
	r4.commitIndex = 1
	reply = r4.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term: 1, LeaderID: "n1", LastIncludedIndex: 2, LastIncludedTerm: 2, Data: []byte(`{}`),
	})
	if !reply.Success {
		t.Fatalf("安装快照应成功：%+v", reply)
	}
	if len(r4.log) != 1 {
		t.Fatalf("任期不一致时快照之后的条目应全丢：%+v", r4.log)
	}
	// (f) 空数据拒绝安装。
	r5 := newFollower()
	reply = r5.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term: 1, LeaderID: "n1", LastIncludedIndex: 9, LastIncludedTerm: 1,
	})
	if reply.Success {
		t.Fatal("空快照数据应拒绝安装")
	}
}

// TestSnapshot_PendingDelivery：安装的快照经 applyCh 按序投递——
// 先是快照恢复消息，再是快照之后的日志；lastApplied 跳到快照位置后再继续。
func TestSnapshot_PendingDelivery(t *testing.T) {
	applyCh := make(chan ApplyMsg, 100)
	r := newSnapshotTestRaft(t, applyCh)
	r.log = []LogEntry{{Term: 0}, {Term: 1, Command: []byte("A")}, {Term: 1, Command: []byte("B")}, {Term: 1, Command: []byte("C")}}
	r.lastApplied = 1
	r.commitIndex = 1
	r.Run()
	defer r.Stop()

	reply := r.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term: 100, LeaderID: "n1", LastIncludedIndex: 2, LastIncludedTerm: 1, Data: []byte(`{"a":"1"}`),
	})
	if !reply.Success {
		t.Fatalf("安装快照应成功：%+v", reply)
	}
	// 快照之后的条目也要提交：手动把 commitIndex 推到 3
	// （测试里直接改是合法的——本文件与 raft 同包）。
	r.mu.Lock()
	r.commitIndex = 3
	r.notifyApplyLocked()
	r.mu.Unlock()

	// 第一跳：快照恢复消息。
	select {
	case msg := <-applyCh:
		if msg.Snapshot == nil || msg.Index != 2 || string(msg.Snapshot) != `{"a":"1"}` {
			t.Fatalf("第一条应是快照恢复消息（下标 2），实际 %+v", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("5 秒内没收到快照恢复消息")
	}
	// 第二跳：快照之后的日志（下标 3，命令 C）。
	select {
	case msg := <-applyCh:
		if msg.Snapshot != nil || msg.Index != 3 || string(msg.Command) != "C" {
			t.Fatalf("第二条应是下标 3 的日志（命令 C），实际 %+v", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("5 秒内没收到快照之后的日志")
	}
	if got := r.LastApplied(); got != 3 {
		t.Fatalf("投递完 lastApplied 应为 3，实际 %d", got)
	}
}

// TestSnapshot_FilePersister：快照文件的 round-trip、损坏拒绝、
// 孤儿快照忽略、缺失快照 loud fail。
func TestSnapshot_FilePersister(t *testing.T) {
	compactedState := PersistentState{
		CurrentTerm: 3,
		VotedFor:    "node2",
		// 原始日志：哨兵 + {1,a} + {2,b} + {3,c}，在下标 2 处快照后
		// 只剩占位条目（任期 2）+ {3,c}。
		Log:           []LogEntry{{Term: 2}, {Term: 3, Command: []byte(`{"op":"put","key":"c","value":"3"}`)}},
		LastApplied:   2,
		SnapshotIndex: 2,
		SnapshotTerm:  2,
	}
	snap := Snapshot{LastIncludedIndex: 2, LastIncludedTerm: 2, Data: []byte(`{"a":"1","b":"2"}`)}

	// (a) round-trip：SaveSnapshot → Save（压缩后日志）→ 新实例 Load。
	dir := t.TempDir()
	fp, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := fp.SaveSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if err := fp.Save(compactedState); err != nil {
		t.Fatal(err)
	}
	fp2, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fp2.Load()
	if err != nil {
		t.Fatalf("Load 失败：%v", err)
	}
	if got.SnapshotIndex != 2 || got.SnapshotTerm != 2 || len(got.Log) != 2 || got.LastApplied != 2 {
		t.Fatalf("Load 回来的状态不对：%+v", got)
	}
	gotSnap, ok, err := fp2.LoadSnapshot()
	if err != nil || !ok || !bytes.Equal(gotSnap.Data, snap.Data) {
		t.Fatalf("LoadSnapshot 不对：%+v %v %v", gotSnap, ok, err)
	}

	// (b) 快照文件损坏 → Load 拒绝启动。
	badDir := t.TempDir()
	fpBad, _ := NewFilePersister(badDir)
	_ = fpBad.SaveSnapshot(snap)
	_ = fpBad.Save(compactedState)
	if err := os.WriteFile(filepath.Join(badDir, snapshotFileName), []byte("garbage-garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	fpBad2, _ := NewFilePersister(badDir)
	if _, err := fpBad2.Load(); err == nil {
		t.Fatal("快照文件损坏时 Load 应报错（拒绝启动）")
	}
	if _, _, err := fpBad2.LoadSnapshot(); err == nil {
		t.Fatal("快照文件损坏时 LoadSnapshot 应报错")
	}

	// (c) 孤儿快照：只写了快照文件、日志没压缩 → Load 忽略它，基底仍是 0。
	orphanDir := t.TempDir()
	fpOrphan, _ := NewFilePersister(orphanDir)
	if err := fpOrphan.SaveSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	full := testPersistentState()
	if err := fpOrphan.Save(full); err != nil {
		t.Fatal(err)
	}
	fpOrphan2, _ := NewFilePersister(orphanDir)
	gotOrphan, err := fpOrphan2.Load()
	if err != nil {
		t.Fatalf("孤儿快照不应让 Load 失败：%v", err)
	}
	if gotOrphan.SnapshotIndex != 0 {
		t.Fatalf("日志未压缩时快照基底应为 0，实际 %d", gotOrphan.SnapshotIndex)
	}

	// (d) 日志已压缩但快照文件缺失 → Load 拒绝启动（数据丢了，不能静默空跑）。
	missingDir := t.TempDir()
	fpMissing, _ := NewFilePersister(missingDir)
	_ = fpMissing.SaveSnapshot(snap)
	_ = fpMissing.Save(compactedState)
	if err := os.Remove(filepath.Join(missingDir, snapshotFileName)); err != nil {
		t.Fatal(err)
	}
	fpMissing2, _ := NewFilePersister(missingDir)
	if _, err := fpMissing2.Load(); err == nil {
		t.Fatal("日志已压缩但快照文件缺失时 Load 应报错")
	}

	// (e) 快照文件被截断一半 → LoadSnapshot 报错。
	tornDir := t.TempDir()
	fpTorn, _ := NewFilePersister(tornDir)
	_ = fpTorn.SaveSnapshot(snap)
	data, err := os.ReadFile(filepath.Join(tornDir, snapshotFileName))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tornDir, snapshotFileName), data[:len(data)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	fpTorn2, _ := NewFilePersister(tornDir)
	if _, _, err := fpTorn2.LoadSnapshot(); err == nil {
		t.Fatal("快照文件被截断时 LoadSnapshot 应报错")
	}
}

// TestSnapshot_MemoryPersister_RoundTrip：内存实现的快照 round-trip
// 与文件实现走同一条基底推导规则。
func TestSnapshot_MemoryPersister_RoundTrip(t *testing.T) {
	mp := &MemoryPersister{}
	snap := Snapshot{LastIncludedIndex: 2, LastIncludedTerm: 2, Data: []byte(`{"a":"1"}`)}
	if err := mp.SaveSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	st := PersistentState{
		CurrentTerm: 3,
		Log:         []LogEntry{{Term: 2}, {Term: 3, Command: []byte("C")}},
		LastApplied: 2,
	}
	if err := mp.Save(st); err != nil {
		t.Fatal(err)
	}
	got, err := mp.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.SnapshotIndex != 2 || got.SnapshotTerm != 2 {
		t.Fatalf("内存实现推导的基底不对：%+v", got)
	}
}

// captureTransport 记下最近一次 AppendEntries / InstallSnapshot 的
// 调用参数与回调，让测试手动触发回调（代替真的发 RPC）。
type captureTransport struct {
	mu             sync.Mutex
	lastAppendArgs *AppendEntriesArgs
	lastAppendCb   func(*AppendEntriesReply, error)
	lastSnapArgs   *InstallSnapshotArgs
	lastSnapCb     func(*InstallSnapshotReply, error)
}

func (c *captureTransport) RequestVote(addr string, args *RequestVoteArgs, cb func(*RequestVoteReply, error)) {
}

func (c *captureTransport) AppendEntries(addr string, args *AppendEntriesArgs, cb func(*AppendEntriesReply, error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastAppendArgs = args
	c.lastAppendCb = cb
}

func (c *captureTransport) InstallSnapshot(addr string, args *InstallSnapshotArgs, cb func(*InstallSnapshotReply, error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastSnapArgs = args
	c.lastSnapCb = cb
}

func newRaftWithTransport(t *testing.T, tr Transport) *Raft {
	t.Helper()
	addrs := map[string]string{"n1": "127.0.0.1:1", "n2": "127.0.0.1:1", "n3": "127.0.0.1:1"}
	r, err := NewRaft("n1", []string{"n1", "n2", "n3"}, addrs, tr, &MemoryPersister{}, nil)
	if err != nil {
		t.Fatalf("建测试节点失败：%v", err)
	}
	return r
}

// TestAppendEntries_ConflictIndex：拒绝时 follower 给快速回退提示，
// leader 按提示跳 nextIndex；提示落在快照里时改发 InstallSnapshot。
func TestAppendEntries_ConflictIndex(t *testing.T) {
	// ---- follower 侧：三种拒绝都带提示 ----
	follower := newSnapshotTestRaft(t, nil)
	follower.currentTerm = 1
	follower.log = []LogEntry{{Term: 0}, {Term: 1, Command: []byte("A")}, {Term: 1, Command: []byte("B")}}
	// (a) PrevLogIndex 超出日志：提示 = lastLogIndex+1。
	reply := follower.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: "n1", PrevLogIndex: 5, PrevLogTerm: 1})
	if reply.Success || reply.ConflictIndex != 3 {
		t.Fatalf("超出日志的拒绝应带 ConflictIndex=3，实际 %+v", reply)
	}
	// (b) 任期冲突：提示 = 冲突任期连续段的首下标（不是逐格回退）。
	follower.log = []LogEntry{{Term: 0}, {Term: 1, Command: []byte("A")}, {Term: 2, Command: []byte("B")}, {Term: 2, Command: []byte("C")}}
	reply = follower.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: "n1", PrevLogIndex: 3, PrevLogTerm: 1})
	if reply.Success || reply.ConflictIndex != 2 {
		t.Fatalf("任期冲突的拒绝应带 ConflictIndex=2，实际 %+v", reply)
	}
	// (c) 快照任期对不上：提示 = 快照位置（leader 看到它改发快照）。
	follower.snapshotIndex = 5
	follower.snapshotTerm = 2
	follower.log = []LogEntry{{Term: 2}}
	reply = follower.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: "n1", PrevLogIndex: 5, PrevLogTerm: 1})
	if reply.Success || reply.ConflictIndex != 5 {
		t.Fatalf("快照任期冲突应带 ConflictIndex=5，实际 %+v", reply)
	}
	// (d) 一致的请求照常成功，不带提示。
	follower.snapshotIndex = 0
	follower.snapshotTerm = 0
	follower.log = []LogEntry{{Term: 0}, {Term: 1, Command: []byte("A")}}
	reply = follower.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: "n1", PrevLogIndex: 1, PrevLogTerm: 1})
	if !reply.Success || reply.ConflictIndex != 0 {
		t.Fatalf("一致的请求应成功且无提示，实际 %+v", reply)
	}

	// ---- leader 侧：按提示跳 nextIndex ----
	ct := &captureTransport{}
	leader := newRaftWithTransport(t, ct)
	leader.state = Leader
	leader.currentTerm = 1
	leader.snapshotIndex = 5000
	leader.snapshotTerm = 1
	// 日志：占位条目（下标 5000）+ 4500 条，覆盖到 9500。
	leader.log = make([]LogEntry, 4501)
	leader.log[0] = LogEntry{Term: 1}
	for i := 1; i < len(leader.log); i++ {
		leader.log[i] = LogEntry{Term: 1, Command: []byte("x")}
	}
	leader.nextIndex = map[string]int{"n2": 9501, "n3": 9501}
	leader.matchIndex = map[string]int{"n2": 0, "n3": 0}
	defer leader.Stop()

	leader.replicateOnce("n2", 1)
	ct.mu.Lock()
	args, cb := ct.lastAppendArgs, ct.lastAppendCb
	ct.mu.Unlock()
	if args == nil || cb == nil {
		t.Fatal("replicateOnce 没发 AppendEntries")
	}
	if args.PrevLogIndex != 9500 {
		t.Fatalf("PrevLogIndex 应为 9500，实际 %d", args.PrevLogIndex)
	}
	// follower 回“我的日志只有 0 条”：nextIndex 直接跳到快照位置，
	// 而不是从 9501 一格一格退 4500 次。
	cb(&AppendEntriesReply{Term: 1, Success: false, ConflictIndex: 1}, nil)
	if got := leader.nextIndex["n2"]; got != 5000 {
		t.Fatalf("收到 ConflictIndex=1 后 nextIndex 应跳到快照位置 5000，实际 %d", got)
	}
	// 再来一次：nextIndex 已掉进快照，改发 InstallSnapshot。
	leader.replicateOnce("n2", 1)
	ct.mu.Lock()
	snapArgs, snapCb := ct.lastSnapArgs, ct.lastSnapCb
	ct.mu.Unlock()
	if snapArgs == nil || snapCb == nil {
		t.Fatal("nextIndex 掉进快照后应改发 InstallSnapshot")
	}
	if snapArgs.LastIncludedIndex != 5000 {
		t.Fatalf("快照下标应为 5000，实际 %d", snapArgs.LastIncludedIndex)
	}
	snapCb(&InstallSnapshotReply{Term: 1, Success: true}, nil)
	if got := leader.nextIndex["n2"]; got != 5001 {
		t.Fatalf("快照成功后 nextIndex 应为 5001，实际 %d", got)
	}
	if got := leader.matchIndex["n2"]; got != 5000 {
		t.Fatalf("快照成功后 matchIndex 应为 5000，实际 %d", got)
	}
}

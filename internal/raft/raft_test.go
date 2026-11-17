package raft

import (
	"testing"
)

// newTestRaft 建一个不启动 goroutine 的节点（不调 Run），
// 单元测试直接调它的 handler 方法，不走网络。
func newTestRaft(id string, peers []string) *Raft {
	addrs := make(map[string]string, len(peers))
	for _, p := range peers {
		addrs[p] = "127.0.0.1:1" // 占位地址，单元测试不拨号
	}
	return NewRaft(id, peers, addrs, NewHTTPTransport(), &MemoryPersister{}, nil)
}

func TestHandleRequestVote_Rules(t *testing.T) {
	peers := []string{"n1", "n2", "n3"}
	// follower：任期 1，日志 [哨兵, {1,A}, {1,B}]，最后日志 (index 2, term 1)。
	r := newTestRaft("n1", peers)
	r.currentTerm = 1
	r.log = []LogEntry{{Term: 0}, {Term: 1, Command: []byte("A")}, {Term: 1, Command: []byte("B")}}

	t.Run("任期更小的请求直接拒绝并返回自己的任期", func(t *testing.T) {
		reply := r.HandleRequestVote(&RequestVoteArgs{Term: 0, CandidateID: "n2", LastLogIndex: 2, LastLogTerm: 1})
		if reply.VoteGranted || reply.Term != 1 {
			t.Fatalf("got %+v, want VoteGranted=false Term=1", reply)
		}
	})

	t.Run("日志更旧的候选人拿不到票（lastLogTerm 更小）", func(t *testing.T) {
		reply := r.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "n2", LastLogIndex: 10, LastLogTerm: 0})
		if reply.VoteGranted {
			t.Fatalf("旧日志候选人不应拿到票：%+v", reply)
		}
	})

	t.Run("日志更旧的候选人拿不到票（lastLogIndex 更短）", func(t *testing.T) {
		reply := r.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "n2", LastLogIndex: 1, LastLogTerm: 1})
		if reply.VoteGranted {
			t.Fatalf("日志更短的候选人不应拿到票：%+v", reply)
		}
	})

	t.Run("日志不比自己旧的候选人拿到票", func(t *testing.T) {
		reply := r.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "n2", LastLogIndex: 2, LastLogTerm: 1})
		if !reply.VoteGranted {
			t.Fatalf("应投票：%+v", reply)
		}
		if r.votedFor != "n2" {
			t.Fatalf("votedFor = %q, want n2", r.votedFor)
		}
	})

	t.Run("同一任期已投过票，不再投给别人", func(t *testing.T) {
		reply := r.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "n3", LastLogIndex: 2, LastLogTerm: 1})
		if reply.VoteGranted {
			t.Fatalf("同一任期已投过票，不应再投：%+v", reply)
		}
	})

	t.Run("同一任期重复拉票可以再投给同一个人", func(t *testing.T) {
		reply := r.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "n2", LastLogIndex: 2, LastLogTerm: 1})
		if !reply.VoteGranted {
			t.Fatalf("重复拉票应再次投票：%+v", reply)
		}
	})

	t.Run("更大任期到来时任期前进、票清空", func(t *testing.T) {
		reply := r.HandleRequestVote(&RequestVoteArgs{Term: 2, CandidateID: "n3", LastLogIndex: 2, LastLogTerm: 1})
		if !reply.VoteGranted || reply.Term != 2 || r.currentTerm != 2 || r.votedFor != "n3" {
			t.Fatalf("got %+v term=%d votedFor=%q", reply, r.currentTerm, r.votedFor)
		}
	})
}

func TestHandleAppendEntries_ConflictTruncation(t *testing.T) {
	peers := []string{"n1", "n2", "n3"}
	r := newTestRaft("n1", peers)
	r.currentTerm = 1
	// 日志 [哨兵, {1,A}, {1,B}, {1,C}]。
	r.log = []LogEntry{{Term: 0}, {Term: 1, Command: []byte("A")}, {Term: 1, Command: []byte("B")}, {Term: 1, Command: []byte("C")}}

	t.Run("任期更小的 AppendEntries 直接拒绝", func(t *testing.T) {
		reply := r.HandleAppendEntries(&AppendEntriesArgs{Term: 0, LeaderID: "n2", PrevLogIndex: 3, PrevLogTerm: 1})
		if reply.Success || reply.Term != 1 {
			t.Fatalf("got %+v, want Success=false Term=1", reply)
		}
	})

	t.Run("PrevLogIndex 超出日志长度时拒绝", func(t *testing.T) {
		reply := r.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: "n2", PrevLogIndex: 9, PrevLogTerm: 1})
		if reply.Success {
			t.Fatalf("PrevLogIndex 超出日志应拒绝：%+v", reply)
		}
	})

	t.Run("PrevLogTerm 不匹配时拒绝", func(t *testing.T) {
		reply := r.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: "n2", PrevLogIndex: 3, PrevLogTerm: 2})
		if reply.Success {
			t.Fatalf("PrevLogTerm 不匹配应拒绝：%+v", reply)
		}
	})

	t.Run("冲突条目截断并覆盖", func(t *testing.T) {
		reply := r.HandleAppendEntries(&AppendEntriesArgs{
			Term: 2, LeaderID: "n2", PrevLogIndex: 2, PrevLogTerm: 1,
			Entries:      []LogEntry{{Term: 2, Command: []byte("D")}},
			LeaderCommit: 2,
		})
		if !reply.Success {
			t.Fatalf("应接受：%+v", reply)
		}
		// 日志应为 [哨兵, {1,A}, {1,B}, {2,D}]，旧的 {1,C} 被截掉。
		if len(r.log) != 4 || r.log[3].Term != 2 || string(r.log[3].Command) != "D" {
			t.Fatalf("截断后日志不对：%+v", r.log)
		}
		if r.commitIndex != 2 {
			t.Fatalf("commitIndex = %d, want 2", r.commitIndex)
		}
		if r.leaderID != "n2" {
			t.Fatalf("leaderID = %q, want n2", r.leaderID)
		}
	})

	t.Run("重复发送同一批条目是幂等的（不重复追加）", func(t *testing.T) {
		before := len(r.log)
		reply := r.HandleAppendEntries(&AppendEntriesArgs{
			Term: 2, LeaderID: "n2", PrevLogIndex: 2, PrevLogTerm: 1,
			Entries:      []LogEntry{{Term: 2, Command: []byte("D")}},
			LeaderCommit: 2,
		})
		if !reply.Success || len(r.log) != before {
			t.Fatalf("重复发送应幂等：reply=%+v len=%d want %d", reply, len(r.log), before)
		}
	})

	t.Run("心跳（空 Entries）只推进 commitIndex", func(t *testing.T) {
		reply := r.HandleAppendEntries(&AppendEntriesArgs{
			Term: 2, LeaderID: "n2", PrevLogIndex: 3, PrevLogTerm: 2,
			LeaderCommit: 3,
		})
		if !reply.Success || r.commitIndex != 3 {
			t.Fatalf("心跳应推进 commitIndex：reply=%+v commitIndex=%d", reply, r.commitIndex)
		}
	})

	t.Run("LeaderCommit 不会超过本地日志长度", func(t *testing.T) {
		reply := r.HandleAppendEntries(&AppendEntriesArgs{
			Term: 2, LeaderID: "n2", PrevLogIndex: 3, PrevLogTerm: 2,
			LeaderCommit: 99,
		})
		if !reply.Success || r.commitIndex != 3 {
			t.Fatalf("commitIndex 不应超过日志：reply=%+v commitIndex=%d", reply, r.commitIndex)
		}
	})
}

func TestAdvanceCommitIndex_Median(t *testing.T) {
	peers := []string{"n1", "n2", "n3"}
	newLeader := func(terms []int) *Raft {
		r := newTestRaft("n1", peers)
		r.state = Leader
		r.currentTerm = 2
		r.log = []LogEntry{{Term: 0}}
		for _, tm := range terms {
			r.log = append(r.log, LogEntry{Term: tm, Command: []byte("x")})
		}
		return r
	}

	t.Run("中位数是当前任期条目时推进", func(t *testing.T) {
		// 日志任期 [0,1,1,2,2]，matchIndex [4,3,2] → 中位数 3，log[3] 是任期 2。
		r := newLeader([]int{1, 1, 2, 2})
		r.matchIndex["n1"] = 4
		r.matchIndex["n2"] = 3
		r.matchIndex["n3"] = 2
		r.advanceCommitIndexLocked()
		if r.commitIndex != 3 {
			t.Fatalf("commitIndex = %d, want 3", r.commitIndex)
		}
	})

	t.Run("多数派都到了最新时推到最新", func(t *testing.T) {
		r := newLeader([]int{1, 1, 2, 2})
		r.matchIndex["n1"] = 4
		r.matchIndex["n2"] = 4
		r.matchIndex["n3"] = 2
		r.advanceCommitIndexLocked()
		if r.commitIndex != 4 {
			t.Fatalf("commitIndex = %d, want 4", r.commitIndex)
		}
	})

	t.Run("中位数是旧任期条目时不推进（Raft 安全规则）", func(t *testing.T) {
		// 日志任期 [0,1,1,1,2]，matchIndex [2,2,4] → 中位数 2，log[2] 是任期 1。
		r := newLeader([]int{1, 1, 1, 2})
		r.matchIndex["n1"] = 2
		r.matchIndex["n2"] = 2
		r.matchIndex["n3"] = 4
		r.advanceCommitIndexLocked()
		if r.commitIndex != 0 {
			t.Fatalf("commitIndex = %d, want 0（旧任期条目不能靠计数提交）", r.commitIndex)
		}
	})

	t.Run("只有一个 follower 追上时不够多数派", func(t *testing.T) {
		r := newLeader([]int{2, 2})
		r.matchIndex["n1"] = 2
		r.matchIndex["n2"] = 0
		r.matchIndex["n3"] = 0
		r.advanceCommitIndexLocked()
		if r.commitIndex != 0 {
			t.Fatalf("commitIndex = %d, want 0", r.commitIndex)
		}
	})
}

func TestStart_OnlyLeader(t *testing.T) {
	r := newTestRaft("n1", []string{"n1", "n2", "n3"})

	t.Run("follower 不能接受写入", func(t *testing.T) {
		if _, _, ok := r.Start([]byte("x")); ok {
			t.Fatal("follower 的 Start 应返回 isLeader=false")
		}
	})

	t.Run("leader 追加日志并返回下标与任期", func(t *testing.T) {
		r.state = Leader
		r.currentTerm = 3
		idx, term, ok := r.Start([]byte("hello"))
		if !ok || idx != 1 || term != 3 {
			t.Fatalf("got idx=%d term=%d ok=%v", idx, term, ok)
		}
		if len(r.log) != 2 || string(r.log[1].Command) != "hello" {
			t.Fatalf("日志不对：%+v", r.log)
		}
		if r.matchIndex["n1"] != 1 {
			t.Fatalf("leader 自己的 matchIndex = %d, want 1", r.matchIndex["n1"])
		}
	})
}

func TestAppendEntries_StepsDownCandidate(t *testing.T) {
	r := newTestRaft("n1", []string{"n1", "n2", "n3"})
	r.state = Candidate
	r.currentTerm = 5
	r.votedFor = "n1" // 自己投了自己
	reply := r.HandleAppendEntries(&AppendEntriesArgs{Term: 5, LeaderID: "n2", PrevLogIndex: 0, PrevLogTerm: 0})
	if !reply.Success {
		t.Fatalf("同任期合法 leader 的心跳应接受：%+v", reply)
	}
	if r.state != Follower {
		t.Fatalf("收到合法 leader 心跳后应退回 follower，当前 %v", r.state)
	}
	if r.votedFor != "n1" {
		t.Fatalf("votedFor 不应被清空（本任期已投的票不能再投），当前 %q", r.votedFor)
	}
}

package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// 计时参数（论文建议值，M1 实测选主稳定后写进文档，不再调）。
const (
	// ElectionTimeoutMin/Max：follower 收不到心跳后，随机等这么久才参选。
	// 随机化是为避免多个节点同时参选、反复分票。
	ElectionTimeoutMin = 150 * time.Millisecond
	ElectionTimeoutMax = 300 * time.Millisecond
	// HeartbeatInterval：leader 给每个 follower 发 AppendEntries 的间隔。
	HeartbeatInterval = 50 * time.Millisecond
	// applyTickInterval：apply 循环的兜底检查间隔（正常由 notify 唤醒）。
	applyTickInterval = 10 * time.Millisecond
)

// Role 是节点在 Raft 里的角色。
type Role int

const (
	// Follower 只响应 RPC，超时未收到心跳就参选。
	Follower Role = iota
	// Candidate 正在拉票。
	Candidate
	// Leader 负责接收客户端写入并复制日志。
	Leader
)

// String 返回角色的可读名字（日志与 /debug/log 用）。
func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// ApplyMsg 是 Raft 交给状态机的一条已提交命令。
// 状态机必须按 Index 递增的顺序应用（Raft 保证 applyCh 按序投递）。
type ApplyMsg struct {
	Index   int
	Command []byte
}

// RequestVoteArgs 是 RequestVote RPC 的参数。
type RequestVoteArgs struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidateId"`
	LastLogIndex int    `json:"lastLogIndex"`
	LastLogTerm  int    `json:"lastLogTerm"`
}

// RequestVoteReply 是 RequestVote RPC 的回复。
type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

// AppendEntriesArgs 是 AppendEntries RPC 的参数。
// Entries 为空时就是心跳。
type AppendEntriesArgs struct {
	Term         int        `json:"term"`
	LeaderID     string     `json:"leaderId"`
	PrevLogIndex int        `json:"prevLogIndex"`
	PrevLogTerm  int        `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leaderCommit"`
}

// AppendEntriesReply 是 AppendEntries RPC 的回复。
type AppendEntriesReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

// DebugInfo 是 /debug/log 返回的节点快照（字段名见 transport 与 node）。
type DebugInfo struct {
	ID          string `json:"id"`
	Term        int    `json:"term"`
	Role        string `json:"role"`
	LogLen      int    `json:"logLen"`
	CommitIndex int    `json:"commitIndex"`
	LastApplied int    `json:"lastApplied"`
	Leader      string `json:"leader"`
}

// Raft 是一个 Raft 节点。所有状态都由 mu 保护；
// RPC handler 只做“收包、加锁、处理、回包”，不直接碰状态机；
// 对外 RPC 一律在不持锁时发出。
type Raft struct {
	mu sync.Mutex

	id        string
	peers     []string          // 集群全部节点 id（含自己）
	addrs     map[string]string // 节点 id -> host:port（不含自己也行，查不到就跳过）
	transport Transport

	// 持久状态（论文图 2；每次改动后调 persistLocked）。
	state       Role
	currentTerm int
	votedFor    string
	// log[0] 是哨兵条目（Term 0，无命令），让日志下标从 1 开始，
	// 与论文的“日志下标从 1 开始”一致。
	log []LogEntry

	// 易失状态。
	commitIndex int
	lastApplied int
	// Leader 的易失状态。
	nextIndex  map[string]int
	matchIndex map[string]int
	leaderID   string

	persister     Persister
	applyCh       chan ApplyMsg
	applyNotifyCh chan struct{}
	resetCh       chan struct{}
	notifyChs     map[string]chan struct{}

	done     chan struct{}
	stopOnce sync.Once
	running  bool
}

// NewRaft 建一个节点，不启动任何 goroutine（Run 才启动）。
// applyCh 由调用方消费（node 层把它交给 KV 状态机）；
// 传 nil 时用一个带缓冲的默认 channel（单元测试可直接忽略它）。
// 持久化状态从 persister.Load() 恢复：term、votedFor、日志、lastApplied。
// 恢复后 commitIndex 也设为 lastApplied——
// 被应用过的条目一定是已提交的，设回去是安全的；
// lastApplied 之后的已提交条目等新 leader 的心跳带 LeaderCommit
// 过来后，再由 apply 循环正常应用。
// Load 出错（meta 损坏、日志与 meta 自相矛盾）时返回错误，
// 调用方应拒绝启动，而不是带着猜错的状态跑。
func NewRaft(id string, peers []string, addrs map[string]string, transport Transport, persister Persister, applyCh chan ApplyMsg) (*Raft, error) {
	if applyCh == nil {
		applyCh = make(chan ApplyMsg, 100)
	}
	r := &Raft{
		id:            id,
		peers:         append([]string(nil), peers...),
		addrs:         addrs,
		transport:     transport,
		state:         Follower,
		log:           []LogEntry{{Term: 0}},
		nextIndex:     make(map[string]int),
		matchIndex:    make(map[string]int),
		persister:     persister,
		applyCh:       applyCh,
		applyNotifyCh: make(chan struct{}, 1),
		resetCh:       make(chan struct{}, 1),
		notifyChs:     make(map[string]chan struct{}),
		done:          make(chan struct{}),
	}
	for _, p := range r.peers {
		r.nextIndex[p] = 1
		r.matchIndex[p] = 0
		if p != id {
			r.notifyChs[p] = make(chan struct{}, 1)
		}
	}
	st, err := persister.Load()
	if err != nil {
		return nil, fmt.Errorf("恢复持久状态失败：%w", err)
	}
	if len(st.Log) > 0 {
		r.currentTerm = st.CurrentTerm
		r.votedFor = st.VotedFor
		r.log = append([]LogEntry(nil), st.Log...)
	}
	if st.LastApplied > len(r.log)-1 {
		return nil, fmt.Errorf("恢复的状态不一致：lastApplied=%d，但日志只有 %d 条用户条目", st.LastApplied, len(r.log)-1)
	}
	r.lastApplied = st.LastApplied
	r.commitIndex = st.LastApplied
	return r, nil
}

// Run 启动选举计时器与 apply 循环。只调一次，多次调用是空操作。
func (r *Raft) Run() {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return
	}
	r.running = true
	r.mu.Unlock()
	go r.runElectionTimer()
	go r.runApplyLoop()
}

// Stop 停掉本节点的所有后台 goroutine（选举计时、apply 循环、
// 各 follower 的复制循环）。可多次调用。HTTP 服务由调用方另行停。
func (r *Raft) Stop() {
	r.stopOnce.Do(func() { close(r.done) })
}

// GetState 返回当前任期与“是不是 leader”（测试与 node 层用）。
func (r *Raft) GetState() (term int, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.state == Leader
}

// LeaderID 返回最近一次确认的 leader id（自己是 leader 时返回自己；
// 刚启动、还没收到过心跳时返回 ""）。
func (r *Raft) LeaderID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderID
}

// RestoreInfo 返回持久日志的一份拷贝（含下标 0 的哨兵条目）与恢复出来的
// lastApplied，供 node 层在启动时把 1..lastApplied 的已提交日志
// 重放到 KV 状态机。
func (r *Raft) RestoreInfo() ([]LogEntry, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]LogEntry(nil), r.log...), r.lastApplied
}

// LastApplied 返回已应用到状态机的最大日志下标。
func (r *Raft) LastApplied() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastApplied
}

// Debug 返回 /debug/log 要的快照。LogLen 是用户日志条数（不含哨兵）。
func (r *Raft) Debug() DebugInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	return DebugInfo{
		ID:          r.id,
		Term:        r.currentTerm,
		Role:        r.state.String(),
		LogLen:      len(r.log) - 1,
		CommitIndex: r.commitIndex,
		LastApplied: r.lastApplied,
		Leader:      r.leaderID,
	}
}

// Start 是客户端写入的入口（只在 leader 上有效）。
// 把命令追加到本地日志，返回它的下标与任期；
// 不是 leader 时返回 isLeader=false，调用方应去找 leader。
func (r *Raft) Start(command []byte) (index, term int, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != Leader {
		return -1, r.currentTerm, false
	}
	index = r.lastLogIndexLocked() + 1
	term = r.currentTerm
	r.log = append(r.log, LogEntry{Term: term, Command: append([]byte(nil), command...)})
	r.persistLocked()
	r.matchIndex[r.id] = index
	r.nextIndex[r.id] = index + 1
	r.advanceCommitIndexLocked()
	r.notifyFollowersLocked()
	return index, term, true
}

// ---- 内部辅助（调用方必须持 mu，函数名以 Locked 结尾） ----

func (r *Raft) lastLogIndexLocked() int { return len(r.log) - 1 }

func (r *Raft) lastLogTermLocked() int { return r.log[len(r.log)-1].Term }

// persistLocked 把持久状态写进 Persister（文件实现里写文件 + fsync，
// 调用点不用改）。
// 持久化失败是致命错误：节点直接退出（fail-stop），而不是带着
// 可能丢数据的状态继续服务、回复已经没法保证的写入。
// 重启后从磁盘恢复，能恢复多少就是多少，不会把“已确认”的写丢了
// 又装作没事。
func (r *Raft) persistLocked() {
	st := PersistentState{
		CurrentTerm: r.currentTerm,
		VotedFor:    r.votedFor,
		Log:         append([]LogEntry(nil), r.log...),
		LastApplied: r.lastApplied,
	}
	if err := r.persister.Save(st); err != nil {
		log.Fatalf("节点 %s 持久化失败，停机（避免继续服务却丢数据）：%v", r.id, err)
	}
}

// stepDownLocked 转成 follower（任期变成 term，票清空）。
func (r *Raft) stepDownLocked(term int) {
	r.state = Follower
	r.currentTerm = term
	r.votedFor = ""
	r.leaderID = ""
	r.persistLocked()
	r.resetElectionTimerLocked()
}

// resetElectionTimerLocked 让选举计时器重新开始计时（非阻塞发送，
// 计时器 goroutine 下一轮 select 会收到；持锁调用是安全的）。
func (r *Raft) resetElectionTimerLocked() {
	select {
	case r.resetCh <- struct{}{}:
	default:
	}
}

// notifyApplyLocked 唤醒 apply 循环（commitIndex 前进了）。
func (r *Raft) notifyApplyLocked() {
	select {
	case r.applyNotifyCh <- struct{}{}:
	default:
	}
}

// notifyFollowersLocked 唤醒各 follower 的复制循环（有新日志要发）。
func (r *Raft) notifyFollowersLocked() {
	for _, ch := range r.notifyChs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// advanceCommitIndexLocked 按论文图 2 的 leader 规则推进 commitIndex：
// 取全部 matchIndex 的中位数 N，若 log[N] 是当前任期的条目且 N 更大，
// 就把 commitIndex 推进到 N。
func (r *Raft) advanceCommitIndexLocked() {
	if r.state != Leader {
		return
	}
	vals := make([]int, 0, len(r.peers))
	for _, p := range r.peers {
		vals = append(vals, r.matchIndex[p])
	}
	sort.Ints(vals)
	n := vals[len(vals)/2]
	if n > r.commitIndex && n <= r.lastLogIndexLocked() && r.log[n].Term == r.currentTerm {
		r.commitIndex = n
		r.notifyApplyLocked()
	}
}

// ---- 选举 ----

// electionTimeout 返回一次随机选举超时（150–300ms）。
func electionTimeout() time.Duration {
	return ElectionTimeoutMin + time.Duration(rand.Int63n(int64(ElectionTimeoutMax-ElectionTimeoutMin)))
}

// runElectionTimer 是选举计时 goroutine：超时未收到心跳（或投票）就参选。
// leader 也跑这个循环，但超时后直接跳过（不参选）。
func (r *Raft) runElectionTimer() {
	for {
		timer := time.NewTimer(electionTimeout())
		select {
		case <-r.done:
			timer.Stop()
			return
		case <-r.resetCh:
			timer.Stop()
			continue
		case <-timer.C:
		}
		r.mu.Lock()
		if r.state == Leader {
			r.mu.Unlock()
			continue
		}
		r.mu.Unlock()
		r.startElection()
	}
}

// startElection 发起一次选举：任期 +1、投自己一票、并行向其他节点拉票。
// 拿到多数票就地转 leader；收到更大任期就退回 follower。
func (r *Raft) startElection() {
	r.mu.Lock()
	r.currentTerm++
	r.state = Candidate
	r.votedFor = r.id
	r.persistLocked()
	term := r.currentTerm
	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  r.id,
		LastLogIndex: r.lastLogIndexLocked(),
		LastLogTerm:  r.lastLogTermLocked(),
	}
	r.mu.Unlock()

	votes := 1 // 自己的一票（下面所有读写都在 r.mu 保护下进行）
	for _, peer := range r.peers {
		if peer == r.id {
			continue
		}
		go func(peer string) {
			addr, ok := r.addrs[peer]
			if !ok {
				return
			}
			reply, err := r.transport.RequestVote(addr, &args)
			if err != nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.state != Candidate || r.currentTerm != term {
				return // 已经不是这次选举的候选人了
			}
			if reply.Term > r.currentTerm {
				r.stepDownLocked(reply.Term)
				return
			}
			if !reply.VoteGranted {
				return
			}
			votes++
			if votes > len(r.peers)/2 {
				r.becomeLeaderLocked()
			}
		}(peer)
	}

	// 单节点集群：自己的一票就是多数派，没有别的节点可拉票，
	// 上面的循环一个 goroutine 都不会起，必须在这里直接当选。
	r.mu.Lock()
	if r.state == Candidate && r.currentTerm == term && votes > len(r.peers)/2 {
		r.becomeLeaderLocked()
	}
	r.mu.Unlock()
}

// becomeLeaderLocked 转成 leader：初始化各 follower 的 nextIndex / matchIndex，
// 立刻发一轮心跳，并为每个 follower 起一个复制循环。
func (r *Raft) becomeLeaderLocked() {
	r.state = Leader
	r.leaderID = r.id
	last := r.lastLogIndexLocked()
	for _, p := range r.peers {
		r.nextIndex[p] = last + 1
		r.matchIndex[p] = 0
	}
	r.matchIndex[r.id] = last
	r.nextIndex[r.id] = last + 1
	r.notifyFollowersLocked()
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		go r.replicateTo(p)
	}
}

// replicateTo 是一个 follower 的专属复制循环：每 50ms 发一次 AppendEntries
// （有新日志时立刻发，不等心跳间隔）。
func (r *Raft) replicateTo(peer string) {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.done:
			return
		case <-r.notifyChs[peer]:
		case <-ticker.C:
		}

		r.mu.Lock()
		if r.state != Leader {
			r.mu.Unlock()
			return
		}
		next := r.nextIndex[peer]
		args := AppendEntriesArgs{
			Term:         r.currentTerm,
			LeaderID:     r.id,
			PrevLogIndex: next - 1,
			PrevLogTerm:  r.log[next-1].Term,
			LeaderCommit: r.commitIndex,
		}
		if next <= r.lastLogIndexLocked() {
			args.Entries = append([]LogEntry(nil), r.log[next:]...)
		}
		r.mu.Unlock()

		addr, ok := r.addrs[peer]
		if !ok {
			return
		}
		reply, err := r.transport.AppendEntries(addr, &args)
		if err != nil {
			continue // 网络不通就等下一轮心跳重试
		}

		r.mu.Lock()
		if reply.Term > r.currentTerm {
			r.stepDownLocked(reply.Term)
			r.mu.Unlock()
			return
		}
		if r.state != Leader || r.currentTerm != args.Term {
			r.mu.Unlock()
			continue // 任期已变，这次回复作废
		}
		if reply.Success {
			r.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			r.nextIndex[peer] = r.matchIndex[peer] + 1
			r.advanceCommitIndexLocked()
		} else if r.nextIndex[peer] > 1 {
			// 日志不一致：一次回退一格再试（M1 求简单正确；
			// 快速回退是 M2 之后的优化，见规划风险 2）。
			r.nextIndex[peer]--
		}
		r.mu.Unlock()
	}
}

// ---- RPC 处理（transport 层调这两个方法；单元测试也直接调） ----

// HandleRequestVote 处理投票请求（论文图 2 投票规则）。
func (r *Raft) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	reply := &RequestVoteReply{}
	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		return reply
	}
	if args.Term > r.currentTerm {
		r.stepDownLocked(args.Term)
	}
	// 只投给日志不比自己旧的候选人：先比 lastLogTerm，再比 lastLogIndex。
	upToDate := args.LastLogTerm > r.lastLogTermLocked() ||
		(args.LastLogTerm == r.lastLogTermLocked() && args.LastLogIndex >= r.lastLogIndexLocked())
	if (r.votedFor == "" || r.votedFor == args.CandidateID) && upToDate {
		r.votedFor = args.CandidateID
		r.persistLocked()
		reply.VoteGranted = true
		r.resetElectionTimerLocked()
	}
	reply.Term = r.currentTerm
	return reply
}

// HandleAppendEntries 处理日志复制/心跳（论文图 2 follower 规则）。
func (r *Raft) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	reply := &AppendEntriesReply{}
	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		return reply
	}
	if args.Term > r.currentTerm {
		r.stepDownLocked(args.Term)
	}
	// 同任期的合法 leader 出现（我是候选人或旧 leader）：退回 follower。
	// 注意 votedFor 不能动——本任期已经投过的票不能再投，
	// 否则同一个任期可能选出两个 leader。
	if r.state != Follower {
		r.state = Follower
	}
	r.leaderID = args.LeaderID
	r.resetElectionTimerLocked()

	// 一致性检查：PrevLogIndex 处的任期必须与 leader 说的一致。
	if args.PrevLogIndex > r.lastLogIndexLocked() {
		reply.Term = r.currentTerm
		return reply
	}
	if args.PrevLogIndex > 0 && r.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = r.currentTerm
		return reply
	}
	// 逐条合入：已有的相同任期条目跳过，冲突条目截断后覆盖，缺的追加。
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx > r.lastLogIndexLocked() {
			r.log = append(r.log, args.Entries[i:]...)
			r.persistLocked()
			break
		}
		if r.log[idx].Term != e.Term {
			r.log = append(r.log[:idx], args.Entries[i:]...)
			r.persistLocked()
			break
		}
	}
	if args.LeaderCommit > r.commitIndex {
		r.commitIndex = min(args.LeaderCommit, r.lastLogIndexLocked())
		r.notifyApplyLocked()
	}
	reply.Term = r.currentTerm
	reply.Success = true
	return reply
}

// ---- apply 循环 ----

// runApplyLoop 把已提交的日志按序交给状态机（经 applyCh）。
// lastApplied 只在这里前进，保证应用顺序与日志顺序一致。
func (r *Raft) runApplyLoop() {
	ticker := time.NewTicker(applyTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.done:
			return
		case <-r.applyNotifyCh:
		case <-ticker.C:
		}
		for {
			r.mu.Lock()
			if r.lastApplied >= r.commitIndex {
				r.mu.Unlock()
				break
			}
			r.lastApplied++
			msg := ApplyMsg{Index: r.lastApplied, Command: r.log[r.lastApplied].Command}
			r.mu.Unlock()
			select {
			case r.applyCh <- msg:
				// 已投递给状态机消费端，落盘 lastApplied。
				// 注意“投递”不等于“状态机已执行”：崩溃在两者之间时，
				// 重启会把这条重放一遍——Put/Delete 都是幂等的，
				// 多应用一条是安全的；且 lastApplied 永远 ≤ commitIndex，
				// 重放的绝不会是未提交的尾部日志。
				r.mu.Lock()
				r.persistLocked()
				r.mu.Unlock()
			case <-r.done:
				return
			}
		}
	}
}

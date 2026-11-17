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
// Snapshot 非空时表示“恢复快照”而不是“应用一条命令”：
// Command 为空，状态机应把 Snapshot 里的数据整体恢复（见 snapshot.go），
// Index 是快照覆盖的最后一条日志下标。
type ApplyMsg struct {
	Index    int
	Command  []byte
	Snapshot []byte
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
	// ConflictIndex 是拒绝时 follower 给的“快速回退”提示（Raft 论文 §5.3）：
	// follower 日志里第一个与 leader 对不上的下标。
	// PrevLogIndex 超出 follower 日志时是 lastLogIndex+1；
	// PrevLogIndex 处任期冲突时，是该冲突任期连续段的第一个下标。
	// leader 按它直接跳 nextIndex，不用一次一格地试——
	// M1 的逐格回退在快照场景下太慢（落后 9000 条要 4500 次心跳，
	// 225 秒，才掉进快照区间，快照追赶根本等不起）。
	// 0 表示“没有提示”（老版本对端），leader 退回逐格回退。
	ConflictIndex int `json:"conflict_index,omitempty"`
}

// DebugInfo 是 /debug/log 返回的节点快照（字段名见 transport 与 node）。
type DebugInfo struct {
	ID            string `json:"id"`
	Term          int    `json:"term"`
	Role          string `json:"role"`
	LogLen        int    `json:"logLen"`
	CommitIndex   int    `json:"commitIndex"`
	LastApplied   int    `json:"lastApplied"`
	Leader        string `json:"leader"`
	SnapshotIndex int    `json:"snapshotIndex"`
}

// Raft 是一个 Raft 节点。所有状态都由 mu 保护；
// RPC handler 只做“收包、加锁、处理、回包”，不直接碰状态机；
// 对外 RPC 一律在不持锁时发出。
//
// 并发结构（M3 重构后）：
//   - 选举计时与逐 follower 的复制心跳走 Clock.AfterFunc 回调链，
//     不再各起一个 goroutine：回调里发 RPC、收回复（经 Transport 的
//     cb），决策都在回调里同步完成；
//   - 只有 apply 循环还是 goroutine（它只负责把已提交日志按序
//     交给状态机，时序不影响决策；模拟器在断言前把它排空）。
//
// 这样一来，模拟器用虚拟时钟驱动时，全部决策都在 Advance 的调用栈里
// 同步发生，不受 goroutine 调度影响——同一种子两次跑的结果相同。
type Raft struct {
	mu sync.Mutex

	id        string
	peers     []string          // 集群全部节点 id（含自己）
	addrs     map[string]string // 节点 id -> host:port（不含自己也行，查不到就跳过）
	transport Transport
	clock     Clock
	rng       *rand.Rand // 选举超时的随机源：生产用时间种子，模拟器注入带种子的

	// 持久状态（论文图 2；每次改动后调 persistLocked）。
	state       Role
	currentTerm int
	votedFor    string
	// log[0] 是哨兵条目（Term 0，无命令），让日志下标从 1 开始，
	// 与论文的“日志下标从 1 开始”一致。
	// 快照（M5）之后 log[0] 变成快照位置的占位条目（Term = 快照任期，
	// 无命令）：log[i] 是绝对下标 snapshotIndex+i 的条目，
	// 所有按下标取日志的地方都要减这个基底（见 logTermAtLocked）。
	log []LogEntry
	// snapshotIndex 是 log[0] 的绝对下标（0 表示从未快照）；
	// snapshotTerm 是该位置的任期（即 log[0].Term）；
	// snapshotData 是该快照的状态机全量（发 InstallSnapshot 时用）。
	snapshotIndex int
	snapshotTerm  int
	snapshotData  []byte
	// pendingSnap 是已安装、等 apply 循环投递给状态机的快照
	// （见 snapshot.go 的 pendingSnapshot）。
	pendingSnap *pendingSnapshot

	// 易失状态。
	commitIndex int
	lastApplied int
	// Leader 的易失状态。
	nextIndex  map[string]int
	matchIndex map[string]int
	leaderID   string

	// 选举计时器（AfterFunc 回调链）：同一时刻最多一个在排队。
	// electionGen 是代次——旧回调（已被停止、或在 Stop 与回调
	// 的竞态里漏网的）靠它认出自己已过期，直接返回。
	electionTimer Timer
	electionGen   int
	// replTimers 是各 follower 的复制心跳回调句柄（leader 才有）。
	replTimers map[string]Timer

	persister     Persister
	applyCh       chan ApplyMsg
	applyNotifyCh chan struct{}

	done     chan struct{}
	stopOnce sync.Once
	running  bool
	stopped  bool // Stop() 后置位：所有回调先查它，已停的节点直接返回
}

// RaftOption 是 NewRaft 的可选参数（Clock 与随机源，不传用默认的
// 真实实现；模拟器注入虚拟时钟与带种子的 RNG）。
type RaftOption func(*Raft)

// WithClock 指定 Raft 用的时钟（默认真实时钟）。
func WithClock(c Clock) RaftOption {
	return func(r *Raft) {
		if c != nil {
			r.clock = c
		}
	}
}

// WithRand 指定选举超时的随机源（默认用当前时间做种子）。
// 模拟器给每个节点一个带种子的 RNG，让选举超时序列可复现。
func WithRand(rng *rand.Rand) RaftOption {
	return func(r *Raft) {
		if rng != nil {
			r.rng = rng
		}
	}
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
// opts 可注入 Clock（默认真实时钟）与 RNG（默认时间种子），
// 模拟器用它换虚拟时钟与可复现的随机源。
func NewRaft(id string, peers []string, addrs map[string]string, transport Transport, persister Persister, applyCh chan ApplyMsg, opts ...RaftOption) (*Raft, error) {
	if applyCh == nil {
		applyCh = make(chan ApplyMsg, 100)
	}
	r := &Raft{
		id:            id,
		peers:         append([]string(nil), peers...),
		addrs:         addrs,
		transport:     transport,
		clock:         NewRealClock(),
		rng:           rand.New(rand.NewSource(time.Now().UnixNano())),
		state:         Follower,
		log:           []LogEntry{{Term: 0}},
		nextIndex:     make(map[string]int),
		matchIndex:    make(map[string]int),
		replTimers:    make(map[string]Timer),
		persister:     persister,
		applyCh:       applyCh,
		applyNotifyCh: make(chan struct{}, 1),
		done:          make(chan struct{}),
	}
	for _, o := range opts {
		o(r)
	}
	for _, p := range r.peers {
		r.nextIndex[p] = 1
		r.matchIndex[p] = 0
	}
	st, err := persister.Load()
	if err != nil {
		return nil, fmt.Errorf("恢复持久状态失败：%w", err)
	}
	if st.SnapshotIndex > 0 && len(st.Log) == 0 {
		return nil, fmt.Errorf("恢复的状态不一致：快照下标 %d，但日志为空", st.SnapshotIndex)
	}
	if len(st.Log) > 0 {
		r.currentTerm = st.CurrentTerm
		r.votedFor = st.VotedFor
		r.log = append([]LogEntry(nil), st.Log...)
	}
	r.snapshotIndex = st.SnapshotIndex
	r.snapshotTerm = st.SnapshotTerm
	if r.snapshotIndex > 0 {
		// 日志已压缩：快照数据必须在，否则 1..snapshotIndex 的状态丢了。
		snap, ok, err := persister.LoadSnapshot()
		if err != nil {
			return nil, fmt.Errorf("加载快照失败：%w", err)
		}
		if !ok {
			return nil, fmt.Errorf("日志已压缩到下标 %d，但快照文件缺失，没法恢复", r.snapshotIndex)
		}
		if snap.LastIncludedIndex != r.snapshotIndex || snap.LastIncludedTerm != r.snapshotTerm {
			return nil, fmt.Errorf("快照文件与日志基底不一致：快照是（下标 %d，任期 %d），日志基底是（下标 %d，任期 %d）", snap.LastIncludedIndex, snap.LastIncludedTerm, r.snapshotIndex, r.snapshotTerm)
		}
		r.snapshotData = snap.Data
	}
	// lastApplied 的上界：日志只覆盖到 snapshotIndex+len(log)-1。
	if st.LastApplied > r.snapshotIndex+len(r.log)-1 {
		return nil, fmt.Errorf("恢复的状态不一致：lastApplied=%d，但日志只覆盖到下标 %d", st.LastApplied, r.snapshotIndex+len(r.log)-1)
	}
	// lastApplied < snapshotIndex 是合法的中间态——快照已落盘、
	// 但 apply 循环还没把 lastApplied 推过去就崩了。
	// 快照本身就是“1..snapshotIndex 已应用”的状态，钳过去是安全的；
	// 启动重放时状态机先从快照恢复，再重放快照之后的日志。
	r.lastApplied = st.LastApplied
	if r.lastApplied < r.snapshotIndex {
		r.lastApplied = r.snapshotIndex
	}
	r.commitIndex = r.lastApplied
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
	r.armElectionTimerLocked()
	r.mu.Unlock()
	go r.runApplyLoop()
}

// Stop 停掉本节点的所有后台工作（选举计时、复制心跳、apply 循环）。
// 已排队的回调靠 stopped 标志与代次认出自己已过期，直接返回。
// 可多次调用。HTTP 服务由调用方另行停。
func (r *Raft) Stop() {
	r.mu.Lock()
	r.stopped = true
	r.stopElectionTimerLocked()
	r.stopReplicateTimersLocked()
	r.mu.Unlock()
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

// RestoreInfo 返回持久日志的一份拷贝、快照与恢复出来的 lastApplied，
// 供 node 层在启动时恢复状态机：
//   - 快照非空时先 Store.Restore(snapshot.Data)；
//   - 再把快照之后、lastApplied 之前的日志按序重放
//     （下标 i 的条目在 log[i-snapshot.LastIncludedIndex]）。
//
// log[0] 是基底位置的占位条目（快照位置、快照任期）或哨兵条目
// （从未快照时），调用方不要把它当命令重放。
func (r *Raft) RestoreInfo() ([]LogEntry, Snapshot, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	snap := Snapshot{
		LastIncludedIndex: r.snapshotIndex,
		LastIncludedTerm:  r.snapshotTerm,
		Data:              append([]byte(nil), r.snapshotData...),
	}
	return append([]LogEntry(nil), r.log...), snap, r.lastApplied
}

// LastApplied 返回已应用到状态机的最大日志下标。
func (r *Raft) LastApplied() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastApplied
}

// Debug 返回 /debug/log 要的快照。LogLen 是快照之后保留的日志条数
// （不含基底占位条目）；SnapshotIndex 是快照覆盖的最后一条日志下标。
func (r *Raft) Debug() DebugInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	return DebugInfo{
		ID:            r.id,
		Term:          r.currentTerm,
		Role:          r.state.String(),
		LogLen:        len(r.log) - 1,
		CommitIndex:   r.commitIndex,
		LastApplied:   r.lastApplied,
		Leader:        r.leaderID,
		SnapshotIndex: r.snapshotIndex,
	}
}

// LogSnapshot 返回日志的一份拷贝与当前的 commitIndex、lastApplied，
// 以及日志的基底下标 base（log[0] 是绝对下标 base 的条目；
// base 为 0 时 log[0] 是哨兵条目，否则是快照位置的占位条目）。
// 模拟器的不变式检查用它比对各节点日志；
// 返回的是拷贝，调用方改了也不会影响节点内部状态。
func (r *Raft) LogSnapshot() ([]LogEntry, int, int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]LogEntry(nil), r.log...), r.snapshotIndex, r.commitIndex, r.lastApplied
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

func (r *Raft) lastLogIndexLocked() int { return r.snapshotIndex + len(r.log) - 1 }

func (r *Raft) lastLogTermLocked() int { return r.log[len(r.log)-1].Term }

// logTermAtLocked 返回绝对下标 index 处条目的任期。
// index == snapshotIndex 时返回快照任期（log[0] 就是占位条目）；
// index < snapshotIndex 时条目已被压缩掉，返回 ok=false。
func (r *Raft) logTermAtLocked(index int) (term int, ok bool) {
	if index < r.snapshotIndex {
		return 0, false
	}
	return r.log[index-r.snapshotIndex].Term, true
}

// persistLocked 把持久状态写进 Persister（文件实现里写文件 + fsync，
// 调用点不用改）。
// 持久化失败是致命错误：节点直接退出（fail-stop），而不是带着
// 可能丢数据的状态继续服务、回复已经没法保证的写入。
// 重启后从磁盘恢复，能恢复多少就是多少，不会把“已确认”的写丢了
// 又装作没事。
//
// 注意：这里传给 Save 的 PersistentState 带快照下标与任期，
// 但 Save 不写它们——基底只由“日志首条目 + 快照文件”决定
// （见 log.go 的 PersistentState 注释）；
// 快照数据本身也不在这里写（TakeSnapshot / HandleInstallSnapshot
// 调 SaveSnapshot 先写快照文件）。
// persistMetaLocked 只落盘元数据（任期/投票/已应用位置），日志不动。
// apply 循环专用：应用推进只改 lastApplied，走全量 Save 的话
// 每次都要把整条日志比一遍、拷一遍（见 Persister.SaveMeta）。
// 调用方持 r.mu；失败同样 fail-stop。
func (r *Raft) persistMetaLocked() {
	if err := r.persister.SaveMeta(r.currentTerm, r.votedFor, r.lastApplied); err != nil {
		log.Fatalf("节点 %s 持久化元数据失败，停机（避免继续服务却丢数据）：%v", r.id, err)
	}
}

func (r *Raft) persistLocked() {
	st := PersistentState{
		CurrentTerm:   r.currentTerm,
		VotedFor:      r.votedFor,
		Log:           append([]LogEntry(nil), r.log...),
		LastApplied:   r.lastApplied,
		SnapshotIndex: r.snapshotIndex,
		SnapshotTerm:  r.snapshotTerm,
	}
	if err := r.persister.Save(st); err != nil {
		log.Fatalf("节点 %s 持久化失败，停机（避免继续服务却丢数据）：%v", r.id, err)
	}
}

// stepDownLocked 转成 follower（任期变成 term，票清空），
// 并重新开始选举计时。
func (r *Raft) stepDownLocked(term int) {
	r.state = Follower
	r.currentTerm = term
	r.votedFor = ""
	r.leaderID = ""
	r.stopReplicateTimersLocked()
	r.persistLocked()
	r.armElectionTimerLocked()
}

// armElectionTimerLocked 重新开始选举计时：停掉旧的，
// 用新的随机超时（150–300ms）排一个回调。调用方必须持 mu。
// 回调捕获排队时的代次，旧回调（停掉后漏网的、或与 Stop 竞态的）
// 靠代次认出自己已过期，直接返回。
// 没调 Run() 的节点不排计时器（单元测试直接调 handler，不启动节点，
// 排了也没人消费，还会在测试结束后漏网回调、读写已结束的测试对象）。
func (r *Raft) armElectionTimerLocked() {
	if r.stopped || !r.running {
		return
	}
	r.stopElectionTimerLocked()
	r.electionGen++
	gen := r.electionGen
	d := r.electionTimeout()
	r.electionTimer = r.clock.AfterFunc(d, func() { r.onElectionTimeout(gen) })
}

// stopElectionTimerLocked 停掉当前排队的选举计时器（有的话）。
// 调用方必须持 mu。
func (r *Raft) stopElectionTimerLocked() {
	if r.electionTimer != nil {
		r.electionTimer.Stop()
		r.electionTimer = nil
	}
}

// stopReplicateTimersLocked 停掉全部 follower 的复制心跳回调。
// 调用方必须持 mu。
func (r *Raft) stopReplicateTimersLocked() {
	for peer, t := range r.replTimers {
		if t != nil {
			t.Stop()
		}
		delete(r.replTimers, peer)
	}
}

// notifyApplyLocked 唤醒 apply 循环（commitIndex 前进了）。
func (r *Raft) notifyApplyLocked() {
	select {
	case r.applyNotifyCh <- struct{}{}:
	default:
	}
}

// notifyFollowersLocked 收到新日志后立刻给各 follower 发一次
// AppendEntries（不等下一轮 50ms 心跳）：把各 peer 的心跳回调
// 提前到现在。调用方必须持 mu（排回调本身不发包、不阻塞）。
func (r *Raft) notifyFollowersLocked() {
	if r.state != Leader {
		return
	}
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		r.triggerReplicateLocked(p)
	}
}

// triggerReplicateLocked 把某 follower 的下一次复制提前到现在：
// 停掉它排队的心跳回调，改排一个立即执行的。调用方必须持 mu。
// 没调 Run() 的节点不排回调（同 armElectionTimerLocked 的原因）。
func (r *Raft) triggerReplicateLocked(peer string) {
	if r.stopped || !r.running || r.state != Leader {
		return
	}
	if t, ok := r.replTimers[peer]; ok && t != nil {
		t.Stop()
	}
	term := r.currentTerm
	r.replTimers[peer] = r.clock.AfterFunc(0, func() { r.replicateOnce(peer, term) })
}

// scheduleReplicateLocked 给某 follower 排一次心跳回调（delay 后执行）。
// 调用方必须持 mu。没调 Run() 的节点不排回调。
func (r *Raft) scheduleReplicateLocked(peer string, delay time.Duration, term int) {
	if r.stopped || !r.running || r.state != Leader || r.currentTerm != term {
		return
	}
	if t, ok := r.replTimers[peer]; ok && t != nil {
		t.Stop()
	}
	r.replTimers[peer] = r.clock.AfterFunc(delay, func() { r.replicateOnce(peer, term) })
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
	// n > commitIndex >= snapshotIndex，所以 n 一定在日志保留范围内，
	// 减基底取下标是安全的。
	if n > r.commitIndex && n <= r.lastLogIndexLocked() && r.log[n-r.snapshotIndex].Term == r.currentTerm {
		r.commitIndex = n
		r.notifyApplyLocked()
	}
}

// ---- 选举 ----

// electionTimeout 返回一次随机选举超时（150–300ms）。
// 用节点自己的 RNG：模拟器注入带种子的 RNG，超时序列可复现。
func (r *Raft) electionTimeout() time.Duration {
	return ElectionTimeoutMin + time.Duration(r.rng.Int63n(int64(ElectionTimeoutMax-ElectionTimeoutMin)))
}

// onElectionTimeout 是选举计时器的回调：超时未收到心跳（或投票）就参选。
// gen 是排队时的代次——过期回调（已被新的计时取代、或节点已停）直接返回。
// 回调在时钟的 goroutine / 虚拟时钟的事件循环里执行，不持锁进入。
func (r *Raft) onElectionTimeout(gen int) {
	r.mu.Lock()
	if r.stopped || gen != r.electionGen {
		r.mu.Unlock()
		return
	}
	if r.state == Leader {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	r.startElection()
}

// startElection 发起一次选举：任期 +1、投自己一票、向其他节点拉票。
// 拉票是同步发、回调计票：发出去就返回，回复到了在回调里处理
// （拿到多数票就地转 leader；收到更大任期就退回 follower）。
// 选完立刻重排下一次超时——这次选不上，超时后会再选一轮。
func (r *Raft) startElection() {
	r.mu.Lock()
	if r.stopped || r.state == Leader {
		r.mu.Unlock()
		return
	}
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
	r.armElectionTimerLocked()
	r.mu.Unlock()

	votes := 1 // 自己的一票（下面所有读写都在 r.mu 保护下进行）
	for _, peer := range r.peers {
		if peer == r.id {
			continue
		}
		addr, ok := r.addrs[peer]
		if !ok {
			continue
		}
		r.transport.RequestVote(addr, &args, func(reply *RequestVoteReply, err error) {
			if err != nil {
				return // 消息丢了：等下一次选举重试，不在这里重试
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.stopped || r.state != Candidate || r.currentTerm != term {
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
		})
	}

	// 单节点集群：自己的一票就是多数派，没有别的节点可拉票，
	// 上面的循环一个请求都不会发，必须在这里直接当选。
	r.mu.Lock()
	if !r.stopped && r.state == Candidate && r.currentTerm == term && votes > len(r.peers)/2 {
		r.becomeLeaderLocked()
	}
	r.mu.Unlock()
}

// becomeLeaderLocked 转成 leader：初始化各 follower 的 nextIndex / matchIndex，
// 立刻给每人发一轮心跳（delay 0 的回调），并排好之后每 50ms 的心跳链。
func (r *Raft) becomeLeaderLocked() {
	r.state = Leader
	r.leaderID = r.id
	r.stopElectionTimerLocked() // leader 不需要选举计时器
	last := r.lastLogIndexLocked()
	for _, p := range r.peers {
		r.nextIndex[p] = last + 1
		r.matchIndex[p] = 0
	}
	r.matchIndex[r.id] = last
	r.nextIndex[r.id] = last + 1
	r.notifyFollowersLocked()
}

// replicateOnce 给一个 follower 发一次 AppendEntries（心跳或带日志）。
// 发完立刻排好下一次心跳（50ms 后），形成回调链；
// 任期变了 / 退回 follower / 节点停了，链就断了，不再排。
// 回调在时钟的 goroutine / 虚拟时钟的事件循环里执行，不持锁进入。
func (r *Raft) replicateOnce(peer string, term int) {
	r.mu.Lock()
	if r.stopped || r.state != Leader || r.currentTerm != term {
		r.mu.Unlock()
		return
	}
	next := r.nextIndex[peer]
	// follower 落后的程度已经超出 leader 保留的日志
	// （nextIndex 掉进了快照里）：不再逐条发，改发整个快照。
	if next <= r.snapshotIndex {
		args := InstallSnapshotArgs{
			Term:              r.currentTerm,
			LeaderID:          r.id,
			LastIncludedIndex: r.snapshotIndex,
			LastIncludedTerm:  r.snapshotTerm,
			Data:              append([]byte(nil), r.snapshotData...),
		}
		log.Printf("节点 %s 给 %s 发快照：lastIncludedIndex=%d", r.id, peer, r.snapshotIndex)
		// 先排好下一次心跳，再发包：对端再慢也不影响心跳节奏。
		r.scheduleReplicateLocked(peer, HeartbeatInterval, term)
		r.mu.Unlock()

		addr, ok := r.addrs[peer]
		if !ok {
			return
		}
		r.transport.InstallSnapshot(addr, &args, func(reply *InstallSnapshotReply, err error) {
			if err != nil {
				return // 消息丢了：等下一轮心跳重试
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.stopped {
				return
			}
			if reply.Term > r.currentTerm {
				r.stepDownLocked(reply.Term)
				return
			}
			if r.state != Leader || r.currentTerm != args.Term {
				return // 任期已变，这次回复作废
			}
			if reply.Success {
				if args.LastIncludedIndex > r.matchIndex[peer] {
					r.matchIndex[peer] = args.LastIncludedIndex
				}
				r.nextIndex[peer] = r.matchIndex[peer] + 1
				r.advanceCommitIndexLocked()
			}
			// 失败：nextIndex 不变，下一轮心跳继续发快照重试。
		})
		return
	}
	args := AppendEntriesArgs{
		Term:         r.currentTerm,
		LeaderID:     r.id,
		PrevLogIndex: next - 1,
		// next-1 >= snapshotIndex（上面的分支已排除更小的情况），
		// next-1 == snapshotIndex 时取到的就是占位条目（快照任期）。
		PrevLogTerm:  r.log[next-1-r.snapshotIndex].Term,
		LeaderCommit: r.commitIndex,
	}
	if next <= r.lastLogIndexLocked() {
		args.Entries = append([]LogEntry(nil), r.log[next-r.snapshotIndex:]...)
	}
	// 先排好下一次心跳，再发包：对端再慢也不影响心跳节奏。
	r.scheduleReplicateLocked(peer, HeartbeatInterval, term)
	r.mu.Unlock()

	addr, ok := r.addrs[peer]
	if !ok {
		return
	}
	r.transport.AppendEntries(addr, &args, func(reply *AppendEntriesReply, err error) {
		if err != nil {
			return // 消息丢了：等下一轮心跳重试
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.stopped {
			return
		}
		if reply.Term > r.currentTerm {
			r.stepDownLocked(reply.Term)
			return
		}
		if r.state != Leader || r.currentTerm != args.Term {
			return // 任期已变，这次回复作废
		}
		if reply.Success {
			r.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			r.nextIndex[peer] = r.matchIndex[peer] + 1
			r.advanceCommitIndexLocked()
		} else if reply.ConflictIndex > 0 {
			// 快速回退：按 follower 的提示直接跳 nextIndex
			// （见 AppendEntriesReply.ConflictIndex）。
			// 提示落在快照里时压到 snapshotIndex——
			// 下次心跳改发 InstallSnapshot。
			// 只往小里跳：过期的回复不能把 nextIndex 往前推
			// （会跳过 follower 还没确认的条目）。
			next := reply.ConflictIndex
			if next <= r.snapshotIndex {
				next = r.snapshotIndex
			}
			if next < r.nextIndex[peer] {
				r.nextIndex[peer] = next
			}
		} else if r.nextIndex[peer] > r.snapshotIndex+1 {
			// 日志不一致：一次回退一格再试（M1 求简单正确；
			// 快速回退是 M2 之后的优化，见规划风险 2）。
			// 下限是 snapshotIndex+1：再往下就是快照里，
			// 下次心跳改发 InstallSnapshot，不用继续回退。
			// 下次心跳发的是回退后的位置，不用立刻重发。
			r.nextIndex[peer]--
		}
	})
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
		r.armElectionTimerLocked()
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
	r.armElectionTimerLocked()

	// 一致性检查：PrevLogIndex 处的任期必须与 leader 说的一致。
	// PrevLogIndex 落在快照里（< snapshotIndex）时跳过检查——
	// 快照只覆盖已提交的条目，已提交的条目在各节点上一定相同，
	// 没法也不用再验；PrevLogIndex 恰好是快照位置时验快照任期。
	// 拒绝时都带 ConflictIndex（快速回退提示，见 AppendEntriesReply）。
	if args.PrevLogIndex > r.lastLogIndexLocked() {
		reply.Term = r.currentTerm
		reply.ConflictIndex = r.lastLogIndexLocked() + 1
		return reply
	}
	if args.PrevLogIndex == r.snapshotIndex {
		if args.PrevLogTerm != r.snapshotTerm {
			reply.Term = r.currentTerm
			// 快照任期对不上：leader 只能发 InstallSnapshot 覆盖，
			// 提示就指回快照位置（leader 看到它会把 nextIndex
			// 压到 snapshotIndex，下次心跳改发快照）。
			reply.ConflictIndex = r.snapshotIndex
			return reply
		}
	} else if args.PrevLogIndex > r.snapshotIndex {
		if r.log[args.PrevLogIndex-r.snapshotIndex].Term != args.PrevLogTerm {
			reply.Term = r.currentTerm
			// 任期冲突：回扫到该任期连续段的第一个下标——
			// leader 从那里重发，一次跳过整个冲突任期，
			// 而不是一格一格往回试。
			idx := args.PrevLogIndex - r.snapshotIndex
			term := r.log[idx].Term
			for idx > 0 && r.log[idx-1].Term == term {
				idx--
			}
			reply.ConflictIndex = r.snapshotIndex + idx
			return reply
		}
	}
	// 逐条合入：已有的相同任期条目跳过，冲突条目截断后覆盖，缺的追加。
	// 下标已在快照里的条目直接跳过（状态机里已有它们的效果）。
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= r.snapshotIndex {
			continue
		}
		local := idx - r.snapshotIndex
		if idx > r.lastLogIndexLocked() {
			r.log = append(r.log, args.Entries[i:]...)
			r.persistLocked()
			break
		}
		if r.log[local].Term != e.Term {
			r.log = append(r.log[:local], args.Entries[i:]...)
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
// 兜底 ticker 也走 Clock（模拟器里是虚拟时钟）。
func (r *Raft) runApplyLoop() {
	ticker := r.clock.NewTicker(applyTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.done:
			return
		case <-r.applyNotifyCh:
		case <-ticker.C():
		}
		for {
			r.mu.Lock()
			// 待投递的快照优先：先把快照交给状态机恢复，
			// 再应用它之后的日志——顺序不能乱，否则快照之后的
			// 日志会先于快照被应用，状态机就错了。
			// 快照投递后 lastApplied 直接跳到快照位置
			// （快照覆盖的条目不用再逐条投递）。
			if r.pendingSnap != nil {
				snap := r.pendingSnap
				r.pendingSnap = nil
				// 先落盘 lastApplied、再投递给状态机（顺序不能反）：
				// 状态机执行完、HTTP 回了 200，客户端就认为这条写稳固了；
				// 如果先投递后落盘，kill -9 落在两者之间时，
				// 重启后 lastApplied 还是旧值，快照位置的条目虽然在
				// 快照文件里，却因图 2 的提交规则（新 leader 不能用
				// 数人头的方式提交旧任期条目）迟迟进不了状态机，
				// 客户端刚读到的写就“丢”了。
				// 先落盘的话，崩溃在落盘与投递之间时，重启从快照恢复
				// （见 NewRaft 的钳制逻辑），不会丢、也不会多应用。
				r.lastApplied = snap.index
				r.persistMetaLocked()
				r.mu.Unlock()
				select {
				case r.applyCh <- ApplyMsg{Index: snap.index, Snapshot: snap.data}:
				case <-r.done:
					return
				}
				continue
			}
			if r.lastApplied >= r.commitIndex {
				r.mu.Unlock()
				break
			}
			r.lastApplied++
			msg := ApplyMsg{Index: r.lastApplied, Command: r.log[r.lastApplied-r.snapshotIndex].Command}
			// 先落盘 lastApplied、再投递（与快照路径同款理由）：
			// 注意“投递”不等于“状态机已执行”：崩溃在落盘与投递之间时，
			// 重启会把这条重放一遍——Put/Delete 都是幂等的，
			// 多应用一条是安全的；且 lastApplied 永远 ≤ commitIndex，
			// 重放的绝不会是未提交的尾部日志。
			// 反过来（先投递后落盘）会在“HTTP 已回 200、lastApplied
			// 还没落盘”的窗口里丢 durability（M5 测试抓到过）。
			r.persistMetaLocked()
			r.mu.Unlock()
			select {
			case r.applyCh <- msg:
			case <-r.done:
				return
			}
		}
	}
}

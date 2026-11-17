package sim

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"time"

	"github.com/OL486/raftkv/internal/kv"
	"github.com/OL486/raftkv/internal/raft"
)

// 模拟器的虚拟时间步进常量（驱动器与 Put 的等待循环用）。
const (
	// putLeaderWait 是 Put 找 leader 时，每轮推进的虚拟时间。
	putLeaderWait = 100 * time.Millisecond
	// putLeaderBudget 是 Put 找 leader 的虚拟时间上限（5 秒，
	// 够好几轮选举；选举超时本身是 150–300ms）。
	putLeaderBudget = 5 * time.Second
	// putCommitWait 是 Put 等一条日志提交的虚拟时间上限。
	putCommitWait = 10 * time.Second
	// putAdvanceStep 是 Put 等待提交时每次推进的虚拟时间。
	putAdvanceStep = 10 * time.Millisecond
	// drainTimeout 是排空 apply 队列的真实时间上限。
	drainTimeout = 10 * time.Second
)

// appliedEntry 是状态机实际应用的一条记录（不变式 (c) 用它核对顺序）。
type appliedEntry struct {
	index   int
	command []byte
}

// simNode 是模拟器里的一个节点：Raft 实例 + KV 状态机 + apply 消费端。
// alive=false 表示已被 Kill（Raft 与 Store 对象已丢，只剩 dataDir 与 restarts）。
type simNode struct {
	id       string
	addr     string
	raft     *raft.Raft
	store    *kv.Store
	applyCh  chan raft.ApplyMsg
	done     chan struct{}
	wg       sync.WaitGroup
	alive    bool
	dataDir  string
	restarts int
	rng      *rand.Rand

	appliedMu sync.Mutex
	applied   []appliedEntry
	// results 是下标 -> 应用结果（kv.ApplyResult 的 JSON），
	// consumeApply 与启动重放时记下；Sim.Read 靠它拿读命令
	// 在日志里应用那一刻的值（线性化点处的值）。
	results map[int][]byte
}

// appliedCopy 返回已应用序列的一份拷贝。
func (n *simNode) appliedCopy() []appliedEntry {
	n.appliedMu.Lock()
	defer n.appliedMu.Unlock()
	out := make([]appliedEntry, len(n.applied))
	copy(out, n.applied)
	return out
}

// consumeApply 是 apply 循环的消费端（node.consumeApply 的模拟器版）：
// 把已提交命令按序交给状态机，并记下应用顺序供不变式 (c) 检查。
func (n *simNode) consumeApply() {
	defer n.wg.Done()
	for {
		select {
		case msg := <-n.applyCh:
			result, err := n.store.Apply(msg.Command)
			if err != nil {
				// 日志里的命令都是合法的，出错即是 bug：记下来，
				// 不让整个节点停下（测试失败时再从这里查）。
				panic(fmt.Sprintf("节点 %s 应用日志 %d 失败：%v", n.id, msg.Index, err))
			}
			n.appliedMu.Lock()
			n.applied = append(n.applied, appliedEntry{index: msg.Index, command: append([]byte(nil), msg.Command...)})
			if n.results == nil {
				n.results = make(map[int][]byte)
			}
			n.results[msg.Index] = result
			n.appliedMu.Unlock()
		case <-n.done:
			return
		}
	}
}

// LeaderEvent 是观测到的 leader 变化（确定性对比用）。
type LeaderEvent struct {
	Term   int
	Leader string // "" 表示当前没有 leader
}

// Sim 是确定性模拟器：虚拟时钟 + 模拟网络 + 三个 Raft 节点。
// 全部决策都在 Advance 的调用栈里同步发生（见包注释）；
// 驱动器（FaultDriver）与测试只通过 Sim 的方法操作集群。
type Sim struct {
	seed    int64
	clock   *VirtualClock
	net     *Network
	ids     []string
	addrs   map[string]string
	baseDir string
	nodes   map[string]*simNode

	// driverRng 是故障驱动器的 RNG（种子由主种子派生）。
	driverRng *rand.Rand
	// committed 是“已提交前缀”水位线（不含哨兵条目）：历史上任何节点
	// commitIndex 达到过的最长前缀，只增不减；不变式 (b) 用它核对。
	committed []raft.LogEntry
	// leaderHist 是观测到的 leader 变化序列（每次 Advance 后采样）。
	leaderHist    []LeaderEvent
	lastLeader    LeaderEvent
	leaderSampled bool
}

// NewSim 建一个模拟器（不启动节点，调 Start 才启动）。
// seed 是主种子：网络、节点、驱动器的 RNG 都由它派生，
// 所以同一个 seed 跑两次，事件序列完全相同。
// baseDir 是数据目录的父目录（调用方传 t.TempDir()），
// 每个节点的数据目录是 baseDir/<id>。
// ids 是节点 id（按顺序），addrs 是 id -> host:port。
func NewSim(seed int64, baseDir string, ids []string, addrs map[string]string) *Sim {
	clock := NewVirtualClock()
	net := NewNetwork(clock, rand.New(rand.NewSource(seed*7+1)))
	return &Sim{
		seed:      seed,
		clock:     clock,
		net:       net,
		ids:       append([]string(nil), ids...),
		addrs:     addrs,
		baseDir:   baseDir,
		nodes:     make(map[string]*simNode),
		driverRng: rand.New(rand.NewSource(seed*13 + 5)),
	}
}

// Seed 返回主种子（驱动器与测试用）。
func (s *Sim) Seed() int64 { return s.seed }

// Clock 返回虚拟时钟（测试里直接推进时间用）。
func (s *Sim) Clock() *VirtualClock { return s.clock }

// Network 返回模拟网络（测试里调分区 / 丢包率用）。
func (s *Sim) Network() *Network { return s.net }

// Start 启动全部节点（各建一个 Raft 实例 + KV + apply 消费端）。
func (s *Sim) Start() error {
	for i, id := range s.ids {
		if err := s.startNode(id, i, 0); err != nil {
			return err
		}
	}
	return nil
}

// startNode 启动一个节点（新建或重启都走这里）。
// restarts 是该节点到目前为止重启过的次数（决定 RNG 种子的派生）。
func (s *Sim) startNode(id string, idx int, restarts int) error {
	addr, ok := s.addrs[id]
	if !ok {
		return fmt.Errorf("集群配置里没有节点 %q", id)
	}
	dataDir := filepath.Join(s.baseDir, id)
	rng := rand.New(rand.NewSource(nodeSeed(s.seed, idx, restarts)))
	persister, err := raft.NewFilePersister(dataDir)
	if err != nil {
		return err
	}
	applyCh := make(chan raft.ApplyMsg, 100)
	r, err := raft.NewRaft(id, s.ids, s.addrs, s.net.TransportFor(id), persister, applyCh,
		raft.WithClock(s.clock), raft.WithRand(rng))
	if err != nil {
		return err
	}
	n := &simNode{
		id:       id,
		addr:     addr,
		raft:     r,
		store:    kv.NewStore(),
		applyCh:  applyCh,
		done:     make(chan struct{}),
		alive:    true,
		dataDir:  dataDir,
		restarts: restarts,
		rng:      rng,
	}
	// 启动恢复：重放已应用的日志（只到 lastApplied——之后的条目
	// 等新 leader 的心跳带 LeaderCommit 过来后再正常应用）。
	logEntries, lastApplied := r.RestoreInfo()
	for i := 1; i <= lastApplied; i++ {
		result, err := n.store.Apply(logEntries[i].Command)
		if err != nil {
			return fmt.Errorf("节点 %s 重放日志 %d 失败：%w", id, i, err)
		}
		n.applied = append(n.applied, appliedEntry{index: i, command: append([]byte(nil), logEntries[i].Command...)})
		if n.results == nil {
			n.results = make(map[int][]byte)
		}
		n.results[i] = result
	}
	s.net.Register(id, addr, n)
	s.nodes[id] = n
	n.wg.Add(1)
	go n.consumeApply()
	r.Run()
	return nil
}

// Stop 停掉全部节点（测试收尾用）。
func (s *Sim) Stop() {
	for _, id := range s.ids {
		n := s.nodes[id]
		if n == nil || !n.alive {
			continue
		}
		s.stopNode(n)
	}
}

// stopNode 停掉一个节点的后台工作（Raft 计时/心跳/apply 循环 +
// apply 消费端），对象本身留着（Kill 还要读 dataDir 与 restarts）。
func (s *Sim) stopNode(n *simNode) {
	n.alive = false
	s.net.Unregister(n.id, n.addr)
	n.raft.Stop()
	close(n.done)
	n.wg.Wait()
}

// Advance 把虚拟时间推进 d，并采样 leader 变化。
// 推进过程中到期的定时器与消息都在这次调用里同步执行完。
func (s *Sim) Advance(d time.Duration) {
	s.clock.Advance(d)
	s.sampleLeader()
}

// Settle 推进虚拟时间 d，再排空各节点的 apply 队列。
// 调完后集群在虚拟时间上是静止的（没有待处理的事件），
// 各活节点的 lastApplied 都追上了自己的 commitIndex，
// 可以安全地读快照、断言不变式。
func (s *Sim) Settle(d time.Duration) error {
	s.Advance(d)
	return s.drain()
}

// drain 排空各活节点的 apply 队列（真实时间轮询，10 秒上限）。
// 只等“已提交的都应用了”，不等虚拟时间再推进——
// commitIndex 只在 Advance 里变，所以排空后状态是稳定的。
func (s *Sim) drain() error {
	deadline := time.Now().Add(drainTimeout)
	for {
		done := true
		for _, id := range s.ids {
			n := s.nodes[id]
			if n == nil || !n.alive {
				continue
			}
			dbg := n.raft.Debug()
			if len(n.appliedCopy()) != dbg.LastApplied || dbg.LastApplied != dbg.CommitIndex {
				done = false
				break
			}
		}
		if done {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("10 秒内 apply 队列没排空")
		}
		time.Sleep(time.Millisecond)
	}
}

// findLeader 返回当前 leader 的节点：活着的节点里任期最高的那个。
// 集群里可能短暂出现两个 leader（旧 leader 还没收到新任期的消息）：
// 任期高的那个才是合法的，任期低的那个一收到高任期消息就会退下。
// 没有 leader 时返回 nil。
func (s *Sim) findLeader() *simNode {
	var best *simNode
	bestTerm := -1
	for _, id := range s.ids {
		n := s.nodes[id]
		if n == nil || !n.alive {
			continue
		}
		term, isLeader := n.raft.GetState()
		if isLeader && term > bestTerm {
			best, bestTerm = n, term
		}
	}
	return best
}

// maxCommitIndex 返回各活节点 commitIndex 的最大值。
func (s *Sim) maxCommitIndex() int {
	max := 0
	for _, id := range s.ids {
		n := s.nodes[id]
		if n == nil || !n.alive {
			continue
		}
		if c := n.raft.Debug().CommitIndex; c > max {
			max = c
		}
	}
	return max
}

// minCommitIndex 返回各活节点 commitIndex 的最小值
// （没有活节点时返回 0）。
func (s *Sim) minCommitIndex() int {
	min := -1
	for _, id := range s.ids {
		n := s.nodes[id]
		if n == nil || !n.alive {
			continue
		}
		if c := n.raft.Debug().CommitIndex; min < 0 || c < min {
			min = c
		}
	}
	if min < 0 {
		return 0
	}
	return min
}

// Put 往集群写一条 Put：找 leader → Start → 等提交。
// 找 leader 与等提交都靠推进虚拟时间（不靠真实等待），
// 所以同样的种子、同样的调用序列，结果完全相同。
// 成功返回日志下标；集群在 10 秒虚拟时间内提交不了
// （比如多数派都死了 / leader 被孤立）时返回错误——
// 这时不要立刻重试同一条命令（它可能已在某个多数派的日志里，
// 日后还会提交；重试会让日志里多一条重复命令）。
// Put 向集群写一条记录（key=value），返回它在日志里的下标。
// 语义：写进了 leader 的日志且被多数派提交后返回成功；
// 提交后还会等一会（最多 500ms 虚拟时间），让各活节点的
// commitIndex 追上来，这样 Put 一返回就能在所有活节点上读到它。
// 注意：调用时集群里可能短暂有两个 leader（旧 leader 还没收到
// 新任期的消息）。Put 先找任期最高的 leader 写；如果 2 秒虚拟
// 时间内没提交（多半是写到了被孤立的旧 leader），就重新找
// leader——如果 leader 换人了，在新 leader 上再写一次
// （旧 leader 那条未提交的记录会在它重进集群时被截掉，
// 不会进最终已提交日志）；如果 leader 没换，就继续等满 10 秒。
// 同一个 key 写两次是幂等的（值相同），所以重试不会弄脏 KV。
func (s *Sim) Put(key, value string) (int, error) {
	cmd := kv.EncodePut(key, value)
	index, leaderID, committed := s.tryPut(cmd, 2*time.Second)
	if committed {
		return s.waitVisible(index)
	}
	if index == 0 {
		// 连 leader 都没找到（或 Start 被拒）：写根本没开始。
		return 0, fmt.Errorf("写入 %q：5 秒虚拟时间内没有可用的 leader", key)
	}
	// 第一次没提交：看 leader 是不是换人了。
	if l2 := s.findLeader(); l2 != nil && l2.id != leaderID {
		if index2, _, committed2 := s.tryPut(cmd, putCommitWait); committed2 {
			return s.waitVisible(index2)
		}
		return 0, fmt.Errorf("写入 %q 在新 leader %s 上 10 秒虚拟时间内也没提交", key, l2.id)
	}
	// leader 没换：继续等第一条提交满 10 秒。
	for waited := 2 * time.Second; waited < putCommitWait; waited += putAdvanceStep {
		s.Advance(putAdvanceStep)
		if s.maxCommitIndex() >= index {
			return s.waitVisible(index)
		}
	}
	return 0, fmt.Errorf("写入 %q（下标 %d）10 秒虚拟时间内没提交", key, index)
}

// tryPut 找当前 leader（最多等 5 秒虚拟时间），把 cmd 写进它的日志，
// 再等 budget 虚拟时间看它提交没有。返回（下标，leader id，是否提交）。
// 没找到 leader 或 leader 拒绝时返回下标 0。
func (s *Sim) tryPut(cmd []byte, budget time.Duration) (int, string, bool) {
	var leader *simNode
	for waited := time.Duration(0); waited < putLeaderBudget; waited += putLeaderWait {
		if l := s.findLeader(); l != nil {
			leader = l
			break
		}
		s.Advance(putLeaderWait)
	}
	if leader == nil {
		return 0, "", false
	}
	index, _, ok := leader.raft.Start(cmd)
	if !ok {
		// leader 在找和写之间退位了：推进一小段再试一次。
		s.Advance(putAdvanceStep)
		leader = s.findLeader()
		if leader == nil {
			return 0, "", false
		}
		index, _, ok = leader.raft.Start(cmd)
		if !ok {
			return 0, "", false
		}
	}
	for waited := time.Duration(0); waited < budget; waited += putAdvanceStep {
		s.Advance(putAdvanceStep)
		if s.maxCommitIndex() >= index {
			return index, leader.id, true
		}
	}
	return index, leader.id, false
}

// waitVisible 等各活节点的 commitIndex 追上 index
// （最多 500ms 虚拟时间；被分区隔离的节点追不上，预算用完
// 就直接返回成功——写本身已提交），再排空应用队列。
func (s *Sim) waitVisible(index int) (int, error) {
	for waited := time.Duration(0); waited < 500*time.Millisecond; waited += putAdvanceStep {
		if s.minCommitIndex() >= index {
			break
		}
		s.Advance(putAdvanceStep)
	}
	if err := s.drain(); err != nil {
		return 0, err
	}
	return index, nil
}

// Read 是线性化读：把 Read 命令走 Raft 日志（与 Put 同样的提交路径），
// 返回它在日志里应用那一刻的值。实现复用 tryPut/waitVisible：
// Read 命令在状态机里是 no-op，所以重试（多追加一条读命令）
// 不会弄脏 KV；返回的值取自应用结果记录，不是事后读 Store，
// 事后读可能读到更晚的 Put。
func (s *Sim) Read(key string) (string, bool, error) {
	cmd := kv.EncodeRead(key)
	index, leaderID, committed := s.tryPut(cmd, 2*time.Second)
	if committed {
		if _, err := s.waitVisible(index); err != nil {
			return "", false, err
		}
		return s.readResult(index)
	}
	if index == 0 {
		// 连 leader 都没找到（或 Start 被拒）：读根本没开始。
		return "", false, fmt.Errorf("读 %q：5 秒虚拟时间内没有可用的 leader", key)
	}
	// 第一次没提交：看 leader 是不是换人了。
	if l2 := s.findLeader(); l2 != nil && l2.id != leaderID {
		if index2, _, committed2 := s.tryPut(cmd, putCommitWait); committed2 {
			if _, err := s.waitVisible(index2); err != nil {
				return "", false, err
			}
			return s.readResult(index2)
		}
		return "", false, fmt.Errorf("读 %q 在新 leader %s 上 10 秒虚拟时间内也没提交", key, l2.id)
	}
	// leader 没换：继续等第一条提交满 10 秒。
	for waited := 2 * time.Second; waited < putCommitWait; waited += putAdvanceStep {
		s.Advance(putAdvanceStep)
		if s.maxCommitIndex() >= index {
			if _, err := s.waitVisible(index); err != nil {
				return "", false, err
			}
			return s.readResult(index)
		}
	}
	return "", false, fmt.Errorf("读 %q（下标 %d）10 秒虚拟时间内没提交", key, index)
}

// readResult 从任一活节点取下标 index 的应用结果
// （各节点按序应用同一条日志，同一位置的结果相同）。
// waitVisible 已保证该下标被应用，找不到结果即是 bug。
func (s *Sim) readResult(index int) (string, bool, error) {
	res := s.AppliedResult(index)
	if res == nil {
		return "", false, fmt.Errorf("下标 %d 已提交但没有节点记下应用结果", index)
	}
	ar, err := kv.DecodeApplyResult(res)
	if err != nil {
		return "", false, err
	}
	return ar.Value, ar.Found, nil
}

// AppliedResult 返回下标 index 的应用结果（任一活节点记下的），
// 没有活节点应用过该下标时返回 nil。
func (s *Sim) AppliedResult(index int) []byte {
	for _, id := range s.ids {
		n := s.nodes[id]
		if n == nil || !n.alive {
			continue
		}
		n.appliedMu.Lock()
		res, ok := n.results[index]
		n.appliedMu.Unlock()
		if ok {
			return res
		}
	}
	return nil
}

// Get 读某节点的本地状态机（可能读到旧数据，与真实节点一致）。
// 节点已死时返回 found=false。
func (s *Sim) Get(id, key string) (string, bool) {
	n := s.nodes[id]
	if n == nil || !n.alive {
		return "", false
	}
	return n.store.Get(key)
}

// KV 返回某节点状态机的全量拷贝（测试比较各节点一致性用）。
// 节点已死时返回 nil。
func (s *Sim) KV(id string) map[string]string {
	n := s.nodes[id]
	if n == nil || !n.alive {
		return nil
	}
	return n.store.All()
}

// CommittedLog 返回已提交前缀水位线的一份拷贝（不含哨兵条目），
// 确定性对比用：同一种子跑两次，它必须完全相同。
func (s *Sim) CommittedLog() []raft.LogEntry {
	out := make([]raft.LogEntry, len(s.committed))
	copy(out, s.committed)
	return out
}

// LeaderHistory 返回观测到的 leader 变化序列（确定性对比的参考）。
func (s *Sim) LeaderHistory() []LeaderEvent {
	out := make([]LeaderEvent, len(s.leaderHist))
	copy(out, s.leaderHist)
	return out
}

// sampleLeader 在每次 Advance 后采样当前 leader，变化了就记下来。
func (s *Sim) sampleLeader() {
	leader := ""
	term := 0
	for _, id := range s.ids {
		n := s.nodes[id]
		if n == nil || !n.alive {
			continue
		}
		dbg := n.raft.Debug()
		if dbg.Term > term {
			term = dbg.Term
		}
		if dbg.Role == "leader" && leader == "" {
			leader = id
		}
	}
	cur := LeaderEvent{Term: term, Leader: leader}
	if !s.leaderSampled || cur != s.lastLeader {
		s.leaderHist = append(s.leaderHist, cur)
		s.lastLeader = cur
		s.leaderSampled = true
	}
}

// Kill 停掉一个节点（模拟 kill -9）：内存状态丢，磁盘文件保留。
// 它的 Raft 对象与 Store 都丢弃，只留 dataDir 与重启次数；
// 排队的定时器由 Raft.Stop() 停掉，发往它的消息由网络在投递时丢掉，
// 它发出去还没回的 RPC 回调靠 stopped 标志直接返回。
func (s *Sim) Kill(id string) error {
	n := s.nodes[id]
	if n == nil {
		return fmt.Errorf("没有节点 %q", id)
	}
	if !n.alive {
		return fmt.Errorf("节点 %q 已经死了", id)
	}
	s.stopNode(n)
	n.raft = nil
	n.store = nil
	return nil
}

// Restart 从磁盘恢复重进一个已 Kill 的节点（模拟重启）。
// 新 Raft 从同一 dataDir 的 FilePersister Load 恢复，
// RNG 种子由（主种子，节点下标，重启次数）派生，保证可复现。
func (s *Sim) Restart(id string) error {
	n := s.nodes[id]
	if n == nil {
		return fmt.Errorf("没有节点 %q", id)
	}
	if n.alive {
		return fmt.Errorf("节点 %q 还活着，不用重启", id)
	}
	idx := 0
	for i, x := range s.ids {
		if x == id {
			idx = i
			break
		}
	}
	return s.startNode(id, idx, n.restarts+1)
}

// Partition 把 a 孤立出来（{a} 一组，其余节点一组），见 Network.Partition。
func (s *Sim) Partition(a, b string) {
	s.net.Partition(a, b)
}

// SetPartition 按给定的分组设置分区。
func (s *Sim) SetPartition(groups [][]string) {
	s.net.SetPartition(groups)
}

// Heal 恢复全连通。
func (s *Sim) Heal() {
	s.net.Heal()
}

// IDs 返回节点 id（按创建顺序）。
func (s *Sim) IDs() []string {
	return append([]string(nil), s.ids...)
}

// Alive 返回节点是否活着。
func (s *Sim) Alive(id string) bool {
	n := s.nodes[id]
	return n != nil && n.alive
}

// Debug 返回某节点的 Debug 快照（测试用；死了的节点返回零值）。
func (s *Sim) Debug(id string) raft.DebugInfo {
	n := s.nodes[id]
	if n == nil || !n.alive {
		return raft.DebugInfo{ID: id}
	}
	return n.raft.Debug()
}

// nodeSeed 由主种子派生节点 RNG 的种子：
// 不同节点、不同重启次数的种子都不同，但同样的（主种子，节点，次数）
// 永远派生出同一个种子——重启多少次，结果都可复现。
func nodeSeed(seed int64, idx, restarts int) int64 {
	return seed*100000 + int64(idx)*1000 + int64(restarts)
}

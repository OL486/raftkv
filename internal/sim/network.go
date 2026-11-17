package sim

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/OL486/raftkv/internal/raft"
)

// Network 是 raft.Transport 的模拟实现：进程内消息队列。
//
// 发送是异步的：Send 只把消息排进虚拟时钟的事件队列就返回，
// 回复在投递时（虚拟时间到了）同步调 cb——全部发生在 Advance 的
// 调用栈里，不经过 goroutine 调度，所以同一种子跑两次，
// 消息的投递顺序完全相同。
//
// 可编程的故障（都在发送时按 RNG 判定，判定顺序即发送顺序，
// 所以是确定的）：
//   - 分区：两个节点不在同一组时，消息直接丢掉；
//   - 丢包：按 dropRate 的概率丢掉；
//   - 延迟：投递时间 = 发送时间 + [minLatency, maxLatency] 内的均匀随机值。
//
// 丢掉的消息 cb 永远不会被调用（与真实网络的超时等价）：
// 调用方靠下一轮心跳 / 选举重试兜底。
type Network struct {
	clock *VirtualClock
	rng   *rand.Rand

	// nodes 是节点注册表（id -> 节点），投递时查。
	// 节点对象本身由 Sim 持有；这里只存指针。
	nodes map[string]*simNode
	// addrToID 是地址到节点 id 的映射（Transport 接口只给 addr）。
	addrToID map[string]string

	dropRate   float64
	minLatency time.Duration
	maxLatency time.Duration

	// groups 是当前的分区（每组是一组能互相通信的节点 id）。
	// nil 表示全连通；Heal() 后恢复成 nil。
	groups [][]string

	// snapshotDeliveries 是成功投递的 InstallSnapshot 次数
	// （分区/丢包丢掉的不算）：M5 测试靠它观测“落后 follower
	// 是不是真的走了快照追赶，而不是逐条复制”。
	// 只在 Advance 的调用栈里读写，与事件执行同线程，测试在
	// Advance 返回后读，不用加锁。
	snapshotDeliveries int
}

// NewNetwork 建一个模拟网络。rng 是网络专用的 RNG（种子由主种子派生），
// 所有丢包 / 延迟判定都从它取数；不要与节点、驱动器的 RNG 混用。
func NewNetwork(clock *VirtualClock, rng *rand.Rand) *Network {
	return &Network{
		clock:      clock,
		rng:        rng,
		nodes:      make(map[string]*simNode),
		addrToID:   make(map[string]string),
		minLatency: 0,
		maxLatency: 5 * time.Millisecond,
	}
}

// SetDropRate 设丢包率（0–1）。测试里用 1.0 模拟全断网。
func (n *Network) SetDropRate(p float64) {
	n.dropRate = p
}

// SetLatency 设消息延迟区间（均匀分布，含端点）。
func (n *Network) SetLatency(min, max time.Duration) {
	n.minLatency = min
	n.maxLatency = max
}

// Register 把一个节点挂到网络上（id 与地址都要登记）。
func (n *Network) Register(id, addr string, node *simNode) {
	n.nodes[id] = node
	n.addrToID[addr] = id
}

// Unregister 把节点从网络上摘掉（Kill 时调；之后发往它的消息直接丢掉）。
func (n *Network) Unregister(id, addr string) {
	delete(n.nodes, id)
	delete(n.addrToID, addr)
}

// TransportFor 返回绑定了本节点 id 的 Transport（Raft 核心用它发 RPC）。
// 每个节点拿到的 Transport 都知道自己是谁，分区判断靠它。
func (n *Network) TransportFor(selfID string) raft.Transport {
	return &nodeTransport{net: n, selfID: selfID}
}

// nodeTransport 是绑定了发送方 id 的 Transport。
type nodeTransport struct {
	net    *Network
	selfID string
}

// RequestVote 发投票请求（异步，回复到了调 cb）。
func (t *nodeTransport) RequestVote(addr string, args *raft.RequestVoteArgs, cb func(*raft.RequestVoteReply, error)) {
	t.net.send(t.selfID, addr, func(dst *simNode) {
		cb(dst.raft.HandleRequestVote(args), nil)
	})
}

// AppendEntries 发日志复制 / 心跳请求（异步，回复到了调 cb）。
func (t *nodeTransport) AppendEntries(addr string, args *raft.AppendEntriesArgs, cb func(*raft.AppendEntriesReply, error)) {
	t.net.send(t.selfID, addr, func(dst *simNode) {
		cb(dst.raft.HandleAppendEntries(args), nil)
	})
}

// InstallSnapshot 发快照安装请求（异步，回复到了调 cb）。
// 投递成功时计数（M5 测试的观测点：落后 follower 是不是走了快照追赶）。
func (t *nodeTransport) InstallSnapshot(addr string, args *raft.InstallSnapshotArgs, cb func(*raft.InstallSnapshotReply, error)) {
	t.net.send(t.selfID, addr, func(dst *simNode) {
		t.net.snapshotDeliveries++
		cb(dst.raft.HandleInstallSnapshot(args), nil)
	})
}

// send 是两种 RPC 的公共发送路径：分区 / 丢包判定 + 排投递事件。
// deliver 在投递时执行：查目标节点（还活着才调 handler）、
// 调 handler 拿回复、同步调 cb。整个 deliver 都在 Advance 的调用栈里。
func (n *Network) send(fromID, addr string, deliver func(dst *simNode)) {
	toID, ok := n.addrToID[addr]
	if !ok {
		return // 目标地址没登记：当作发丢了
	}
	if !n.canTalk(fromID, toID) {
		return // 分区：组间消息全丢
	}
	if n.dropRate > 0 && n.rng.Float64() < n.dropRate {
		return // 丢包
	}
	latency := n.minLatency
	if n.maxLatency > n.minLatency {
		latency += time.Duration(n.rng.Int63n(int64(n.maxLatency - n.minLatency + 1)))
	}
	n.clock.Post(latency, func() {
		dst, ok := n.nodes[toID]
		if !ok || !dst.alive {
			return // 目标已死（Kill 后）：消息丢掉
		}
		deliver(dst)
	})
}

// canTalk 判断两个节点现在能不能通信（同一分区组内才行）。
func (n *Network) canTalk(a, b string) bool {
	if len(n.groups) == 0 {
		return true
	}
	for _, g := range n.groups {
		inA, inB := false, false
		for _, id := range g {
			if id == a {
				inA = true
			}
			if id == b {
				inB = true
			}
		}
		if inA && inB {
			return true
		}
	}
	return false
}

// SetPartition 按给定的分组设置分区（每组是一组能互相通信的节点）。
// groups 必须覆盖全部节点（漏掉的节点将被孤立——调用方应保证覆盖）。
func (n *Network) SetPartition(groups [][]string) {
	n.groups = groups
}

// Partition 把 a 孤立出来（{a} 一组，其余节点一组）。
// 三节点集群里一切“二分”都是 1 对 2，所以这一个调用就覆盖了
// “单节点孤立”与“2 对 1 分裂”两种场景；b 是另一侧的代表节点，
// 仅用于校验（b 必须存在且不是 a）。
func (n *Network) Partition(a, b string) {
	if a == b {
		panic(fmt.Sprintf("Partition 参数错误：a 与 b 不能是同一个节点 %q", a))
	}
	if _, ok := n.nodes[a]; !ok {
		panic(fmt.Sprintf("Partition 参数错误：没有节点 %q", a))
	}
	if _, ok := n.nodes[b]; !ok {
		panic(fmt.Sprintf("Partition 参数错误：没有节点 %q", b))
	}
	rest := make([]string, 0, len(n.nodes)-1)
	for id := range n.nodes {
		if id != a {
			rest = append(rest, id)
		}
	}
	// 排个序：Go 的 map 遍历顺序是随机的，rest 的顺序虽然不影响
	// canTalk 的判断，但 Groups() 返回的结果应是确定的。
	sort.Strings(rest)
	n.groups = [][]string{{a}, rest}
}

// Heal 恢复全连通。
func (n *Network) Heal() {
	n.groups = nil
}

// Groups 返回当前分区的一份拷贝（nil 表示全连通；测试用）。
func (n *Network) Groups() [][]string {
	out := make([][]string, len(n.groups))
	for i, g := range n.groups {
		out[i] = append([]string(nil), g...)
	}
	return out
}

// SnapshotDeliveries 返回成功投递的 InstallSnapshot 次数（测试用）。
func (n *Network) SnapshotDeliveries() int {
	return n.snapshotDeliveries
}

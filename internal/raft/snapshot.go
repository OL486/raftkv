package raft

import (
	"fmt"
	"log"
)

// 快照触发阈值（M5）：自上次快照以来已应用的条目达到这么多，
// 就拍一次快照、把日志压缩到该位置。
//
// 选 5000 的理由：验收要求“写 100k 条后日志条目数 < 5000”——
// 快照后保留的日志 = lastLogIndex - snapshotIndex，
// 拍完那一刻是 0，随后每应用一条长一条，到下次快照前最多
// 4999 条（消费端每应用一条就检查一次，见 node/sim 的 consumeApply）。
// 规划里写的“日志超过 10,000 条且 commitIndex - snapshotIndex > 5000”
// 达不到它自己的 < 5000 验收（保留量会冲到 ~10000），所以这里改成 5000，
// 见 docs/02-persistence.md。
const SnapshotThreshold = 5000

// pendingSnapshot 是已安装、等 apply 循环投递给状态机的快照。
// InstallSnapshot 只改 Raft 自己的日志与快照位置，
// 状态机恢复快照这一步必须走 applyCh、按序进行——
// 不能在 handler 里直接碰状态机（Raft 核心不拥有状态机），
// 也不能跳过它（否则快照之后的日志会先于快照被应用）。
type pendingSnapshot struct {
	index int
	data  []byte
}

// InstallSnapshotArgs 是 InstallSnapshot RPC 的参数。
type InstallSnapshotArgs struct {
	Term              int    `json:"term"`
	LeaderID          string `json:"leaderId"`
	LastIncludedIndex int    `json:"lastIncludedIndex"`
	LastIncludedTerm  int    `json:"lastIncludedTerm"`
	Data              []byte `json:"data"`
}

// InstallSnapshotReply 是 InstallSnapshot RPC 的回复。
type InstallSnapshotReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

// SnapshotIndex 返回当前快照覆盖的最后一条日志下标（0 表示从未快照）。
// node/sim 的 consumeApply 用它算“自上次快照以来应用了多少条”，
// 决定要不要拍新快照。
func (r *Raft) SnapshotIndex() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshotIndex
}

// TakeSnapshot 在下标 index 处拍快照、把日志压缩到该位置。
// data 是状态机在 index 处的全量（调用方刚把 index 应用到状态机后
// 取的 Store.Snapshot()，内容与 1..index 的日志完全对应）。
// 只压缩到 lastApplied 为止——绝不能压缩还没应用的条目。
//
// 顺序是“先写快照文件、再改内存日志、再落盘日志”：
// 写快照文件与改内存之间崩溃，日志还没压缩，快照文件是孤儿，
// Load 会忽略它；改内存与落盘之间崩溃，persistLocked 是 fail-stop，
// 重启后按“日志首条目 + 快照文件”恢复（见 deriveSnapshotBase）。
// index <= 已有快照时是空操作（返回 nil）；
// index > lastApplied 时返回错误（调用方 bug：不能压缩还没应用的日志）。
func (r *Raft) TakeSnapshot(index int, data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index <= r.snapshotIndex {
		return nil
	}
	if index > r.lastApplied {
		return fmt.Errorf("快照下标 %d 超出已应用的 %d：不能压缩还没应用的日志", index, r.lastApplied)
	}
	if len(data) == 0 {
		return fmt.Errorf("快照数据为空，拒绝保存")
	}
	term := r.log[index-r.snapshotIndex].Term
	snap := Snapshot{
		LastIncludedIndex: index,
		LastIncludedTerm:  term,
		Data:              append([]byte(nil), data...),
	}
	if err := r.persister.SaveSnapshot(snap); err != nil {
		return fmt.Errorf("保存快照失败：%w", err)
	}
	// 压缩内存日志：首条目换成快照位置的占位条目（快照任期、空命令），
	// index 之后的条目原样保留。注意 nextIndex/matchIndex 都是绝对下标，
	// 不用动；Debug().LogLen 会自动变小（len(log)-1 是保留量）。
	newLog := make([]LogEntry, 0, len(r.log)-(index-r.snapshotIndex))
	newLog = append(newLog, LogEntry{Term: term})
	newLog = append(newLog, r.log[index-r.snapshotIndex+1:]...)
	r.log = newLog
	r.snapshotIndex = index
	r.snapshotTerm = term
	r.snapshotData = snap.Data
	r.persistLocked()
	return nil
}

// HandleInstallSnapshot 处理 leader 发来的快照（论文 §7）。
// follower 落后太多、leader 手里的日志已经压缩到它前面时，
// leader 不再逐条发 AppendEntries，改发整个快照。
//
// 接受条件：快照比本地已有的新（LastIncludedIndex > snapshotIndex），
// 否则直接忽略（返回成功——旧快照不用重复安装）。
// 安装时：替换快照、截断日志、commitIndex 推进到快照位置；
// lastApplied 不在这里直接推——状态机恢复快照必须走 applyCh 按序进行，
// 所以挂一个待投递的快照，让 apply 循环先把快照交给状态机，
// 再应用它之后的日志。如果这段日志本地早已应用过
// （lastApplied 已超过快照位置），状态机里已经有这份数据，
// 就不用再恢复一次，只压缩日志。
// 快照落盘（SaveSnapshot + persistLocked）成功后才回成功——
// leader 收到成功才敢把 matchIndex 推进到快照位置。
func (r *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	reply := &InstallSnapshotReply{}
	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		return reply
	}
	if args.Term > r.currentTerm {
		r.stepDownLocked(args.Term)
	}
	// 同任期的合法 leader 出现：退回 follower（与 HandleAppendEntries 同款）。
	if r.state != Follower {
		r.state = Follower
	}
	r.leaderID = args.LeaderID
	r.armElectionTimerLocked()
	reply.Term = r.currentTerm

	if args.LastIncludedIndex <= r.snapshotIndex {
		reply.Success = true // 已有更新的快照，旧快照直接忽略
		return reply
	}
	if r.pendingSnap != nil && args.LastIncludedIndex <= r.pendingSnap.index {
		reply.Success = true // 已在等一个更新的快照投递，不用重复安装
		return reply
	}
	if len(args.Data) == 0 {
		log.Printf("节点 %s 拒绝安装空快照（来自 leader %s，下标 %d）", r.id, args.LeaderID, args.LastIncludedIndex)
		return reply
	}
	// 先落盘快照文件，再改内存状态——崩溃在中间时，
	// 日志还没压缩，快照文件是孤儿，Load 会忽略它（见 deriveSnapshotBase）。
	snap := Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              append([]byte(nil), args.Data...),
	}
	if err := r.persister.SaveSnapshot(snap); err != nil {
		log.Printf("节点 %s 保存快照失败：%v", r.id, err)
		return reply
	}
	// 截断日志：保留 LastIncludedIndex 之后、且与快照同一分支的条目——
	// 本地该下标的任期与快照任期一致时才保留之后的部分，
	// 否则全丢（那些条目在分叉的分支上，不可能已提交）。
	newLog := []LogEntry{{Term: args.LastIncludedTerm}}
	if idx := args.LastIncludedIndex - r.snapshotIndex; idx >= 0 && idx < len(r.log) && r.log[idx].Term == args.LastIncludedTerm {
		newLog = append(newLog, r.log[idx+1:]...)
	}
	r.log = newLog
	r.snapshotIndex = args.LastIncludedIndex
	r.snapshotTerm = args.LastIncludedTerm
	r.snapshotData = snap.Data
	if args.LastIncludedIndex > r.commitIndex {
		r.commitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > r.lastApplied {
		r.pendingSnap = &pendingSnapshot{index: args.LastIncludedIndex, data: snap.Data}
		r.notifyApplyLocked()
	}
	r.persistLocked()
	log.Printf("节点 %s 安装快照：lastIncludedIndex=%d lastIncludedTerm=%d（来自 leader %s）", r.id, args.LastIncludedIndex, args.LastIncludedTerm, args.LeaderID)
	reply.Success = true
	return reply
}

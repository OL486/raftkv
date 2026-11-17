package sim

import (
	"bytes"
	"fmt"

	"github.com/OL486/raftkv/internal/raft"
)

// 不变式（每次检查都跑，见规划“模拟器”一节）：
//   (a) 同一任期最多一个 leader；
//   (b) 已提交的日志条目永不丢失；
//   (c) 状态机按序应用。
//
// 检查前先 drain：把各节点 apply 循环里攒着的已提交日志都应用完，
// 再读快照——否则读到的只是“还没来得及应用”的中间态。

// CheckInvariants 先排空各节点的 apply 队列，再跑三个不变式检查。
// 任一检查失败都返回错误（错误里写清是哪个不变式、涉及哪个节点 /
// 哪个下标）；三个都成立返回 nil。驱动器每步之后都调它。
func (s *Sim) CheckInvariants() error {
	if err := s.drain(); err != nil {
		return fmt.Errorf("排空 apply 队列失败：%w", err)
	}
	snap := s.snapshot()
	if err := s.checkOneLeaderPerTerm(snap); err != nil {
		return err
	}
	if err := s.checkCommittedLogNeverLost(snap); err != nil {
		return err
	}
	if err := s.checkApplyInOrder(snap); err != nil {
		return err
	}
	return nil
}

// nodeSnapshot 是一个节点在某一时刻的状态快照（不变式检查只读它）。
type nodeSnapshot struct {
	id          string
	alive       bool
	term        int
	role        string
	leader      string
	log         []raft.LogEntry // 含下标 0 的哨兵条目
	commitIndex int
	lastApplied int
	applied     []appliedEntry // 状态机已应用的序列（按应用顺序）
}

// snapshot 读所有节点的快照（按 s.ids 的顺序，确定的）。
// 死了的节点只记 alive=false，不读它的 Raft（对象已丢）。
func (s *Sim) snapshot() []nodeSnapshot {
	out := make([]nodeSnapshot, 0, len(s.ids))
	for _, id := range s.ids {
		n := s.nodes[id]
		if n == nil || !n.alive {
			out = append(out, nodeSnapshot{id: id, alive: false})
			continue
		}
		dbg := n.raft.Debug()
		log, commit, _ := n.raft.LogSnapshot()
		out = append(out, nodeSnapshot{
			id:          id,
			alive:       true,
			term:        dbg.Term,
			role:        dbg.Role,
			leader:      dbg.Leader,
			log:         log,
			commitIndex: commit,
			lastApplied: dbg.LastApplied,
			applied:     n.appliedCopy(),
		})
	}
	return out
}

// checkOneLeaderPerTerm 是不变式 (a)：同一任期最多一个 leader。
// 只看活着的节点（死了的节点不算——它已经不在集群里了）。
func (s *Sim) checkOneLeaderPerTerm(snap []nodeSnapshot) error {
	leaders := make(map[int][]string)
	for _, n := range snap {
		if !n.alive || n.role != "leader" {
			continue
		}
		leaders[n.term] = append(leaders[n.term], n.id)
	}
	for term, ids := range leaders {
		if len(ids) > 1 {
			return fmt.Errorf("不变式 (a) 破坏：任期 %d 有 %d 个 leader（%v）", term, len(ids), ids)
		}
	}
	return nil
}

// checkCommittedLogNeverLost 是不变式 (b)：已提交的日志条目永不丢失。
// 做法：维护一条“已提交前缀”水位线 committed——它是历史上
// 任何节点 commitIndex 达到过的最长前缀（条目一旦提交就永不消失，
// 所以水位线只增不减）；每次检查都断言：每个活着的节点，
// 在它已提交前缀（下标 ≤ 它自己的 commitIndex）的范围内，
// 条目必须与水位线逐条相同（任期 + 命令都相同）。
// 两个关键点：
//   - 水位线只能从 commitIndex 达到当前最大的那个节点取——
//     它在该范围内的条目是已提交的；从别的节点取会拿到
//     未提交的旧条目，污染水位线。
//   - 比较只到各节点的 commitIndex：未提交的尾部条目在各节点上
//     可以不同（旧 leader 留下的、还没被新 leader 截掉的），
//     这是 Raft 允许的；拿全日志去比会把“落后节点的旧尾巴”
//     误判成安全破坏。
//
// 由此也推出：任意两个活节点在 min(commitIndex) 之前的已提交前缀
// 逐条一致（都与水位线一致）。
func (s *Sim) checkCommittedLogNeverLost(snap []nodeSnapshot) error {
	// 先找当前最大的 commitIndex，并把水位线扩展到它。
	maxCommit := len(s.committed)
	for _, n := range snap {
		if !n.alive {
			continue
		}
		if n.commitIndex > maxCommit {
			maxCommit = n.commitIndex
		}
	}
	if maxCommit > len(s.committed) {
		// 从 commitIndex 达到 maxCommit 的节点取水位线的新段
		// （它的这段条目是已提交的）。
		extended := false
		for _, n := range snap {
			if !n.alive || n.commitIndex < maxCommit {
				continue
			}
			for i := len(s.committed) + 1; i <= maxCommit; i++ {
				s.committed = append(s.committed, n.log[i])
			}
			extended = true
			break
		}
		if !extended {
			return fmt.Errorf("不变式 (b) 内部错误：commitIndex 已到 %d，但没有节点的 commitIndex 有这么大", maxCommit)
		}
	}
	// 断言每个活节点的已提交前缀与水位线一致。
	for _, n := range snap {
		if !n.alive {
			continue
		}
		limit := n.commitIndex
		if len(s.committed) < limit {
			limit = len(s.committed)
		}
		if len(n.log)-1 < limit {
			limit = len(n.log) - 1
		}
		for i := 1; i <= limit; i++ {
			want := s.committed[i-1]
			got := n.log[i]
			if got.Term != want.Term || !bytes.Equal(got.Command, want.Command) {
				return fmt.Errorf("不变式 (b) 破坏：节点 %s 的已提交日志下标 %d 是 {term=%d, cmd=%q}，已提交水位线是 {term=%d, cmd=%q}",
					n.id, i, got.Term, got.Command, want.Term, want.Command)
			}
		}
	}
	return nil
}

// checkApplyInOrder 是不变式 (c)：状态机按序应用。
// 每个活节点的已应用序列必须是下标 1, 2, 3, … 严格递增、
// 无跳号，且每条应用的命令必须与该节点日志同一位置的条目相同。
// （lastApplied 单调递增由 Raft 的 apply 循环保证，
// 这里检查的是“状态机实际应用的顺序与内容”。）
func (s *Sim) checkApplyInOrder(snap []nodeSnapshot) error {
	for _, n := range snap {
		if !n.alive {
			continue
		}
		for k, a := range n.applied {
			wantIndex := k + 1
			if a.index != wantIndex {
				return fmt.Errorf("不变式 (c) 破坏：节点 %s 第 %d 次应用的下标是 %d，想要 %d（跳号或乱序）",
					n.id, k+1, a.index, wantIndex)
			}
			if a.index >= len(n.log) {
				return fmt.Errorf("不变式 (c) 破坏：节点 %s 应用了下标 %d，但日志只有 %d 条",
					n.id, a.index, len(n.log)-1)
			}
			if !bytes.Equal(n.log[a.index].Command, a.command) {
				return fmt.Errorf("不变式 (c) 破坏：节点 %s 应用的下标 %d 命令是 %q，日志里是 %q",
					n.id, a.index, a.command, n.log[a.index].Command)
			}
		}
		if len(n.applied) != n.lastApplied {
			return fmt.Errorf("不变式 (c) 破坏：节点 %s 已应用 %d 条，但 lastApplied=%d（drain 后应相等）",
				n.id, len(n.applied), n.lastApplied)
		}
	}
	return nil
}

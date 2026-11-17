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
	id            string
	alive         bool
	term          int
	role          string
	leader        string
	log           []raft.LogEntry // 从 snapshotIndex 开始的日志（log[0] 是基底占位条目）
	snapshotIndex int             // log[0] 的绝对下标（0 表示从未快照）
	snapshotTerm  int             // 快照位置的任期
	commitIndex   int
	lastApplied   int
	applied       []appliedEntry // 状态机已应用的序列（按应用顺序）
	appliedBase   int            // applied[0] 的下标是 appliedBase+1
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
		log, base, commit, _ := n.raft.LogSnapshot()
		appliedBase, _ := n.appliedStatus()
		out = append(out, nodeSnapshot{
			id:     id,
			alive:  true,
			term:   dbg.Term,
			role:   dbg.Role,
			leader: dbg.Leader,
			log:    log,
			// log[0] 是基底位置的条目：从未快照时是哨兵（Term 0），
			// 快照后是占位条目（Term = 快照任期）——它的 Term 就是快照任期。
			snapshotIndex: base,
			snapshotTerm:  log[0].Term,
			commitIndex:   commit,
			lastApplied:   dbg.LastApplied,
			applied:       n.appliedCopy(),
			appliedBase:   appliedBase,
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
//
// 快照（M5）之后：水位线按绝对下标记，节点日志按基底偏移取。
// 某条已提交的条目如果已被所有活节点快照压缩掉（谁手里都没了），
// 水位线该位置记一个 unknown 占位（Term -1，真实条目的任期永远 >= 0），
// 比较时跳过它——它的效果在快照里，
// 由“快照任期锚点”另行核对（见下面的锚点检查）。
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
		// 逐条扩展：每条从还保留着它、且该下标已提交的节点取；
		// 谁都没留着的（都被快照压缩了），记 unknown 占位。
		for i := len(s.committed) + 1; i <= maxCommit; i++ {
			extended := false
			for _, n := range snap {
				if !n.alive || i > n.commitIndex || i <= n.snapshotIndex {
					continue
				}
				if i-n.snapshotIndex >= len(n.log) {
					continue
				}
				s.committed = append(s.committed, n.log[i-n.snapshotIndex])
				extended = true
				break
			}
			if !extended {
				s.committed = append(s.committed, raft.LogEntry{Term: -1})
			}
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
		// 快照任期锚点：水位线里快照位置那条已知时，
		// 它的任期必须与节点的快照任期一致——
		// 这是“被压缩掉的那段前缀也没丢”的唯一可查的证据。
		// （水位线那条是 unknown 时跳过：它被压缩掉之前
		// 水位线就没记下来，锚不上——但别的节点若还留着它，
		// 下面的逐条比较会查到。）
		if n.snapshotIndex > 0 && n.snapshotIndex <= len(s.committed) {
			want := s.committed[n.snapshotIndex-1]
			if want.Term >= 0 && want.Term != n.snapshotTerm {
				return fmt.Errorf("不变式 (b) 破坏：节点 %s 的快照下标 %d 任期是 %d，已提交水位线是 %d",
					n.id, n.snapshotIndex, n.snapshotTerm, want.Term)
			}
		}
		for i := n.snapshotIndex + 1; i <= limit; i++ {
			want := s.committed[i-1]
			if want.Term < 0 {
				continue // unknown：已被快照压缩，没法逐条比
			}
			got := n.log[i-n.snapshotIndex]
			if got.Term != want.Term || !bytes.Equal(got.Command, want.Command) {
				return fmt.Errorf("不变式 (b) 破坏：节点 %s 的已提交日志下标 %d 是 {term=%d, cmd=%q}，已提交水位线是 {term=%d, cmd=%q}",
					n.id, i, got.Term, got.Command, want.Term, want.Command)
			}
		}
	}
	return nil
}

// checkApplyInOrder 是不变式 (c)：状态机按序应用。
// 每个活节点的已应用序列必须是下标严格递增、无跳号，
// 且每条应用的命令必须与该节点日志同一位置的条目相同。
// （lastApplied 单调递增由 Raft 的 apply 循环保证，
// 这里检查的是“状态机实际应用的顺序与内容”。）
// 快照（M5）之后：已应用序列的起点不再是下标 1——
// 从快照恢复的节点，已应用序列从快照下标之后开始记
// （appliedBase），快照位置之前的条目不再逐条记录
// （它们的效果已在快照里）；应用条数 == lastApplied - appliedBase。
func (s *Sim) checkApplyInOrder(snap []nodeSnapshot) error {
	for _, n := range snap {
		if !n.alive {
			continue
		}
		for k, a := range n.applied {
			wantIndex := n.appliedBase + k + 1
			if a.index != wantIndex {
				return fmt.Errorf("不变式 (c) 破坏：节点 %s 第 %d 次应用的下标是 %d，想要 %d（跳号或乱序）",
					n.id, k+1, a.index, wantIndex)
			}
			// 下标 ≤ 快照位置的条目本就不在日志里：
			// 它们的效果已在快照中（InstallSnapshot 压缩日志后、
			// apply 循环重置 appliedBase 前，有个很小的窗口，
			// 检查可能恰好落在这里——跳过是正确的，不是放水）。
			if a.index <= n.snapshotIndex {
				continue
			}
			if a.index-n.snapshotIndex >= len(n.log) {
				return fmt.Errorf("不变式 (c) 破坏：节点 %s 应用了下标 %d，但日志里没有它（快照下标 %d，日志保留到 %d）",
					n.id, a.index, n.snapshotIndex, n.snapshotIndex+len(n.log)-1)
			}
			if !bytes.Equal(n.log[a.index-n.snapshotIndex].Command, a.command) {
				return fmt.Errorf("不变式 (c) 破坏：节点 %s 应用的下标 %d 命令是 %q，日志里是 %q",
					n.id, a.index, a.command, n.log[a.index-n.snapshotIndex].Command)
			}
		}
		if len(n.applied) != n.lastApplied-n.appliedBase {
			return fmt.Errorf("不变式 (c) 破坏：节点 %s 已应用 %d 条（起点下标 %d），但 lastApplied=%d（drain 后应相等）",
				n.id, len(n.applied), n.appliedBase, n.lastApplied)
		}
	}
	return nil
}

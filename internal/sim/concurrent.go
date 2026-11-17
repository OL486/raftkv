// Package sim 的并发客户端驱动：三个客户端“并发”发 Put/线性化 Read，
// 由单个 driver goroutine 协同调度（不是三个真 goroutine）。
//
// 为什么不用真 goroutine：模拟器的全部决策都在 Advance 的调用栈里
// 同步发生（见包注释），同一种子跑两次的事件顺序才完全相同；
// 三个真 goroutine 调 Advance 会让事件插入顺序取决于 OS 调度，
// 确定性就丢了。这里每个客户端的操作是一个状态机
// （找 leader → 等提交 → 等可见），driver 每推进一小段虚拟时间，
// 就让每个客户端的操作各走一步——操作区间在虚拟时间上真实重叠
// （一个操作调用后、返回前，别的客户端的操作已经开始），
// 同时同一种子跑两次的历史完全相同（M4 的检查可复现）。
package sim

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/OL486/raftkv/internal/kv"
)

// concQuantum 是并发驱动每步推进的虚拟时间（与 Put 等提交的步长一致）。
const concQuantum = 10 * time.Millisecond

// ConcurrentOp 是一个客户端逻辑操作：Put 写一个值，或线性化读一个值。
type ConcurrentOp struct {
	Kind  string // "put" 或 "read"
	Key   string
	Value string // put 才用
}

// OpRecord 是 RunConcurrent 记下的一条操作历史：
// 一次逻辑操作从第一次调用到最终返回的完整区间
// （中间可能因 leader 换人重试过，重试的命令是同一条，
// 对 Put 来说值相同所以幂等，对 Read 来说是 no-op）。
// Call/Return 是虚拟时间（自模拟器启动以来的累计推进）。
type OpRecord struct {
	ClientID int
	Kind     string // "put" 或 "read"
	Key      string
	Value    string // put：写入的值；read：读到的值（!Found 时为空）
	Found    bool   // read 才有意义
	OK       bool   // false 表示该操作最终失败（干净集群里不应出现）
	Call     time.Duration
	Return   time.Duration
}

// concPhase 是单个操作状态机的阶段。
type concPhase int

const (
	concFindLeader  concPhase = iota // 等 leader 出现，再 Start
	concWaitCommit                   // 等 Start 的下标提交
	concWaitVisible                  // 等各活节点 commitIndex 追上（最多 500ms）
)

// concOpState 是一个在途逻辑操作的状态。
type concOpState struct {
	clientID int
	spec     ConcurrentOp
	call     time.Duration // 第一次调用的虚拟时间
	phase    concPhase
	index    int // 已 Start 的日志下标
	leaderID string
	// startTime 是第一次 Start 的虚拟时间；deadline 是当前阶段的截止时间；
	// extended 表示已用过一次延期（换 leader 重试、或 leader 没换等满 10 秒）。
	startTime time.Duration
	deadline  time.Duration
	extended  bool
}

// concClient 是一个并发客户端：按顺序发一串操作，
// 同一时间最多一个在途操作。
type concClient struct {
	id        int
	ops       []ConcurrentOp
	next      int // 下一个要发的操作下标
	nextStart time.Duration
	cur       *concOpState
}

// RunConcurrent 让 len(perClient) 个客户端并发发操作：
// perClient[i] 是第 i 个客户端要按顺序发的操作序列。
// 全部操作跑完后返回完整历史（每条记录一个逻辑操作）。
// seed 只控制客户端之间的调度（谁先发、发完等多久再发下一条），
// 不控制 Raft 内部的随机数（那是 Sim 的种子管的）。
// 任一操作最终失败时仍返回完整历史，并附带错误——
// 干净集群里操作不应失败，失败即是 bug 信号；
// 测试应在调检查器之前先断言没有失败操作
// （失败操作的输出在模型里按“没生效”处理，而它实际上可能
// 日后提交——那样的历史不该拿去检查，见 linearizability 包）。
func (s *Sim) RunConcurrent(seed int64, perClient [][]ConcurrentOp) ([]OpRecord, error) {
	rng := rand.New(rand.NewSource(seed))
	clients := make([]*concClient, len(perClient))
	for i, ops := range perClient {
		clients[i] = &concClient{id: i, ops: ops}
	}
	var history []OpRecord
	var failed []OpRecord
	// 安全上限：虚拟时间推进超过 30 分钟还没跑完，说明模拟器卡住了。
	const maxVirtual = 30 * time.Minute
	start := s.clock.Now()
	for {
		done := true
		for _, c := range clients {
			if c.next < len(c.ops) || c.cur != nil {
				done = false
				break
			}
		}
		if done {
			break
		}
		if s.clock.Now()-start > maxVirtual {
			return history, fmt.Errorf("并发运行超过 %v 虚拟时间还没结束（可能卡住了）", maxVirtual)
		}
		s.Advance(concQuantum)
		for _, c := range clients {
			if c.cur == nil {
				continue
			}
			if rec, finished := c.step(s); finished {
				history = append(history, rec)
				if !rec.OK {
					failed = append(failed, rec)
				}
				c.cur = nil
				// 发完一条后随机等 0–50ms 再发下一条：
				// 让客户端的发操作节奏错开，区间重叠模式更多样。
				c.nextStart = s.clock.Now() + time.Duration(rng.Intn(51))*time.Millisecond
			}
		}
		for _, c := range clients {
			if c.cur == nil && c.next < len(c.ops) && s.clock.Now() >= c.nextStart {
				c.startNext(s)
			}
		}
	}
	if len(failed) > 0 {
		return history, fmt.Errorf("%d 个操作最终失败（第一个：客户端 %d %s %s）", len(failed), failed[0].ClientID, failed[0].Kind, failed[0].Key)
	}
	return history, nil
}

// startNext 开始客户端的下一个操作：记调用时间，进找 leader 阶段。
func (c *concClient) startNext(s *Sim) {
	spec := c.ops[c.next]
	c.next++
	c.cur = &concOpState{
		clientID: c.id,
		spec:     spec,
		call:     s.clock.Now(),
		phase:    concFindLeader,
	}
}

// step 让在途操作走一步。操作完成时返回 (记录, true)，
// 否则返回 (零值, false)。逻辑与 Sim.Put 的同步版一一对应
// （找 leader 5 秒上限、等提交先 2 秒再看 leader 是否换人、
// 等可见最多 500ms），只是把“循环推进虚拟时间”拆成了一步一步。
func (c *concClient) step(s *Sim) (OpRecord, bool) {
	st := c.cur
	now := s.clock.Now()
	cmd := encodeConcOp(st.spec)
	switch st.phase {
	case concFindLeader:
		// 5 秒虚拟时间内没出现 leader：操作失败。
		if now-st.call > putLeaderBudget {
			return c.finish(st, now, "", false, false), true
		}
		leader := s.findLeader()
		if leader == nil {
			return OpRecord{}, false
		}
		index, _, ok := leader.raft.Start(cmd)
		if !ok {
			// 找到与 Start 之间 leader 退位了：下个量子再试。
			return OpRecord{}, false
		}
		st.index = index
		st.leaderID = leader.id
		st.startTime = now
		st.deadline = now + 2*time.Second
		st.phase = concWaitCommit
		return OpRecord{}, false
	case concWaitCommit:
		if s.maxCommitIndex() >= st.index {
			st.phase = concWaitVisible
			st.deadline = now + 500*time.Millisecond
			return OpRecord{}, false
		}
		if now <= st.deadline {
			return OpRecord{}, false
		}
		// 2 秒还没提交：看 leader 是不是换人了。
		if !st.extended {
			st.extended = true
			if l2 := s.findLeader(); l2 != nil && l2.id != st.leaderID {
				// 换人了：在新 leader 上再写一次（同命令；
				// Put 值相同所以幂等，Read 是 no-op）。
				if index, _, ok := l2.raft.Start(cmd); ok {
					st.index = index
					st.leaderID = l2.id
					st.deadline = now + putCommitWait
					return OpRecord{}, false
				}
				// 新 leader 的 Start 被拒（刚找到就退位）：
				// 原下标仍可能提交，继续等到总共 10 秒。
			}
			st.deadline = st.startTime + putCommitWait
			return OpRecord{}, false
		}
		// 已延期过一次、10 秒还没提交：操作失败。
		return c.finish(st, now, "", false, false), true
	case concWaitVisible:
		if s.minCommitIndex() >= st.index {
			return c.finishVisible(s, st, now)
		}
		if now > st.deadline {
			// 500ms 还没追上：写本身已提交，直接返回成功
			// （与 waitVisible 的语义一致）。
			return c.finishVisible(s, st, now)
		}
		return OpRecord{}, false
	}
	return OpRecord{}, false
}

// finishVisible 是等可见阶段的收尾：排空 apply 队列，
// 记返回时间与输出（读操作从应用结果记录里拿线性化点处的值）。
func (c *concClient) finishVisible(s *Sim, st *concOpState, now time.Duration) (OpRecord, bool) {
	if err := s.drain(); err != nil {
		return c.finish(st, now, "", false, false), true
	}
	if st.spec.Kind == "read" {
		value, found, err := s.readResult(st.index)
		if err != nil {
			return c.finish(st, now, "", false, false), true
		}
		return c.finish(st, now, value, found, true), true
	}
	return c.finish(st, now, st.spec.Value, false, true), true
}

// finish 组装一条历史记录。
func (c *concClient) finish(st *concOpState, now time.Duration, value string, found, ok bool) OpRecord {
	return OpRecord{
		ClientID: st.clientID,
		Kind:     st.spec.Kind,
		Key:      st.spec.Key,
		Value:    value,
		Found:    found,
		OK:       ok,
		Call:     st.call,
		Return:   now,
	}
}

// encodeConcOp 把一个并发操作编成日志命令。
func encodeConcOp(op ConcurrentOp) []byte {
	if op.Kind == "read" {
		return kv.EncodeRead(op.Key)
	}
	return kv.EncodePut(op.Key, op.Value)
}

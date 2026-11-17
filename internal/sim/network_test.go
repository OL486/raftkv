package sim

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/OL486/raftkv/internal/raft"
)

func newTestSim(t *testing.T, seed int64) *Sim {
	t.Helper()
	sim := NewSim(seed, t.TempDir(), []string{"node1", "node2", "node3"}, map[string]string{
		"node1": "127.0.0.1:19001",
		"node2": "127.0.0.1:19002",
		"node3": "127.0.0.1:19003",
	})
	t.Cleanup(sim.Stop)
	if err := sim.Start(); err != nil {
		t.Fatalf("Start 失败：%v", err)
	}
	return sim
}

func mustSettle(t *testing.T, sim *Sim, d time.Duration) {
	t.Helper()
	if err := sim.Settle(d); err != nil {
		t.Fatalf("Settle(%v) 失败：%v", d, err)
	}
}

func mustPut(t *testing.T, sim *Sim, key, value string) {
	t.Helper()
	if _, err := sim.Put(key, value); err != nil {
		t.Fatalf("Put(%s=%s) 失败：%v", key, value, err)
	}
}

func putN(t *testing.T, sim *Sim, prefix string, n int) []string {
	t.Helper()
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		k := prefix + string(rune('a'+i))
		mustPut(t, sim, k, "v-"+k)
		keys = append(keys, k)
	}
	return keys
}

// assertKeysOnAll 断言每个活节点都能读到所有 key（值与写入时一致）。
func assertKeysOnAll(t *testing.T, sim *Sim, keys []string) {
	t.Helper()
	for _, id := range sim.IDs() {
		if !sim.Alive(id) {
			continue
		}
		for _, k := range keys {
			v, ok := sim.Get(id, k)
			if !ok || v != "v-"+k {
				t.Fatalf("节点 %s 读不到 %s（got %q, ok=%v）", id, k, v, ok)
			}
		}
	}
}

// assertAllKVEqual 断言所有活节点的 KV 内容完全相同。
func assertAllKVEqual(t *testing.T, sim *Sim) {
	t.Helper()
	var want map[string]string
	for _, id := range sim.IDs() {
		if !sim.Alive(id) {
			continue
		}
		kv := sim.KV(id)
		if want == nil {
			want = kv
			continue
		}
		if len(kv) != len(want) {
			t.Fatalf("节点 %s 的 KV 有 %d 条，与第一个节点 %d 条不一致", id, len(kv), len(want))
		}
		for k, v := range want {
			if kv[k] != v {
				t.Fatalf("节点 %s 的 %s=%q，与第一个节点 %q 不一致", id, k, kv[k], v)
			}
		}
	}
}

// assertAllLogsEqual 断言所有活节点的已提交日志完全相同
// （逐条比较 term+命令，含第 0 条哨兵；以第一个活节点为准）。
// 只比较已提交前缀（下标 ≤ commitIndex），不比较
// 未提交的尾部——follower 可能留着 leader 没有的旧尾巴，
// Raft 不保证它被截掉（leader 只发自己有的条目，空心跳不会
// 截掉 follower 多出来的尾巴；要等下一次写才会被覆盖）。
// 快照（M5）之后：只比较双方都保留着的下标，
// 快照位置的任期用“锚点”核对（见 logsDivergence）。
func assertAllLogsEqual(t *testing.T, sim *Sim) {
	t.Helper()
	if msg := logsDivergence(sim); msg != "" {
		t.Fatalf("各活节点已提交日志不一致：%s", msg)
	}
}

// waitLogsEqual 等各活节点已提交日志收敛到一致（最多 budget 虚拟时间），
// 再检查不变式。收敛慢是因为 leader 的 nextIndex 一次只回退一条。
func waitLogsEqual(t *testing.T, sim *Sim, budget time.Duration) {
	t.Helper()
	for waited := time.Duration(0); waited < budget; waited += 500 * time.Millisecond {
		sim.Advance(500 * time.Millisecond)
		if logsEqual(sim) {
			break
		}
	}
	if !logsEqual(sim) {
		t.Fatalf("%v 虚拟时间内各活节点已提交日志没有收敛到一致：%s", budget, logsDivergence(sim))
	}
	if err := sim.CheckInvariants(); err != nil {
		t.Fatalf("收敛后不变式失败：%v", err)
	}
}

// logsEqual 判断各活节点的已提交日志是否收敛到一致：
// commitIndex 相同，且已提交前缀（下标 ≤ commitIndex）逐条相同。
// 不比较未提交的尾部——follower 可能留着 leader 没有的旧尾巴，
// Raft 不保证它被截掉（见 assertAllLogsEqual 的注释）。
func logsEqual(sim *Sim) bool {
	return logsDivergence(sim) == ""
}

// logsDivergence 返回各活节点已提交日志的第一个分歧描述，
// 一致时返回 ""。
func logsDivergence(sim *Sim) string {
	var want []raft.LogEntry
	var wantBase int
	wantCommit := -1
	for _, id := range sim.IDs() {
		if !sim.Alive(id) {
			continue
		}
		log, base, commit, _ := sim.nodes[id].raft.LogSnapshot()
		if wantCommit < 0 {
			wantCommit, want, wantBase = commit, log, base
			continue
		}
		if commit != wantCommit {
			return fmt.Sprintf("节点 %s commitIndex=%d，第一个节点是 %d", id, commit, wantCommit)
		}
		if base+len(log)-1 < commit {
			return fmt.Sprintf("节点 %s 日志只覆盖到下标 %d，commitIndex 却到 %d", id, base+len(log)-1, commit)
		}
		// 快照边界锚点：该节点快照位置的任期，必须与第一个节点
		// 同一下标的条目任期一致（若第一个节点还留着它）。
		if base > wantBase && base-wantBase < len(want) {
			if log[0].Term != want[base-wantBase].Term {
				return fmt.Sprintf("节点 %s 快照下标 %d 任期是 %d，第一个节点同一下标是 %d",
					id, base, log[0].Term, want[base-wantBase].Term)
			}
		}
		if wantBase > base && wantBase-base < len(log) {
			if want[0].Term != log[wantBase-base].Term {
				return fmt.Sprintf("第一个节点快照下标 %d 任期是 %d，节点 %s 同一下标是 %d",
					wantBase, want[0].Term, id, log[wantBase-base].Term)
			}
		}
		// 只比较双方都保留着的已提交前缀。
		lo := base
		if wantBase > lo {
			lo = wantBase
		}
		for i := lo + 1; i <= commit; i++ {
			if log[i-base].Term != want[i-wantBase].Term || !bytes.Equal(log[i-base].Command, want[i-wantBase].Command) {
				return fmt.Sprintf("节点 %s 已提交日志下标 %d 是 {%d,%q}，第一个节点是 {%d,%q}",
					id, i, log[i-base].Term, log[i-base].Command, want[i-wantBase].Term, want[i-wantBase].Command)
			}
		}
	}
	return ""
}

// healAndRestartAll 是每个故障场景的收尾：恢复连通、重启所有死节点、
// 等收敛，再断言不变式 + 所有活节点 KV 与日志一致。
func healAndRestartAll(t *testing.T, sim *Sim) {
	t.Helper()
	sim.Heal()
	for _, id := range sim.IDs() {
		if !sim.Alive(id) {
			if err := sim.Restart(id); err != nil {
				t.Fatalf("重启 %s 失败：%v", id, err)
			}
		}
	}
	waitLogsEqual(t, sim, 30*time.Second)
	mustSettle(t, sim, 2*time.Second)
	assertAllKVEqual(t, sim)
	assertAllLogsEqual(t, sim)
}

// 丢包率 100%：没有任何 RPC 能送达，集群永远选不出 leader。
func TestNetwork_DropAll_NoLeader(t *testing.T) {
	sim := NewSim(7, t.TempDir(), []string{"node1", "node2", "node3"}, map[string]string{
		"node1": "127.0.0.1:19001",
		"node2": "127.0.0.1:19002",
		"node3": "127.0.0.1:19003",
	})
	t.Cleanup(sim.Stop)
	sim.net.SetDropRate(1.0)
	if err := sim.Start(); err != nil {
		t.Fatal(err)
	}
	sim.Advance(2 * time.Second)
	if l := sim.findLeader(); l != nil {
		t.Fatalf("丢包 100%% 时不应有 leader，实际 %s", l.id)
	}
}

// 分区后，多数派一侧能在 1 秒虚拟时间内选出新 leader，
// 少数派一侧的旧 leader 仍自称 leader（任期更低）——
// Put 必须走到多数派的新 leader 那里（靠任期高低判断）。
func TestNetwork_Partition_NewLeaderElected(t *testing.T) {
	sim := newTestSim(t, 11)
	mustSettle(t, sim, 2*time.Second)
	oldLeader := sim.findLeader()
	if oldLeader == nil {
		t.Fatal("2 秒后没有 leader")
	}
	mustPut(t, sim, "before", "v-before")

	// 把旧 leader 孤立出去。
	others := make([]string, 0, 2)
	for _, id := range sim.IDs() {
		if id != oldLeader.id {
			others = append(others, id)
		}
	}
	sim.Partition(oldLeader.id, others[0])
	sim.Advance(1 * time.Second)
	newLeader := sim.findLeader()
	if newLeader == nil {
		t.Fatal("分区后多数派 1 秒内没有选出新 leader")
	}
	if newLeader.id == oldLeader.id {
		t.Fatalf("分区后新 leader 仍是旧的 %s", oldLeader.id)
	}
	// 多数派写成功，旧 leader 隔离在外。
	mustPut(t, sim, "after", "v-after")
	if v, ok := sim.Get(newLeader.id, "after"); !ok || v != "v-after" {
		t.Fatalf("多数派 leader 读不到刚写的 after（%q %v）", v, ok)
	}
	// 旧 leader 读不到（它被隔离了）。
	if _, ok := sim.Get(oldLeader.id, "after"); ok {
		t.Fatal("被隔离的旧 leader 不应读到 after")
	}
	if err := sim.CheckInvariants(); err != nil {
		t.Fatalf("分区期间不变式失败：%v", err)
	}
}

// 延迟区间生效：消息不会在延迟到达前投递。
// 注意：延迟不能太大——选举超时是 150–300ms，单向延迟超过
// ~70ms 会让往返时间超过选举超时，选主活锁（这是参数假设，
// 不是模拟器的 bug；见 docs/03-testing.md）。
func TestNetwork_Latency(t *testing.T) {
	sim := NewSim(13, t.TempDir(), []string{"node1", "node2", "node3"}, map[string]string{
		"node1": "127.0.0.1:19001",
		"node2": "127.0.0.1:19002",
		"node3": "127.0.0.1:19003",
	})
	t.Cleanup(sim.Stop)
	sim.net.SetLatency(50*time.Millisecond, 50*time.Millisecond)
	if err := sim.Start(); err != nil {
		t.Fatal(err)
	}
	// 50ms 延迟：选举往返要 100ms，100ms 内不应有 leader。
	sim.Advance(100 * time.Millisecond)
	if l := sim.findLeader(); l != nil {
		t.Fatalf("50ms 延迟下 100ms 内不应有 leader，实际 %s", l.id)
	}
	// 5 秒足够选出 leader 并写一条。
	mustSettle(t, sim, 5*time.Second)
	if l := sim.findLeader(); l == nil {
		t.Fatal("50ms 延迟下 5 秒内没有 leader")
	}
	mustPut(t, sim, "slow", "v-slow")
}

package sim

import (
	"strconv"
	"testing"
	"time"
)

// faultScenario 是一组故障操作，跑完后收尾（恢复连通、重启死节点、
// 等收敛），再断言不变式 + 所有活节点 KV 与日志一致。
// seeds 是每个场景要跑的种子数（每个组合 3 个种子）。
type faultScenario struct {
	name string
	run  func(t *testing.T, sim *Sim)
}

var faultScenarios = []faultScenario{
	{
		name: "baseline/无故障写10条",
		run: func(t *testing.T, sim *Sim) {
			mustSettle(t, sim, 2*time.Second)
			keys := putN(t, sim, "base", 10)
			assertKeysOnAll(t, sim, keys)
		},
	},
	{
		name: "partition/单节点孤立",
		run: func(t *testing.T, sim *Sim) {
			mustSettle(t, sim, 2*time.Second)
			before := putN(t, sim, "pre", 5)
			sim.Partition("node2", "node1")
			after := putN(t, sim, "post", 5)
			// 被孤立的节点读不到分区后写的。
			if _, ok := sim.Get("node2", after[0]); ok {
				t.Fatalf("被孤立的 node2 不应读到 %s", after[0])
			}
			healAndRestartAll(t, sim)
			assertKeysOnAll(t, sim, append(before, after...))
		},
	},
	{
		name: "partition/2对1分裂（leader被孤立）",
		run: func(t *testing.T, sim *Sim) {
			mustSettle(t, sim, 2*time.Second)
			before := putN(t, sim, "pre", 5)
			leader := sim.findLeader()
			if leader == nil {
				t.Fatal("没有 leader")
			}
			others := make([]string, 0, 2)
			for _, id := range sim.IDs() {
				if id != leader.id {
					others = append(others, id)
				}
			}
			sim.Partition(leader.id, others[0])
			after := putN(t, sim, "post", 5)
			healAndRestartAll(t, sim)
			assertKeysOnAll(t, sim, append(before, after...))
		},
	},
	{
		name: "crash/leader崩",
		run: func(t *testing.T, sim *Sim) {
			mustSettle(t, sim, 2*time.Second)
			before := putN(t, sim, "pre", 5)
			leader := sim.findLeader()
			if leader == nil {
				t.Fatal("没有 leader")
			}
			if err := sim.Kill(leader.id); err != nil {
				t.Fatalf("Kill leader 失败：%v", err)
			}
			after := putN(t, sim, "post", 5)
			healAndRestartAll(t, sim)
			assertKeysOnAll(t, sim, append(before, after...))
		},
	},
	{
		name: "crash/follower崩",
		run: func(t *testing.T, sim *Sim) {
			mustSettle(t, sim, 2*time.Second)
			before := putN(t, sim, "pre", 5)
			leader := sim.findLeader()
			if leader == nil {
				t.Fatal("没有 leader")
			}
			var follower string
			for _, id := range sim.IDs() {
				if id != leader.id {
					follower = id
					break
				}
			}
			if err := sim.Kill(follower); err != nil {
				t.Fatalf("Kill follower 失败：%v", err)
			}
			after := putN(t, sim, "post", 5)
			healAndRestartAll(t, sim)
			assertKeysOnAll(t, sim, append(before, after...))
		},
	},
	{
		name: "crash/连续崩两个",
		run: func(t *testing.T, sim *Sim) {
			mustSettle(t, sim, 2*time.Second)
			before := putN(t, sim, "pre", 5)
			leader := sim.findLeader()
			if leader == nil {
				t.Fatal("没有 leader")
			}
			if err := sim.Kill(leader.id); err != nil {
				t.Fatalf("Kill leader 失败：%v", err)
			}
			// 再崩一个 follower：只剩一个节点，写必须失败。
			var follower string
			for _, id := range sim.IDs() {
				if id != leader.id && sim.Alive(id) {
					follower = id
					break
				}
			}
			if err := sim.Kill(follower); err != nil {
				t.Fatalf("Kill follower 失败：%v", err)
			}
			if _, err := sim.Put("should-fail", "x"); err == nil {
				t.Fatal("只剩一个节点时写应失败")
			}
			healAndRestartAll(t, sim)
			assertKeysOnAll(t, sim, before)
		},
	},
	{
		name: "restart/leader立即重启",
		run: func(t *testing.T, sim *Sim) {
			mustSettle(t, sim, 2*time.Second)
			before := putN(t, sim, "pre", 5)
			leader := sim.findLeader()
			if leader == nil {
				t.Fatal("没有 leader")
			}
			if err := sim.Kill(leader.id); err != nil {
				t.Fatalf("Kill leader 失败：%v", err)
			}
			if err := sim.Restart(leader.id); err != nil {
				t.Fatalf("立即重启 leader 失败：%v", err)
			}
			after := putN(t, sim, "post", 5)
			healAndRestartAll(t, sim)
			assertKeysOnAll(t, sim, append(before, after...))
		},
	},
	{
		name: "restart/follower隔5秒重启",
		run: func(t *testing.T, sim *Sim) {
			mustSettle(t, sim, 2*time.Second)
			before := putN(t, sim, "pre", 5)
			leader := sim.findLeader()
			if leader == nil {
				t.Fatal("没有 leader")
			}
			var follower string
			for _, id := range sim.IDs() {
				if id != leader.id {
					follower = id
					break
				}
			}
			if err := sim.Kill(follower); err != nil {
				t.Fatalf("Kill follower 失败：%v", err)
			}
			// 隔 5 秒虚拟时间：期间集群继续写，落后的 follower 要追上。
			after := putN(t, sim, "mid", 5)
			sim.Advance(5 * time.Second)
			if err := sim.Restart(follower); err != nil {
				t.Fatalf("重启 follower 失败：%v", err)
			}
			healAndRestartAll(t, sim)
			assertKeysOnAll(t, sim, append(before, after...))
		},
	},
	{
		name: "combined/分区+杀leader+重启+写",
		run: func(t *testing.T, sim *Sim) {
			mustSettle(t, sim, 2*time.Second)
			before := putN(t, sim, "pre", 5)
			leader := sim.findLeader()
			if leader == nil {
				t.Fatal("没有 leader")
			}
			// 分区：把 leader 孤立。
			others := make([]string, 0, 2)
			for _, id := range sim.IDs() {
				if id != leader.id {
					others = append(others, id)
				}
			}
			sim.Partition(leader.id, others[0])
			// 多数派继续写。
			mid := putN(t, sim, "mid", 5)
			// 杀掉被孤立的 leader（它连不上集群，相当于丢了）。
			if err := sim.Kill(leader.id); err != nil {
				t.Fatalf("Kill 被孤立的 leader 失败：%v", err)
			}
			after := putN(t, sim, "post", 5)
			healAndRestartAll(t, sim)
			assertKeysOnAll(t, sim, append(append(before, mid...), after...))
		},
	},
}

// TestFaultMatrix：每个场景 × 3 个种子（1/2/3），
// 每次跑完都断言三个不变式（场景收尾的 healAndRestartAll 里查，
// 加上场景里每一步 Put/Settle 的内部检查）。
// 一共 9 个场景 × 3 个种子 = 27 次运行。
func TestFaultMatrix(t *testing.T) {
	seeds := []int64{1, 2, 3}
	for _, sc := range faultScenarios {
		for _, seed := range seeds {
			t.Run(sc.name+"/seed"+strconv.FormatInt(seed, 10), func(t *testing.T) {
				sim := newTestSim(t, seed)
				sc.run(t, sim)
				if err := sim.CheckInvariants(); err != nil {
					t.Fatalf("场景结束不变式失败：%v", err)
				}
			})
		}
	}
}

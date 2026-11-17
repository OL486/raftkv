package sim

import (
	"reflect"
	"testing"
	"time"
)

// runFaultLong 是随机长跑的公共部分：种子 seed，跑 n 步，
// 每步结束都检查不变式（FaultDriver.Step 里做）。
// 跑完后收尾（恢复连通、重启死节点、等收敛），
// 返回最终的已提交日志、最终 KV（按节点 id）和 leader 历史。
func runFaultLong(t *testing.T, seed int64, steps int) (log []logEntryView, kvs map[string]map[string]string, history []LeaderEvent) {
	t.Helper()
	sim := newTestSim(t, seed)
	mustSettle(t, sim, 2*time.Second)
	// 先写几条基线数据。
	putN(t, sim, "base", 5)
	drv := NewFaultDriver(sim)
	if err := drv.Run(steps); err != nil {
		t.Fatalf("随机故障长跑（seed=%d）第 %d 步失败：%v", seed, drv.Stats().Steps, err)
	}
	st := drv.Stats()
	t.Logf("seed=%d：%d 步，写 %d（成功 %d/失败 %d），分区 %d/恢复 %d，杀 %d/重启 %d",
		seed, st.Steps, st.Writes, st.WriteOK, st.WriteFail, st.Partitions, st.Heals, st.Kills, st.Restarts)
	healAndRestartAll(t, sim)
	committed := sim.CommittedLog()
	log = make([]logEntryView, 0, len(committed))
	for _, e := range committed {
		log = append(log, logEntryView{Term: e.Term, Command: string(e.Command)})
	}
	kvs = make(map[string]map[string]string, 3)
	for _, id := range sim.IDs() {
		kvs[id] = sim.KV(id)
	}
	history = sim.LeaderHistory()
	return log, kvs, history
}

// logEntryView 是日志条目的可比较视图（term+命令）。
type logEntryView struct {
	Term    int
	Command string
}

// TestRandomFaultLong_1000Steps：种子固定（42）的随机故障长跑，
// 1000 步，每步结束都检查三个不变式。
func TestRandomFaultLong_1000Steps(t *testing.T) {
	log, kvs, _ := runFaultLong(t, 42, 1000)
	t.Logf("长跑结束：已提交日志 %d 条，KV %d 条", len(log), len(kvs["node1"]))
}

// TestDeterminism_SameSeedTwice：同一个种子跑两次，
// 最终已提交日志（逐条 term+命令）与最终 KV 必须完全相同。
// leader 历史也比较，但只记录结果、不参与断言：
// 如果 goroutine 调度让 leader 历史有差异，断言仍只覆盖
// “最终已提交日志 + 最终 KV + 不变式”，残留的非确定性写进文档。
func TestDeterminism_SameSeedTwice(t *testing.T) {
	log1, kvs1, hist1 := runFaultLong(t, 42, 1000)
	log2, kvs2, hist2 := runFaultLong(t, 42, 1000)
	if !reflect.DeepEqual(log1, log2) {
		t.Fatalf("同一种子两次跑的最终已提交日志不一致：第一次 %d 条，第二次 %d 条", len(log1), len(log2))
	}
	if !reflect.DeepEqual(kvs1, kvs2) {
		t.Fatal("同一种子两次跑的最终 KV 不一致")
	}
	t.Logf("确定性：最终已提交日志 %d 条、最终 KV 完全相同", len(log1))
	if reflect.DeepEqual(hist1, hist2) {
		t.Logf("leader 历史也完全相同（%d 次切换，首 %v，末 %v）", len(hist1), hist1[:3], hist1[len(hist1)-3:])
	} else {
		t.Logf("注意：leader 历史两次跑不一致——第一次 %d 次切换，第二次 %d 次切换（断言不覆盖它，见 docs/03-testing.md）", len(hist1), len(hist2))
	}
}

// TestRandomFaultLong_Performance：1000 步随机长跑必须在 10 秒
// 真实时间内跑完（虚拟时钟，不等真实时间）。
func TestRandomFaultLong_Performance(t *testing.T) {
	start := time.Now()
	_, _, _ = runFaultLong(t, 99, 1000)
	elapsed := time.Since(start)
	t.Logf("1000 步随机长跑耗时 %v", elapsed)
	if elapsed > 10*time.Second {
		t.Fatalf("1000 步耗时 %v，超过 10 秒上限", elapsed)
	}
}

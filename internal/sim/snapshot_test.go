package sim

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/OL486/raftkv/internal/kv"
)

// pipelinePuts 用流水线方式写 n 条：直接调 leader 的 Start（不等每条提交），
// 每写 chunk 条推进一次虚拟时间、让复制与提交跟上；
// 写完再推进虚拟时间直到全部提交并应用。
// 返回实际写入的条数（leader 中途换人会重找 leader 继续写，
// 计数不变——重试的 Start 只会发生在旧 leader 上，未提交的旧条目
// 会被新 leader 截掉，不会重复进已提交日志）。
// 比 Sim.Put 快一个数量级（见 M5 设计摸底：1000 条 88ms vs 1.3s），
// 100k 条的验收测试用它；顺序写的路径另有 TestSnapshot_SequentialPuts 覆盖。
func pipelinePuts(t *testing.T, sim *Sim, prefix string, n int) int {
	t.Helper()
	const chunk = 500
	written := 0
	for written < n {
		leader := sim.findLeader()
		if leader == nil {
			t.Fatalf("写了 %d/%d 条后找不到 leader", written, n)
		}
		end := written + chunk
		if end > n {
			end = n
		}
		for i := written; i < end; i++ {
			cmd := kv.EncodePut(fmt.Sprintf("%s%06d", prefix, i), fmt.Sprintf("v-%s%06d", prefix, i))
			if _, _, ok := leader.raft.Start(cmd); !ok {
				// leader 在写之间退位了：重找 leader，这条重写。
				leader = sim.findLeader()
				if leader == nil {
					t.Fatalf("写了 %d/%d 条后找不到 leader", i, n)
				}
				if _, _, ok := leader.raft.Start(cmd); !ok {
					t.Fatalf("在新 leader %s 上 Start 也被拒", leader.id)
				}
			}
		}
		written = end
		// 每 500 条推进 200ms 虚拟时间，让复制、提交、应用跟上，
		// 顺便每 2000 条检查一次不变式（水位线及时扩展，
		// 快照压缩掉的条目就不用事后补——见 invariants.go 的 unknown 占位）。
		sim.Advance(200 * time.Millisecond)
		if written%(chunk*4) == 0 {
			if err := sim.CheckInvariants(); err != nil {
				t.Fatalf("写了 %d 条后不变式失败：%v", written, err)
			}
		}
	}
	// 等全部提交并应用（最多 60 秒真实时间）。
	deadline := time.Now().Add(60 * time.Second)
	for {
		sim.Advance(100 * time.Millisecond)
		if sim.maxCommitIndex() >= n {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("60 秒内没提交完 %d 条，maxCommit=%d", n, sim.maxCommitIndex())
		}
	}
	if err := sim.Settle(2 * time.Second); err != nil {
		t.Fatalf("Settle 失败：%v", err)
	}
	return written
}

// waitSnapshotIndex 等某节点的快照下标至少达到 want（真实时间轮询）。
// 快照触发在 consumeApply 里，跑在 apply 循环的尾巴上：
// Settle/drain 只等 lastApplied（Raft 侧发消息时就推进了），
// 消费端拍快照可能还差一拍——不断言前先等它落地，
// 否则 logLen 的断言会在“快照已触发、还没执行”时误报。
func waitSnapshotIndex(t *testing.T, sim *Sim, id string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if sim.Debug(id).SnapshotIndex >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("节点 %s 的快照下标在 %v 内没达到 %d（实际 %d）",
		id, timeout, want, sim.Debug(id).SnapshotIndex)
}

// snapshotStats 是某节点的快照状态（测试断言用）。
type snapshotStats struct {
	snapshotIndex int
	logLen        int
	lastApplied   int
	kvLen         int
}

func nodeSnapshotStats(sim *Sim, id string) snapshotStats {
	dbg := sim.Debug(id)
	return snapshotStats{
		snapshotIndex: dbg.SnapshotIndex,
		logLen:        dbg.LogLen,
		lastApplied:   dbg.LastApplied,
		kvLen:         len(sim.KV(id)),
	}
}

// TestSnapshot_100kWrites 是 M5 的核心验收（模拟器）：
// 写 10 万条后触发快照，日志被截断到 5000 条以内。
func TestSnapshot_100kWrites(t *testing.T) {
	sim := newTestSim(t, 42)
	mustSettle(t, sim, 2*time.Second)
	start := time.Now()
	if got := pipelinePuts(t, sim, "k", 100000); got != 100000 {
		t.Fatalf("只写了 %d 条，想要 100000", got)
	}
	elapsed := time.Since(start)
	t.Logf("100k 条流水线写完：%v", elapsed)

	// 最后一条（下标 100000）处的快照触发在 apply 循环尾巴上，
	// Settle 只等 lastApplied——先等快照落地再断言保留日志。
	for _, id := range sim.IDs() {
		waitSnapshotIndex(t, sim, id, 100000, 10*time.Second)
	}
	// 每个节点：快照已拍、保留日志 < 5000、KV 全在。
	for _, id := range sim.IDs() {
		st := nodeSnapshotStats(sim, id)
		t.Logf("节点 %s：snapshotIndex=%d logLen=%d lastApplied=%d kvLen=%d",
			id, st.snapshotIndex, st.logLen, st.lastApplied, st.kvLen)
		if st.snapshotIndex < 95000 {
			t.Fatalf("节点 %s 的快照下标应 >= 95000（100k 条、每 5000 拍一次），实际 %d", id, st.snapshotIndex)
		}
		if st.logLen >= 5000 {
			t.Fatalf("节点 %s 快照后保留日志应 < 5000，实际 %d", id, st.logLen)
		}
		if st.lastApplied != 100000 {
			t.Fatalf("节点 %s lastApplied 应为 100000，实际 %d", id, st.lastApplied)
		}
		if st.kvLen != 100000 {
			t.Fatalf("节点 %s KV 应有 100000 条，实际 %d", id, st.kvLen)
		}
		// 磁盘上的 raft.log 也要真正变小（不只是内存）：
		// 每条约 47 字节，5000 条约 235KB；放宽到 512KB。
		logPath := filepath.Join(sim.nodes[id].dataDir, "raft.log")
		fi, err := os.Stat(logPath)
		if err != nil {
			t.Fatalf("节点 %s 读 raft.log 失败：%v", id, err)
		}
		if fi.Size() >= 512*1024 {
			t.Fatalf("节点 %s 磁盘上的 raft.log 应 < 512KB，实际 %d 字节", id, fi.Size())
		}
		// 快照文件在，且有实质内容（10 万条 KV 的全量，MB 级）。
		snapPath := filepath.Join(sim.nodes[id].dataDir, "snapshot")
		sfi, err := os.Stat(snapPath)
		if err != nil {
			t.Fatalf("节点 %s 没有快照文件：%v", id, err)
		}
		if sfi.Size() < 1024*1024 {
			t.Fatalf("节点 %s 快照文件应 > 1MB（10 万条 KV），实际 %d 字节", id, sfi.Size())
		}
		t.Logf("节点 %s 磁盘：raft.log=%d 字节，snapshot=%d 字节", id, fi.Size(), sfi.Size())
	}
	// 三节点 KV 完全一致 + 不变式。
	assertAllKVEqual(t, sim)
	if err := sim.CheckInvariants(); err != nil {
		t.Fatalf("100k 写完不变式失败：%v", err)
	}
	// 抽查几个 key。
	for _, i := range []int{0, 1, 49999, 50000, 99999} {
		k := fmt.Sprintf("k%06d", i)
		for _, id := range sim.IDs() {
			v, ok := sim.Get(id, k)
			if !ok || v != fmt.Sprintf("v-%s", k) {
				t.Fatalf("节点 %s 读不到 %s（got %q %v）", id, k, v, ok)
			}
		}
	}
	// 写完 100k 还能继续正常写。
	mustPut(t, sim, "after-snapshot", "yes")
	assertAllKVEqual(t, sim)
}

// TestSnapshot_SequentialPuts：走正常客户端路径（Sim.Put，逐条等提交）
// 写 6000 条，快照同样会触发（阈值 5000），保留日志 < 5000。
func TestSnapshot_SequentialPuts(t *testing.T) {
	sim := newTestSim(t, 43)
	mustSettle(t, sim, 2*time.Second)
	start := time.Now()
	for i := 0; i < 6000; i++ {
		mustPut(t, sim, fmt.Sprintf("s%04d", i), "v")
	}
	t.Logf("6000 条顺序写：%v", time.Since(start))
	for _, id := range sim.IDs() {
		waitSnapshotIndex(t, sim, id, 5000, 10*time.Second)
	}
	for _, id := range sim.IDs() {
		st := nodeSnapshotStats(sim, id)
		if st.snapshotIndex < 5000 {
			t.Fatalf("节点 %s 顺序写 6000 条后快照下标应 >= 5000，实际 %d", id, st.snapshotIndex)
		}
		if st.logLen >= 5000 {
			t.Fatalf("节点 %s 快照后保留日志应 < 5000，实际 %d", id, st.logLen)
		}
		if st.kvLen != 6000 {
			t.Fatalf("节点 %s KV 应有 6000 条，实际 %d", id, st.kvLen)
		}
	}
	assertAllKVEqual(t, sim)
	if err := sim.CheckInvariants(); err != nil {
		t.Fatalf("顺序写完不变式失败：%v", err)
	}
}

// TestSnapshot_LaggingFollowerInstallSnapshot：落后 9000 条以上的 follower
// 靠 InstallSnapshot 追上，不走逐条复制。
func TestSnapshot_LaggingFollowerInstallSnapshot(t *testing.T) {
	sim := newTestSim(t, 44)
	mustSettle(t, sim, 2*time.Second)
	leader := sim.findLeader()
	if leader == nil {
		t.Fatal("没有 leader")
	}
	// 挑一个 follower 隔离（不是 leader 的那个）：单独成组，
	// 与另外两个节点完全断开（光 Partition(lagging, leader) 不够——
	// 它还能通过另一个 follower 间接通信）。
	lagging := ""
	others := []string{}
	for _, id := range sim.IDs() {
		if id != leader.id {
			if lagging == "" {
				lagging = id
			} else {
				others = append(others, id)
			}
		} else {
			others = append(others, id)
		}
	}
	sim.SetPartition([][]string{{others[0], others[1]}, {lagging}})
	t.Logf("隔离 %s（leader 是 %s），多数派写 9500 条", lagging, leader.id)
	if got := pipelinePuts(t, sim, "p", 9500); got != 9500 {
		t.Fatalf("只写了 %d 条，想要 9500", got)
	}
	// 多数派这边：快照已拍到 5000，保留日志 < 5000。
	leaderStats := nodeSnapshotStats(sim, leader.id)
	if leaderStats.snapshotIndex != 5000 {
		t.Fatalf("leader 快照下标应为 5000，实际 %d", leaderStats.snapshotIndex)
	}
	if leaderStats.logLen >= 5000 {
		t.Fatalf("leader 保留日志应 < 5000，实际 %d", leaderStats.logLen)
	}
	// 被隔离的节点什么都没收到。
	lagStats := nodeSnapshotStats(sim, lagging)
	if lagStats.lastApplied != 0 {
		t.Fatalf("被隔离的 %s 不应收到任何写，lastApplied=%d", lagging, lagStats.lastApplied)
	}
	before := sim.Network().SnapshotDeliveries()
	sim.Heal()
	// 等落后节点追上（最多 60 秒虚拟时间）。
	// 注意：Advance 只推进虚拟时钟，apply 循环是真实 goroutine，
	// 循环转得太快会把它饿死——所以虚拟时间走完后还要 Settle
	// （真实时间 drain，等 apply 消费完已投递的消息）。
	for waited := time.Duration(0); waited < 60*time.Second; waited += 500 * time.Millisecond {
		sim.Advance(500 * time.Millisecond)
		if sim.Debug(lagging).LastApplied >= 9500 {
			break
		}
	}
	mustSettle(t, sim, 5*time.Second)
	if sim.Debug(lagging).LastApplied < 9500 {
		t.Fatalf("恢复后 %s 没追上（lastApplied=%d）", lagging, sim.Debug(lagging).LastApplied)
	}
	// 观测证据：InstallSnapshot 真的被投递过（不只是最终数据一致——
	// 逐条复制也能让数据一致，但它发不出去快照）。
	deliveries := sim.Network().SnapshotDeliveries() - before
	if deliveries == 0 {
		t.Fatalf("恢复后一次 InstallSnapshot 都没投递——%s 可能是逐条复制追上的", lagging)
	}
	t.Logf("恢复后 InstallSnapshot 投递了 %d 次", deliveries)
	// 落后节点安装了 leader 的快照（下标 5000），不是自己慢慢拍出来的。
	waitSnapshotIndex(t, sim, lagging, 5000, 10*time.Second)
	lagStats = nodeSnapshotStats(sim, lagging)
	if lagStats.snapshotIndex != 5000 {
		t.Fatalf("%s 安装快照后下标应为 5000，实际 %d", lagging, lagStats.snapshotIndex)
	}
	assertAllKVEqual(t, sim)
	assertAllLogsEqual(t, sim)
	if err := sim.CheckInvariants(); err != nil {
		t.Fatalf("追上后不变式失败：%v", err)
	}
}

// TestSnapshot_RestartFromSnapshot：模拟器里写够触发快照的量，
// 全部节点 Kill 再 Restart，从快照恢复后数据全在、还能继续写。
func TestSnapshot_RestartFromSnapshot(t *testing.T) {
	sim := newTestSim(t, 45)
	mustSettle(t, sim, 2*time.Second)
	if got := pipelinePuts(t, sim, "r", 6000); got != 6000 {
		t.Fatalf("只写了 %d 条，想要 6000", got)
	}
	for _, id := range sim.IDs() {
		waitSnapshotIndex(t, sim, id, 5000, 10*time.Second)
	}
	for _, id := range sim.IDs() {
		if st := nodeSnapshotStats(sim, id); st.snapshotIndex < 5000 {
			t.Fatalf("节点 %s 快照下标应 >= 5000，实际 %d", id, st.snapshotIndex)
		}
	}
	// 全部 Kill 再 Restart。
	for _, id := range sim.IDs() {
		if err := sim.Kill(id); err != nil {
			t.Fatalf("Kill %s 失败：%v", id, err)
		}
	}
	for _, id := range sim.IDs() {
		if err := sim.Restart(id); err != nil {
			t.Fatalf("Restart %s 失败：%v", id, err)
		}
	}
	mustSettle(t, sim, 5*time.Second)
	// 数据全在：抽查快照里与快照之后的 key。
	for _, i := range []int{0, 4999, 5000, 5999} {
		k := fmt.Sprintf("r%06d", i)
		for _, id := range sim.IDs() {
			v, ok := sim.Get(id, k)
			if !ok || v != fmt.Sprintf("v-%s", k) {
				t.Fatalf("重启后节点 %s 读不到 %s（got %q %v）", id, k, v, ok)
			}
		}
	}
	// 还能继续写。
	mustPut(t, sim, "after-restart", "yes")
	assertAllKVEqual(t, sim)
	if err := sim.CheckInvariants(); err != nil {
		t.Fatalf("重启后不变式失败：%v", err)
	}
}

// TestSnapshot_AppendEntriesAfterSnapshot：快照之后，leader 与 follower
// 的 AppendEntries 仍按绝对下标走（PrevLogIndex == 快照位置时验快照任期）。
func TestSnapshot_AppendEntriesAfterSnapshot(t *testing.T) {
	sim := newTestSim(t, 46)
	mustSettle(t, sim, 2*time.Second)
	if got := pipelinePuts(t, sim, "a", 6000); got != 6000 {
		t.Fatalf("只写了 %d 条，想要 6000", got)
	}
	// 快照之后再写 100 条（走 AppendEntries，PrevLogIndex 在快照之后）。
	for i := 0; i < 100; i++ {
		mustPut(t, sim, fmt.Sprintf("a-after-%d", i), "v")
	}
	assertAllKVEqual(t, sim)
	assertAllLogsEqual(t, sim)
	if err := sim.CheckInvariants(); err != nil {
		t.Fatalf("快照后继续写，不变式失败：%v", err)
	}
	for _, id := range sim.IDs() {
		st := nodeSnapshotStats(sim, id)
		if st.lastApplied != 6100 {
			t.Fatalf("节点 %s lastApplied 应为 6100，实际 %d", id, st.lastApplied)
		}
	}
}

// Package raft_test 的 M5 快照测试：真实集群（3 个 server 进程）上的
// 快照验收——重启后从快照恢复、落后 follower 靠 InstallSnapshot 追赶。
// 与 crash_test.go 共用 crashCluster（同包，端口 18401–18403，
// 测试串行跑，不会撞端口）。
package raft_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// waitApplied 等节点的 lastApplied 达到 want（不看 logLen——
// 快照会压缩日志，logLen 永远追不上 want；见 crash_test.go 的
// waitCaughtUp，它断言 logLen，不适合快照测试）。
func (c *crashCluster) waitApplied(id string, want int, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if dbg, err := c.debug(id); err == nil && dbg.LastApplied >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	dbg, _ := c.debug(id)
	c.t.Fatalf("节点 %s %v 内 lastApplied 没达到 %d（当前 %+v）", id, timeout, want, dbg)
}

// waitSnapshotIndex 等节点的快照下标达到 want。
func (c *crashCluster) waitSnapshotIndex(id string, want int, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if dbg, err := c.debug(id); err == nil && dbg.SnapshotIndex >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	dbg, _ := c.debug(id)
	c.t.Fatalf("节点 %s %v 内快照下标没达到 %d（当前 %+v）", id, timeout, want, dbg)
}

// putManyToLeader 向 leader 写 n 条（key 前缀 + 6 位序号，值同名）。
// 只找一次 leader，写的过程里 leader 换了就重找、那条重写——
// putToLeader 每条都找一次 leader，12k 条要 30–50 秒，太慢。
// 返回实际写耗时（测试日志里回报）。
func (c *crashCluster) putManyToLeader(n int, prefix string) time.Duration {
	c.t.Helper()
	start := time.Now()
	leader := c.waitLeader(10 * time.Second)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%s%06d", prefix, i)
		status, ok, err := c.putRaw(c.addr(leader), key, "v-"+key)
		if err != nil || status != http.StatusOK || !ok {
			// 不是 leader 了（换届或短暂失联）：重找 leader，这条重写。
			leader = c.waitLeader(10 * time.Second)
			status, ok, err = c.putRaw(c.addr(leader), key, "v-"+key)
			if err != nil || status != http.StatusOK || !ok {
				c.t.Fatalf("写 %s 失败（重找 leader %s 后仍不行）：status=%d ok=%v err=%v", key, leader, status, ok, err)
			}
		}
	}
	return time.Since(start)
}

// readLinearizable 走 /kv/read 读一个 key（跟 not_leader 重定向），
// 读到的是线性化点处的值——重启恢复的抽查必须用它，
// 否则 /kv/get 可能读到落后副本的旧数据，造成误判。
func (c *crashCluster) readLinearizable(key string) (value string, found bool) {
	c.t.Helper()
	body, _ := json.Marshal(map[string]string{"key": key})
	for _, id := range crashIDs {
		resp, err := c.client.Post("http://"+c.addr(id)+"/kv/read", "application/json", bytes.NewReader(body))
		if err != nil {
			continue
		}
		var r struct {
			Value  string `json:"value"`
			Found  bool   `json:"found"`
			Error  string `json:"error"`
			Leader string `json:"leader"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&r)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return r.Value, r.Found
		}
		if r.Error == "not_leader" && r.Leader != "" {
			resp2, err2 := c.client.Post("http://"+c.addr(r.Leader)+"/kv/read", "application/json", bytes.NewReader(body))
			if err2 != nil {
				continue
			}
			var r2 struct {
				Value string `json:"value"`
				Found bool   `json:"found"`
			}
			_ = json.NewDecoder(resp2.Body).Decode(&r2)
			resp2.Body.Close()
			if resp2.StatusCode == http.StatusOK {
				return r2.Value, r2.Found
			}
		}
	}
	c.t.Fatalf("线性化读 %s 失败（三个节点都读不到）", key)
	return "", false
}

// logContains 查某节点的 server 日志文件（<workDir>/<id>.log）里
// 有没有包含 substr 的行（观测证据：快照发送/安装的日志行）。
func (c *crashCluster) logContains(id, substr string) bool {
	c.t.Helper()
	data, err := os.ReadFile(filepath.Join(c.workDir, id+".log"))
	if err != nil {
		c.t.Fatalf("读节点 %s 的日志失败：%v", id, err)
	}
	return strings.Contains(string(data), substr)
}

// TestSnapshot_RestartFromSnapshot_RealCluster 是 M5 的真实集群验收：
// 写 12000 条（触发快照），kill -9 全部节点再重启，
// 随机抽 100 个 key 走线性化读，全部正确；再写几条验证集群还能工作。
//
// 写量说明：规划验收是真实集群写 100k；HTTP 逐条写 100k 在
// 本机实测约 0.95ms/条，要 ~95 秒——太慢。
// 12k 已远超快照阈值（5000），足以验证“快照触发→落盘→重启恢复”
// 整条链路；100k 的量级由模拟器测试覆盖（见 sim/snapshot_test.go）。
func TestSnapshot_RestartFromSnapshot_RealCluster(t *testing.T) {
	c := newCrashCluster(t)
	c.startAll()
	leader := c.waitLeader(10 * time.Second)
	t.Logf("首任 leader 是 %s", leader)

	elapsed := c.putManyToLeader(12000, "rk")
	t.Logf("真实集群写 12000 条：%v", elapsed)
	for _, id := range crashIDs {
		c.waitApplied(id, 12000, 30*time.Second)
		c.waitSnapshotIndex(id, 5000, 30*time.Second)
		dbg, err := c.debug(id)
		if err != nil {
			t.Fatal(err)
		}
		if dbg.LogLen >= 5000 {
			t.Fatalf("节点 %s 快照后保留日志应 < 5000，实际 %d（snapshotIndex=%d）", id, dbg.LogLen, dbg.SnapshotIndex)
		}
		// 磁盘上的 raft.log 也要真正变小。
		logPath := filepath.Join(c.dataDir(id), "raft.log")
		fi, err := os.Stat(logPath)
		if err != nil {
			t.Fatalf("节点 %s 读 raft.log 失败：%v", id, err)
		}
		if fi.Size() >= 512*1024 {
			t.Fatalf("节点 %s 磁盘上的 raft.log 应 < 512KB，实际 %d 字节", id, fi.Size())
		}
		snapPath := filepath.Join(c.dataDir(id), "snapshot")
		if _, err := os.Stat(snapPath); err != nil {
			t.Fatalf("节点 %s 没有快照文件：%v", id, err)
		}
		t.Logf("节点 %s：snapshotIndex=%d logLen=%d lastApplied=%d，磁盘 raft.log=%d 字节",
			id, dbg.SnapshotIndex, dbg.LogLen, dbg.LastApplied, fi.Size())
	}

	// kill -9 全部节点，再逐个重启。
	for _, id := range crashIDs {
		c.kill9(id)
	}
	for _, id := range crashIDs {
		c.start(id)
	}
	leader = c.waitLeader(10 * time.Second)
	t.Logf("重启后 leader 是 %s", leader)
	for _, id := range crashIDs {
		c.waitApplied(id, 12000, 30*time.Second)
	}

	// 随机抽 100 个 key，走线性化读核对。
	rng := rand.New(rand.NewSource(7))
	checked := map[int]bool{}
	for len(checked) < 100 {
		i := rng.Intn(12000)
		if checked[i] {
			continue
		}
		checked[i] = true
		key := fmt.Sprintf("rk%06d", i)
		v, found := c.readLinearizable(key)
		if !found || v != "v-"+key {
			t.Fatalf("重启后线性化读 %s 失败：found=%v value=%q，想要 %q", key, found, v, "v-"+key)
		}
	}
	t.Logf("重启后随机抽 100 个 key，线性化读全部正确")

	// 集群还能正常工作：再写 5 条并读回。
	for i := 0; i < 5; i++ {
		c.putToLeader(fmt.Sprintf("after-restart-%d", i), "yes")
	}
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("after-restart-%d", i)
		v, found := c.readLinearizable(key)
		if !found || v != "yes" {
			t.Fatalf("重启后新写 %s 读不到：found=%v value=%q", key, found, v)
		}
	}
	for _, id := range crashIDs {
		dbg, _ := c.debug(id)
		if dbg.SnapshotIndex < 5000 || dbg.LastApplied < 12005 {
			t.Fatalf("节点 %s 重启后状态不对：%+v", id, dbg)
		}
	}
}

// TestSnapshot_LaggingFollowerInstallSnapshot_RealCluster 是 M5 的
// 真实集群追赶验收：杀掉一个 follower，多数派写 9000 条
// （leader 快照到 5000，落后节点落后 9000 条、超出保留日志），
// 重启 follower，断言它通过 InstallSnapshot 追上——
// 观测证据是 follower 日志里的“安装快照”行与 leader 日志里的
// “发快照”行，不只是最终数据一致（逐条复制也能让数据一致，
// 但它发不出去快照；且逐条复制在这里根本走不通：
// follower 的 nextIndex 早已掉进快照里）。
func TestSnapshot_LaggingFollowerInstallSnapshot_RealCluster(t *testing.T) {
	c := newCrashCluster(t)
	c.startAll()
	leader := c.waitLeader(10 * time.Second)
	lagging := ""
	for _, id := range crashIDs {
		if id != leader {
			lagging = id
			break
		}
	}
	t.Logf("leader 是 %s，杀掉 follower %s", leader, lagging)
	c.kill9(lagging)

	elapsed := c.putManyToLeader(9000, "pk")
	t.Logf("多数派写 9000 条：%v", elapsed)
	c.waitSnapshotIndex(leader, 5000, 30*time.Second)
	dbg, _ := c.debug(leader)
	if dbg.LogLen >= 5000 {
		t.Fatalf("leader 快照后保留日志应 < 5000，实际 %d", dbg.LogLen)
	}
	t.Logf("leader：snapshotIndex=%d logLen=%d lastApplied=%d", dbg.SnapshotIndex, dbg.LogLen, dbg.LastApplied)

	// 重启落后节点，等它追上。
	c.start(lagging)
	c.waitApplied(lagging, 9000, 60*time.Second)
	lagDbg, _ := c.debug(lagging)
	if lagDbg.SnapshotIndex != 5000 {
		t.Fatalf("落后节点 %s 安装快照后下标应为 5000，实际 %d", lagging, lagDbg.SnapshotIndex)
	}
	if lagDbg.LogLen >= 5000 {
		t.Fatalf("落后节点 %s 追上后保留日志应 < 5000，实际 %d", lagging, lagDbg.LogLen)
	}
	// 观测证据：两边的快照日志行。
	if !c.logContains(lagging, "安装快照：lastIncludedIndex=5000") {
		t.Fatalf("落后节点 %s 的日志里没有“安装快照”行——可能是逐条复制追上的", lagging)
	}
	if !c.logContains(leader, "给 "+lagging+" 发快照：lastIncludedIndex=5000") {
		t.Fatalf("leader %s 的日志里没有给 %s 发快照的行", leader, lagging)
	}
	t.Logf("观测到 InstallSnapshot：leader 发快照，%s 安装快照（下标 5000）", lagging)
	// 数据一致：抽查快照里与快照之后的 key。
	for _, i := range []int{0, 4999, 5000, 8999} {
		key := fmt.Sprintf("pk%06d", i)
		v, found := c.readLinearizable(key)
		if !found || v != "v-"+key {
			t.Fatalf("追上后线性化读 %s 失败：found=%v value=%q", key, found, v)
		}
	}
}

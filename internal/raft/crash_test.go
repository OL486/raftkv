// Package raft_test 的崩溃测试：起 3 个真实 server 进程（各用独立 --data
// 目录），写 100 条，逐个 kill -9 再重启，验证数据、任期、日志长度。
//
// 注意：这是真实进程测试，不是进程内集成测试（integration_test.go）。
// server 二进制在测试里用 `go build` 现场编译到 t.TempDir()，
// go 工具链从 runtime.GOROOT() 里找。
package raft_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/OL486/raftkv/internal/raft"
)

// 崩溃测试用 18401–18403 端口（与 M1 进程内集成测试的 18201–18303
// 和 node 包测试的 18301 错开，避免并行跑包时撞端口）。
var crashPorts = map[string]int{"node1": 18401, "node2": 18402, "node3": 18403}

var crashIDs = []string{"node1", "node2", "node3"}

// buildServerBinary 把 cmd/server 编到 tmpDir/raftkv-server，返回路径。
func buildServerBinary(t *testing.T, tmpDir string) string {
	t.Helper()
	goBin := filepath.Join(runtime.GOROOT(), "bin", "go")
	if runtime.GOROOT() == "" {
		goBin = "go" // 兜底：走 PATH
	}
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller 拿不到本文件路径")
	}
	root := filepath.Dir(filepath.Dir(filepath.Dir(thisFile))) // <root>/internal/raft/crash_test.go
	serverPkg := filepath.Join(root, "cmd", "server")
	out := filepath.Join(tmpDir, "raftkv-server")
	cmd := exec.Command(goBin, "build", "-o", out, serverPkg)
	cmd.Dir = root
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("编译 server 二进制失败：%v\n%s", err, stderr.String())
	}
	return out
}

// crashCluster 管三个真实 server 进程。
type crashCluster struct {
	t       *testing.T
	bin     string
	workDir string // t.TempDir()：cluster.json、data1-3、日志都在里面
	cfgPath string
	procs   map[string]*exec.Cmd
	client  *http.Client
}

func newCrashCluster(t *testing.T) *crashCluster {
	t.Helper()
	workDir := t.TempDir()
	bin := buildServerBinary(t, workDir)
	cfg := struct {
		Nodes []struct {
			ID   string `json:"id"`
			Addr string `json:"addr"`
		} `json:"nodes"`
	}{}
	for _, id := range crashIDs {
		cfg.Nodes = append(cfg.Nodes, struct {
			ID   string `json:"id"`
			Addr string `json:"addr"`
		}{ID: id, Addr: fmt.Sprintf("127.0.0.1:%d", crashPorts[id])})
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(workDir, "cluster.json")
	if err := os.WriteFile(cfgPath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	c := &crashCluster{
		t:       t,
		bin:     bin,
		workDir: workDir,
		cfgPath: cfgPath,
		procs:   make(map[string]*exec.Cmd),
		client:  &http.Client{Timeout: 3 * time.Second},
	}
	t.Cleanup(func() {
		for id, p := range c.procs {
			if p != nil && p.Process != nil {
				_ = p.Process.Kill()
			}
			_ = id
		}
	})
	return c
}

func (c *crashCluster) dataDir(id string) string {
	return filepath.Join(c.workDir, "data-"+id)
}

func (c *crashCluster) addr(id string) string {
	return fmt.Sprintf("127.0.0.1:%d", crashPorts[id])
}

// start 起一个节点（同一 --data 目录重启就是恢复）。
// 等它的 HTTP 端口能响应 /debug/log 才返回。
func (c *crashCluster) start(id string) {
	c.t.Helper()
	logPath := filepath.Join(c.workDir, id+".log")
	logFile, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		c.t.Fatal(err)
	}
	cmd := exec.Command(c.bin, "--id", id, "--config", c.cfgPath, "--data", c.dataDir(id))
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		c.t.Fatalf("启动节点 %s 失败：%v", id, err)
	}
	c.procs[id] = cmd
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := c.debug(id); err == nil {
			return
		}
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			c.t.Fatalf("节点 %s 启动后立即退出，日志见 %s", id, logPath)
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("节点 %s 10 秒内 HTTP 未响应，日志见 %s", id, logPath)
}

// startAll 起三个节点。
func (c *crashCluster) startAll() {
	for _, id := range crashIDs {
		c.start(id)
	}
}

// kill9 杀掉一个节点（模拟 kill -9 崩溃：不给它优雅停机的机会）。
func (c *crashCluster) kill9(id string) {
	c.t.Helper()
	p := c.procs[id]
	if p == nil || p.Process == nil {
		c.t.Fatalf("节点 %s 没在跑，杀不了", id)
	}
	if err := p.Process.Kill(); err != nil {
		c.t.Fatalf("kill -9 节点 %s 失败：%v", id, err)
	}
	if _, err := p.Process.Wait(); err != nil {
		c.t.Logf("节点 %s Wait 返回：%v", id, err)
	}
	c.procs[id] = nil
}

// debug 读节点的 /debug/log。
func (c *crashCluster) debug(id string) (raft.DebugInfo, error) {
	var dbg raft.DebugInfo
	resp, err := c.client.Get("http://" + c.addr(id) + "/debug/log")
	if err != nil {
		return dbg, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return dbg, err
	}
	if err := json.Unmarshal(data, &dbg); err != nil {
		return dbg, fmt.Errorf("/debug/log 不是合法 JSON：%v（%q）", err, data)
	}
	return dbg, nil
}

// waitLeader 等任意节点当选 leader，返回它的 id。
func (c *crashCluster) waitLeader(timeout time.Duration) string {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, id := range crashIDs {
			dbg, err := c.debug(id)
			if err == nil && dbg.Role == "leader" {
				return id
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("%v 内没有选出 leader", timeout)
	return ""
}

// waitCaughtUp 等节点追上：logLen 与 lastApplied 都达到 want。
func (c *crashCluster) waitCaughtUp(id string, want int, timeout time.Duration) raft.DebugInfo {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		dbg, err := c.debug(id)
		if err == nil && dbg.LogLen >= want && dbg.LastApplied >= want {
			return dbg
		}
		time.Sleep(50 * time.Millisecond)
	}
	dbg, _ := c.debug(id)
	c.t.Fatalf("节点 %s %v 内没追上（要 logLen/lastApplied ≥ %d，当前 %+v）", id, timeout, want, dbg)
	return dbg
}

// putRaw 直接向 addr 的 /kv/put 发写，返回 HTTP 状态码与 ok 字段。
// 不经过 CLI 的 leader 重定向——测试要精确控制“这笔写发给了谁”。
func (c *crashCluster) putRaw(addr, key, value string) (status int, ok bool, err error) {
	body, _ := json.Marshal(map[string]string{"key": key, "value": value})
	resp, err := c.client.Post("http://"+addr+"/kv/put", "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var pr struct {
		OK bool `json:"ok"`
	}
	_ = json.Unmarshal(data, &pr)
	return resp.StatusCode, pr.OK, nil
}

// putToLeader 向当前 leader 写一条，必须成功。
func (c *crashCluster) putToLeader(key, value string) {
	c.t.Helper()
	leader := c.waitLeader(5 * time.Second)
	status, ok, err := c.putRaw(c.addr(leader), key, value)
	if err != nil || status != http.StatusOK || !ok {
		c.t.Fatalf("向 leader %s 写 %s 失败：status=%d ok=%v err=%v", leader, key, status, ok, err)
	}
}

// get 读某节点的 key。
func (c *crashCluster) get(id, key string) (value string, found bool, err error) {
	resp, err := c.client.Get("http://" + c.addr(id) + "/kv/get?key=" + key)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", false, err
	}
	var gr struct {
		Value string `json:"value"`
		Found bool   `json:"found"`
	}
	if err := json.Unmarshal(data, &gr); err != nil {
		return "", false, err
	}
	return gr.Value, gr.Found, nil
}

// assertAllKeys 断言某节点能读到全部 key（带轮询：刚重启的节点
// 追上日志需要几百毫秒，不能只查一次就判失败）。
func (c *crashCluster) assertAllKeys(id string, n int, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allOK := true
		for i := 0; i < n; i++ {
			key := fmt.Sprintf("k%03d", i)
			v, found, err := c.get(id, key)
			if err != nil || !found || v != fmt.Sprintf("v%03d", i) {
				allOK = false
				break
			}
		}
		if allOK {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("节点 %s %v 内读不全 %d 条数据", id, timeout, n)
}

// TestCrashRestart_ThreeNodes 是 M2 的核心验收：写 100 条，逐个 kill -9
// 再重启，数据全在；重启后 currentTerm 与日志长度与杀之前一致
// （follower 重启后 term 不变；leader 重启会触发一次新选举，
// term 只会变大、不会变小，日志长度不变）。
func TestCrashRestart_ThreeNodes(t *testing.T) {
	c := newCrashCluster(t)
	c.startAll()
	leader := c.waitLeader(10 * time.Second)
	t.Logf("首任 leader 是 %s", leader)

	// 写 100 条。
	for i := 0; i < 100; i++ {
		c.putToLeader(fmt.Sprintf("k%03d", i), fmt.Sprintf("v%03d", i))
	}
	// 记下杀之前的 term 与日志长度。
	termBefore := map[string]int{}
	for _, id := range crashIDs {
		dbg := c.waitCaughtUp(id, 100, 10*time.Second)
		termBefore[id] = dbg.Term
		if dbg.LogLen != 100 {
			t.Fatalf("写完 100 条后节点 %s 的日志长度应为 100，实际 %+v", id, dbg)
		}
	}
	t.Logf("写完 100 条，各节点 term=%v，日志长度 100", termBefore)

	// 逐个 kill -9 再重启：先两个 follower，再 leader。
	restartOrder := []string{"node3", "node2", leader}
	seen := map[string]bool{}
	for _, id := range restartOrder {
		if seen[id] {
			continue
		}
		seen[id] = true
		isLeader := id == leader
		c.kill9(id)
		t.Logf("已 kill -9 %s（leader=%v），正在重启……", id, isLeader)
		c.start(id)
		dbg := c.waitCaughtUp(id, 100, 10*time.Second)
		if dbg.LogLen != 100 {
			t.Fatalf("节点 %s 重启后日志长度应为 100，实际 %+v", id, dbg)
		}
		if !isLeader && dbg.Term != termBefore[id] {
			t.Fatalf("follower %s 重启后 term 应不变（%d），实际 %+v", id, termBefore[id], dbg)
		}
		if isLeader && dbg.Term < termBefore[id] {
			t.Fatalf("leader %s 重启后 term 不应变小（之前 %d），实际 %+v", id, termBefore[id], dbg)
		}
		c.assertAllKeys(id, 100, 10*time.Second)
		t.Logf("节点 %s 重启恢复 OK：term=%d logLen=%d lastApplied=%d，100 条全在", id, dbg.Term, dbg.LogLen, dbg.LastApplied)
		// leader 重启后要等新 leader 选出来（旧 leader 回来可能还是 follower）。
		if isLeader {
			newLeader := c.waitLeader(5 * time.Second)
			t.Logf("leader 重启后，新 leader 是 %s", newLeader)
			leader = newLeader
		}
	}

	// 最终：三个节点日志长度都是 100，100 条全能读到。
	for _, id := range crashIDs {
		dbg, err := c.debug(id)
		if err != nil {
			t.Fatal(err)
		}
		if dbg.LogLen != 100 {
			t.Fatalf("最终节点 %s 的日志长度应为 100，实际 %+v", id, dbg)
		}
		c.assertAllKeys(id, 100, 10*time.Second)
	}
	// 重启后写入继续正常。
	c.putToLeader("k100", "v100")
	for _, id := range crashIDs {
		c.assertAllKeys(id, 101, 10*time.Second)
	}
	t.Logf("重启后写入 k100 成功，三节点都读到了")
}

// TestCrashRestart_WriteInFlight 覆盖“杀 leader 的瞬间正在写一笔”：
// 写操作的结果与重启后的状态必须一致——
// 成功返回 ⇒ 重启后一定能读到；明确报错 ⇒ 重启后一定读不到。
// 不会出现“返回成功但重启后丢了”或“明确报错但重启后出现了”。
func TestCrashRestart_WriteInFlight(t *testing.T) {
	c := newCrashCluster(t)
	c.startAll()
	leader := c.waitLeader(10 * time.Second)
	c.putToLeader("base", "1") // 先写一条打底，确认集群正常

	// 杀掉 leader。
	c.kill9(leader)
	t.Logf("已 kill -9 leader %s", leader)

	// (a) 直接向已死的 leader 地址写：连接必然被拒（写根本没进任何日志），
	// 重启后这条一定读不到。
	_, _, errDead := c.putRaw(c.addr(leader), "dead-write", "x")
	if errDead == nil {
		t.Fatal("向已死的 leader 写应连接失败")
	}
	t.Logf("向已死 leader 写如预期地失败：%v", errDead)

	// (b) 立刻向一个存活节点写，记录结果（窗口期极短，结果取决于
	// 该节点此刻是不是已经当选新 leader——两种结果都合法，
	// 但结果与重启后的状态必须一致）。
	survivor := ""
	for _, id := range crashIDs {
		if id != leader {
			survivor = id
			break
		}
	}
	status, ok, errSurv := c.putRaw(c.addr(survivor), "race-write", "y")
	outcome := ""
	switch {
	case errSurv != nil:
		outcome = "error" // 连接/超时错误：写没进日志
	case status == http.StatusOK && ok:
		outcome = "success" // 该节点已是新 leader，写进日志了
	case status == http.StatusServiceUnavailable:
		outcome = "error" // 503 not_leader：写没进日志
	default:
		// 504 等不确定结果：这笔写可能进日志了也可能没有，
		// 不拿它断言，改走新 leader 重写一条确定的。
		outcome = "uncertain"
		t.Logf("存活节点写返回不确定结果（status=%d），跳过该条的断言", status)
	}
	t.Logf("向存活节点 %s 写 race-write：outcome=%s (status=%d ok=%v err=%v)", survivor, outcome, status, ok, errSurv)

	// 等新 leader 选出来。
	newLeader := c.waitLeader(5 * time.Second)
	t.Logf("新 leader 是 %s", newLeader)

	// 新 leader 刚当选时 commitIndex 还是 0：按 Raft 图 2 的规则，
	// 上一任期的日志要等本任期有新条目提交后才能一起提交
	// （leader 只能用本任期条目推进 commitIndex）。
	// 先经新 leader 写一条探针，把前缀一起提交了，再断言。
	// 注意：探针写不影响下面的断言——dead-write 从没进过任何日志，
	// race-write 进没进日志只取决于它当时返回的结果。
	c.putToLeader("probe", "1")
	c.waitCaughtUp(newLeader, 1, 10*time.Second)
	if _, found, err := c.get(newLeader, "dead-write"); err != nil || found {
		t.Fatalf("明确报错的 dead-write 重启后不应出现（found=%v err=%v）", found, err)
	}

	// (b) 的断言：结果与状态一致。
	if outcome != "uncertain" {
		_, found, err := c.get(newLeader, "race-write")
		if err != nil {
			t.Fatal(err)
		}
		if outcome == "success" && !found {
			t.Fatal("返回成功的 race-write 重启后丢了")
		}
		if outcome == "error" && found {
			t.Fatal("明确报错的 race-write 重启后却出现了")
		}
		t.Logf("race-write：outcome=%s，重启后 found=%v，一致", outcome, found)
	}

	// (c) 成功返回的写必须耐崩溃：经新 leader 写一条，杀掉新 leader 再重启，
	// 这条必须还在。
	c.putToLeader("durable-write", "z")
	c.kill9(newLeader)
	t.Logf("已 kill -9 新 leader %s，正在重启……", newLeader)
	c.start(newLeader)
	c.waitCaughtUp(newLeader, 2, 10*time.Second)
	c.waitForKey(newLeader, "durable-write", "z", 10*time.Second)
	// 最初被杀的 leader 也重启：它应带着全部数据回来。
	// 注意：它重启时日志比别人短（死得早），按 Raft 规则选不上 leader，
	// 只会短暂搅动一次选举，随后由其他节点重新选出 leader，它再跟上。
	t.Logf("重启最初被杀的 leader %s……", leader)
	c.start(leader)
	c.waitCaughtUp(leader, 2, 10*time.Second)
	// 关键一步：经当前 leader 写一条探针，再断言各节点的状态机。
	// Raft 图 2 的提交规则：leader 只能用“本任期”的条目推进
	// commitIndex——如果最后当选的 leader 是当初 commitIndex
	// 还没追上的节点（例如本测试里死得早的旧 leader），
	// 它手里虽然已经有全部日志（logLen 是满的），commitIndex
	// 却会卡在旧任期，旧条目迟迟进不了状态机；
	// 这不是持久化丢数据（日志都在），是 Raft 本身的语义，
	// 新 leader 只要提交一条本任期的写，前缀就一起提交了。
	// 所以这里必须先写一条，再查各节点的状态机。
	c.putToLeader("final-probe", "ok")
	// 三个节点都应有 durable-write、base 与 final-probe。
	for _, id := range crashIDs {
		c.waitForKey(id, "durable-write", "z", 10*time.Second)
		c.waitForKey(id, "base", "1", 10*time.Second)
		c.waitForKey(id, "final-probe", "ok", 10*time.Second)
	}
}

// waitForKey 等某节点读到 key=value（刚重启的节点追上日志需要时间）。
func (c *crashCluster) waitForKey(id, key, value string, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		v, found, err := c.get(id, key)
		if err == nil && found && v == value {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	v, found, err := c.get(id, key)
	c.t.Fatalf("节点 %s %v 内读不到 %s=%s（最后一次 found=%v value=%q err=%v）", id, timeout, key, value, found, v, err)
}

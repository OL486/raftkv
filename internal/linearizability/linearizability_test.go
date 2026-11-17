package linearizability_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/OL486/raftkv/internal/linearizability"
	"github.com/OL486/raftkv/internal/sim"
)

// ---- 模型与检查器的单元测试 ----

// TestModel_SequentialHistory：顺序历史（写后读到新值）一定线性一致。
func TestModel_SequentialHistory(t *testing.T) {
	history := []porcupine.Operation{
		{ClientId: 0, Input: linearizability.PutInput{Key: "k", Value: "v1"}, Call: 0, Output: linearizability.PutOutput{OK: true}, Return: 10},
		{ClientId: 0, Input: linearizability.ReadInput{Key: "k"}, Call: 20, Output: linearizability.ReadOutput{Value: "v1", Found: true}, Return: 30},
		{ClientId: 0, Input: linearizability.ReadInput{Key: "missing"}, Call: 40, Output: linearizability.ReadOutput{Found: false}, Return: 50},
	}
	if ok, diag := linearizability.Check(history); !ok {
		t.Fatalf("顺序历史应线性一致：\n%s", diag)
	}
}

// TestChecker_Counterexample：反例——Put 返回成功之后才开始的读
// 却返回旧值（读不到），这段历史一定不是线性一致的；
// 检查器必须报不通过（验证检查器真能抓到问题）。
func TestChecker_Counterexample(t *testing.T) {
	history := []porcupine.Operation{
		{ClientId: 0, Input: linearizability.PutInput{Key: "k", Value: "v1"}, Call: 0, Output: linearizability.PutOutput{OK: true}, Return: 10},
		// 读在 Put 返回之后（Call=20 > Return=10）才开始，却读不到 k：
		// Put 必须线性化在读开始之前，所以读到“不存在”是不可能的。
		{ClientId: 1, Input: linearizability.ReadInput{Key: "k"}, Call: 20, Output: linearizability.ReadOutput{Found: false}, Return: 30},
	}
	if ok, _ := linearizability.Check(history); ok {
		t.Fatal("反例历史应被判定为非线性一致，检查器却放过了")
	}
}

// TestChecker_StaleReadValue：另一个反例——Put(k=v2) 返回后，
// 读返回旧值 v1（而不是 v2 或“不存在”），同样不线性一致。
func TestChecker_StaleReadValue(t *testing.T) {
	history := []porcupine.Operation{
		{ClientId: 0, Input: linearizability.PutInput{Key: "k", Value: "v1"}, Call: 0, Output: linearizability.PutOutput{OK: true}, Return: 10},
		{ClientId: 0, Input: linearizability.PutInput{Key: "k", Value: "v2"}, Call: 20, Output: linearizability.PutOutput{OK: true}, Return: 30},
		{ClientId: 1, Input: linearizability.ReadInput{Key: "k"}, Call: 40, Output: linearizability.ReadOutput{Value: "v1", Found: true}, Return: 50},
	}
	if ok, _ := linearizability.Check(history); ok {
		t.Fatal("读到旧值的历史应被判定为非线性一致，检查器却放过了")
	}
}

// TestChecker_ConcurrentButLinearizable：重叠的操作也可以线性一致——
// 读与 Put 重叠、读到新值时，把读线性化在 Put 之后即可。
func TestChecker_ConcurrentButLinearizable(t *testing.T) {
	history := []porcupine.Operation{
		{ClientId: 0, Input: linearizability.PutInput{Key: "k", Value: "v2"}, Call: 0, Output: linearizability.PutOutput{OK: true}, Return: 100},
		{ClientId: 1, Input: linearizability.ReadInput{Key: "k"}, Call: 50, Output: linearizability.ReadOutput{Value: "v2", Found: true}, Return: 150},
	}
	if ok, diag := linearizability.Check(history); !ok {
		t.Fatalf("重叠但可线性化的历史应通过：\n%s", diag)
	}
}

// ---- 模拟器上的并发线性一致性测试 ----

const (
	linClients    = 3    // 并发客户端数
	linOpsTotal   = 1000 // 每个种子总操作数
	linKeySpace   = 10   // key 空间：k0..k9（key 少，冲突多，检查更严）
	linSeeds      = 20   // 种子数
	linPutPercent = 60   // Put 占比（其余是线性化读）
)

// genConcurrentOps 按种子生成每个客户端的操作序列：
// 每个 Put 的值全局唯一（v<seed>-<client>-<seq>），这样读到旧值
// 一定能被检查器发现（不会与新值撞上）；key 在 10 个里随机挑。
func genConcurrentOps(seed int64) [][]sim.ConcurrentOp {
	rng := rand.New(rand.NewSource(seed))
	perClient := make([][]sim.ConcurrentOp, linClients)
	base := linOpsTotal / linClients
	for c := 0; c < linClients; c++ {
		n := base
		if c == linClients-1 {
			n = linOpsTotal - base*(linClients-1)
		}
		ops := make([]sim.ConcurrentOp, 0, n)
		for i := 0; i < n; i++ {
			key := fmt.Sprintf("k%d", rng.Intn(linKeySpace))
			if rng.Intn(100) < linPutPercent {
				ops = append(ops, sim.ConcurrentOp{
					Kind:  "put",
					Key:   key,
					Value: fmt.Sprintf("v%d-%d-%d", seed, c, i),
				})
			} else {
				ops = append(ops, sim.ConcurrentOp{Kind: "read", Key: key})
			}
		}
		perClient[c] = ops
	}
	return perClient
}

// runSimSeed 跑一个种子的模拟器并发测试，返回历史与操作统计。
func runSimSeed(t *testing.T, seed int64) []porcupine.Operation {
	t.Helper()
	s := sim.NewSim(seed, t.TempDir(), []string{"node1", "node2", "node3"}, map[string]string{
		"node1": "127.0.0.1:19001",
		"node2": "127.0.0.1:19002",
		"node3": "127.0.0.1:19003",
	})
	t.Cleanup(s.Stop)
	if err := s.Start(); err != nil {
		t.Fatalf("seed=%d：Start 失败：%v", seed, err)
	}
	if err := s.Settle(2 * time.Second); err != nil {
		t.Fatalf("seed=%d：Settle 失败：%v", seed, err)
	}
	records, err := s.RunConcurrent(seed, genConcurrentOps(seed))
	if err != nil {
		t.Fatalf("seed=%d：并发运行失败：%v", seed, err)
	}
	for _, r := range records {
		if !r.OK {
			t.Fatalf("seed=%d：客户端 %d 的 %s %s 最终失败——干净集群里操作不应失败", seed, r.ClientID, r.Kind, r.Key)
		}
	}
	// 历史必须是真并发：与别的重叠的操作应占大多数。
	// 如果驱动悄悄退化成串行（每操作都等前一个返回），
	// 线性一致的检查就毫无意义——这个断言防的就是这个。
	overlapped := 0
	for i, a := range records {
		for j, b := range records {
			if i != j && a.Call <= b.Return && b.Call <= a.Return {
				overlapped++
				break
			}
		}
	}
	if overlapped*2 < len(records) {
		t.Fatalf("seed=%d：%d 条操作里只有 %d 条与别的重叠，并发度太低，检查失去意义", seed, len(records), overlapped)
	}
	return linearizability.FromSimRecords(records)
}

// TestLinearizability_Sim_20Seeds：3 个客户端并发发 1000 次
// Put/线性化读，20 个不同种子，Porcupine 验证全部线性一致。
// 同一个种子跑两次的历史完全相同（RunConcurrent 是确定性的），
// 所以这 20 个种子也是 20 个可复现的并发场景。
func TestLinearizability_Sim_20Seeds(t *testing.T) {
	start := time.Now()
	for seed := int64(1); seed <= linSeeds; seed++ {
		seedStart := time.Now()
		history := runSimSeed(t, seed)
		if ok, diag := linearizability.Check(history); !ok {
			t.Fatalf("seed=%d：历史不是线性一致的：\n%s", seed, diag)
		}
		t.Logf("seed=%d：%d 条操作，线性一致（%v）", seed, len(history), time.Since(seedStart))
	}
	total := time.Since(start)
	t.Logf("20 个种子全部线性一致，总耗时 %v（平均每种子 %v）", total, total/linSeeds)
	// 耗时上限：20 个种子 5 分钟。实测远低于此（见日志）；
	// 如果以后变慢，先分析是模拟器慢还是检查器慢，不要悄悄减种子数。
	if total > 5*time.Minute {
		t.Fatalf("20 个种子总耗时 %v，超过 5 分钟上限", total)
	}
}

// ---- 真实集群上的 smoke 测试 ----

// 真实集群 smoke 测试用 18501–18503 端口
// （与 M1 集成测试、M2 崩溃测试的端口错开，避免并行跑包时撞端口）。
var smokePorts = map[string]int{"node1": 18501, "node2": 18502, "node3": 18503}

var smokeIDs = []string{"node1", "node2", "node3"}

// buildServerBinary 把 cmd/server 编到 tmpDir/raftkv-server（与 crash_test.go 同款手法）。
func buildServerBinary(t *testing.T, tmpDir string) string {
	t.Helper()
	goBin := filepath.Join(runtime.GOROOT(), "bin", "go")
	if runtime.GOROOT() == "" {
		goBin = "go"
	}
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller 拿不到本文件路径")
	}
	root := filepath.Dir(filepath.Dir(filepath.Dir(thisFile))) // <root>/internal/linearizability/linearizability_test.go
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

// smokeCluster 管三个真实 server 进程。
type smokeCluster struct {
	t       *testing.T
	bin     string
	workDir string
	cfgPath string
	addrs   []string
	procs   map[string]*exec.Cmd
	client  *http.Client
}

func newSmokeCluster(t *testing.T) *smokeCluster {
	t.Helper()
	workDir := t.TempDir()
	bin := buildServerBinary(t, workDir)
	cfg := struct {
		Nodes []struct {
			ID   string `json:"id"`
			Addr string `json:"addr"`
		} `json:"nodes"`
	}{}
	var addrs []string
	for _, id := range smokeIDs {
		addr := fmt.Sprintf("127.0.0.1:%d", smokePorts[id])
		cfg.Nodes = append(cfg.Nodes, struct {
			ID   string `json:"id"`
			Addr string `json:"addr"`
		}{ID: id, Addr: addr})
		addrs = append(addrs, addr)
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(workDir, "cluster.json")
	if err := os.WriteFile(cfgPath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	c := &smokeCluster{
		t:       t,
		bin:     bin,
		workDir: workDir,
		cfgPath: cfgPath,
		addrs:   addrs,
		procs:   make(map[string]*exec.Cmd),
		client:  &http.Client{Timeout: 3 * time.Second},
	}
	t.Cleanup(func() {
		for _, p := range c.procs {
			if p != nil && p.Process != nil {
				_ = p.Process.Kill()
			}
		}
	})
	return c
}

func (c *smokeCluster) dataDir(id string) string {
	return filepath.Join(c.workDir, "data-"+id)
}

// start 起一个节点，等它的 /debug/log 能响应才返回。
func (c *smokeCluster) start(id string) {
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
		resp, err := c.client.Get("http://" + fmt.Sprintf("127.0.0.1:%d", smokePorts[id]) + "/debug/log")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("节点 %s 10 秒内没起来", id)
}

// startAll 起三个节点，等出 leader。
func (c *smokeCluster) startAll() {
	c.t.Helper()
	for _, id := range smokeIDs {
		c.start(id)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, addr := range c.addrs {
			resp, err := c.client.Get("http://" + addr + "/debug/log")
			if err != nil {
				continue
			}
			var dbg struct {
				Role string `json:"role"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&dbg)
			resp.Body.Close()
			if dbg.Role == "leader" {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatal("10 秒内没有选出 leader")
}

type smokeResp struct {
	OK     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`
	Leader string `json:"leader,omitempty"`
	Value  string `json:"value"`
	Found  bool   `json:"found"`
}

// smokePut 发一次 Put（跟 leader 重定向），成功返回 nil。
func (c *smokeCluster) smokePut(key, value string) error {
	body, _ := json.Marshal(map[string]string{"key": key, "value": value})
	for _, addr := range c.addrs {
		resp, err := c.client.Post("http://"+addr+"/kv/put", "application/json", bytes.NewReader(body))
		if err != nil {
			continue
		}
		var r smokeResp
		_ = json.NewDecoder(resp.Body).Decode(&r)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK && r.OK {
			return nil
		}
		if r.Error == "not_leader" && r.Leader != "" {
			// 直接试 leader（下一轮循环会按顺序试到它；
			// 简单起见这里只多试一次 leader）。
			for _, a2 := range c.addrs {
				if a2 == leaderAddr(r.Leader) {
					resp2, err2 := c.client.Post("http://"+a2+"/kv/put", "application/json", bytes.NewReader(body))
					if err2 != nil {
						continue
					}
					var r2 smokeResp
					_ = json.NewDecoder(resp2.Body).Decode(&r2)
					resp2.Body.Close()
					if resp2.StatusCode == http.StatusOK && r2.OK {
						return nil
					}
				}
			}
		}
	}
	return fmt.Errorf("put %s 失败", key)
}

// smokeReadLinearizable 发一次线性化读（跟 leader 重定向）。
func (c *smokeCluster) smokeReadLinearizable(key string) (string, bool, error) {
	body, _ := json.Marshal(map[string]string{"key": key})
	for _, addr := range c.addrs {
		resp, err := c.client.Post("http://"+addr+"/kv/read", "application/json", bytes.NewReader(body))
		if err != nil {
			continue
		}
		var r smokeResp
		_ = json.NewDecoder(resp.Body).Decode(&r)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return r.Value, r.Found, nil
		}
		if r.Error == "not_leader" && r.Leader != "" {
			for _, a2 := range c.addrs {
				if a2 == leaderAddr(r.Leader) {
					resp2, err2 := c.client.Post("http://"+a2+"/kv/read", "application/json", bytes.NewReader(body))
					if err2 != nil {
						continue
					}
					var r2 smokeResp
					_ = json.NewDecoder(resp2.Body).Decode(&r2)
					resp2.Body.Close()
					if resp2.StatusCode == http.StatusOK {
						return r2.Value, r2.Found, nil
					}
				}
			}
		}
	}
	return "", false, fmt.Errorf("线性化读 %s 失败", key)
}

// leaderAddr 把 leader id 换成地址（测试的端口映射是固定的）。
func leaderAddr(leaderID string) string {
	return fmt.Sprintf("127.0.0.1:%d", smokePorts[leaderID])
}

// runSmokeClient 是一个真实集群客户端：按 ops 顺序发操作，
// 每个逻辑操作重试到成功为止（单个操作 10 秒上限），
// 返回该客户端的操作历史（Call/Return 是真实时间纳秒）。
// 注意：它在 goroutine 里跑，不能调 t.Fatalf——
// 失败时返回 error，由主 goroutine 汇总后再失败。
func runSmokeClient(c *smokeCluster, clientID int, ops []sim.ConcurrentOp) ([]porcupine.Operation, error) {
	history := make([]porcupine.Operation, 0, len(ops))
	for _, op := range ops {
		call := time.Now().UnixNano()
		var out interface{}
		var in interface{}
		var err error
		deadline := time.Now().Add(10 * time.Second)
		for {
			if op.Kind == "read" {
				var v string
				var found bool
				v, found, err = c.smokeReadLinearizable(op.Key)
				if err == nil {
					in = linearizability.ReadInput{Key: op.Key}
					out = linearizability.ReadOutput{Value: v, Found: found}
					break
				}
			} else {
				err = c.smokePut(op.Key, op.Value)
				if err == nil {
					in = linearizability.PutInput{Key: op.Key, Value: op.Value}
					out = linearizability.PutOutput{OK: true}
					break
				}
			}
			if time.Now().After(deadline) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		ret := time.Now().UnixNano()
		if err != nil {
			return history, fmt.Errorf("客户端 %d 的 %s %s 10 秒内最终失败：%w", clientID, op.Kind, op.Key, err)
		}
		history = append(history, porcupine.Operation{
			ClientId: clientID,
			Input:    in,
			Call:     call,
			Output:   out,
			Return:   ret,
		})
	}
	return history, nil
}

// TestLinearizability_RealCluster：真实三节点集群上，
// 3 个客户端并发发 200 次 Put/线性化读，Porcupine 验证线性一致。
// 数量比模拟器少（真实 HTTP + Raft 比虚拟时钟慢得多），
// 但覆盖的是同一条检查逻辑——证明真实集群也是线性一致的，
// 不只模拟器里是。
func TestLinearizability_RealCluster(t *testing.T) {
	c := newSmokeCluster(t)
	c.startAll()
	const total = 200
	seed := int64(7)
	perClient := genConcurrentOps(seed)
	// genConcurrentOps 按 1000 条生成，这里每客户端只取前 67 条左右。
	for i := range perClient {
		if len(perClient[i]) > total/linClients {
			perClient[i] = perClient[i][:total/linClients]
		}
	}
	var wg sync.WaitGroup
	histories := make([][]porcupine.Operation, linClients)
	errs := make([]error, linClients)
	for i := 0; i < linClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			histories[clientID], errs[clientID] = runSmokeClient(c, clientID, perClient[clientID])
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("客户端 %d 失败：%v", i, err)
		}
	}
	var history []porcupine.Operation
	for _, h := range histories {
		history = append(history, h...)
	}
	// 按 Call 排序，方便人读（检查器不依赖顺序，只看时间戳）。
	sort.Slice(history, func(i, j int) bool { return history[i].Call < history[j].Call })
	start := time.Now()
	ok, diag := linearizability.Check(history)
	t.Logf("真实集群：%d 条操作，检查耗时 %v", len(history), time.Since(start))
	if !ok {
		t.Fatalf("真实集群的历史不是线性一致的：\n%s", diag)
	}
}

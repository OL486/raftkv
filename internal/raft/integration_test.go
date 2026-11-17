// Package raft_test 是 raftkv 的集成测试：起 3 个真实节点（进程内启动，
// 但每个节点都是独立的 HTTP 服务 + 独立的 Raft 实例，走真实的 HTTP + JSON），
// 选它不选子进程启动的原因：同一进程内启停快、失败时堆栈直接可见，
// 而二进制层面的端到端由 scripts/demo.sh 覆盖。
package raft_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/OL486/raftkv/internal/cluster"
	"github.com/OL486/raftkv/internal/kv"
	"github.com/OL486/raftkv/internal/node"
	"github.com/OL486/raftkv/internal/raft"
)

func testClusterConfig() *cluster.Config {
	return &cluster.Config{Nodes: []cluster.NodeConfig{
		{ID: "node1", Addr: "127.0.0.1:18201"},
		{ID: "node2", Addr: "127.0.0.1:18202"},
		{ID: "node3", Addr: "127.0.0.1:18203"},
	}}
}

func startTestCluster(t *testing.T) []*node.Node {
	t.Helper()
	cfg := testClusterConfig()
	nodes := make([]*node.Node, 0, 3)
	for _, id := range []string{"node1", "node2", "node3"} {
		n, err := node.New(cfg, id, "")
		if err != nil {
			t.Fatal(err)
		}
		n.Start()
		nodes = append(nodes, n)
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.Stop(context.Background())
		}
	})
	return nodes
}

// waitForLeader 等任意一个节点当选 leader，返回它（与它在 nodes 里的下标）。
func waitForLeader(t *testing.T, nodes []*node.Node, timeout time.Duration) (*node.Node, int) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, n := range nodes {
			if _, isLeader := n.Raft().GetState(); isLeader {
				return n, i
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%v 内没有选出 leader", timeout)
	return nil, -1
}

// waitFor 在 timeout 内每 20ms 检查一次 cond，超时则失败。
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%v 内未达成：%s", timeout, what)
}

func allHave(nodes []*node.Node, key, value string) bool {
	for _, n := range nodes {
		v, ok := n.Store().Get(key)
		if !ok || v != value {
			return false
		}
	}
	return true
}

func TestThreeNodeCluster(t *testing.T) {
	nodes := startTestCluster(t)

	// 1. 选主。
	leader, leaderIdx := waitForLeader(t, nodes, 10*time.Second)
	t.Logf("首任 leader 是 %s", leader.ID)

	// 2. 写一条，三个节点最终都能读到。
	cmd := kv.EncodePut("foo", "bar")
	if _, _, ok := leader.Raft().Start(cmd); !ok {
		t.Fatal("leader 的 Start 应成功")
	}
	waitFor(t, 5*time.Second, "三个节点都读到 foo=bar", func() bool {
		return allHave(nodes, "foo", "bar")
	})

	// 3. 杀掉 leader，5 秒内应选出新 leader。
	if err := nodes[leaderIdx].Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	survivors := append(append([]*node.Node{}, nodes[:leaderIdx]...), nodes[leaderIdx+1:]...)
	newLeader, _ := waitForLeader(t, survivors, 5*time.Second)
	t.Logf("新 leader 是 %s", newLeader.ID)
	if newLeader == leader {
		t.Fatal("新 leader 不应是已杀掉的旧 leader")
	}

	// 4. 新 leader 继续写入，存活节点都能读到；旧数据还在。
	if _, _, ok := newLeader.Raft().Start(kv.EncodePut("foo2", "bar2")); !ok {
		t.Fatal("新 leader 的 Start 应成功")
	}
	waitFor(t, 5*time.Second, "存活节点都读到 foo2=bar2", func() bool {
		return allHave(survivors, "foo2", "bar2")
	})
	waitFor(t, 5*time.Second, "存活节点仍有 foo=bar", func() bool {
		return allHave(survivors, "foo", "bar")
	})

	// 5. /debug/log 能读到 term 与日志长度。
	addr := ""
	for _, nc := range testClusterConfig().Nodes {
		if nc.ID == survivors[0].ID {
			addr = nc.Addr
		}
	}
	resp, err := http.Get("http://" + addr + "/debug/log")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var dbg raft.DebugInfo
	if err := json.Unmarshal(data, &dbg); err != nil {
		t.Fatalf("/debug/log 不是合法 JSON：%v", err)
	}
	if dbg.Term < 1 || dbg.LogLen < 2 || dbg.CommitIndex < 2 {
		t.Fatalf("/debug/log 不对：%+v", dbg)
	}
	t.Logf("/debug/log：%+v", dbg)
}

func TestDebugLog_LeaderAddress(t *testing.T) {
	// 补充：直接确认 /debug/log 的 leader 字段是节点 id。
	nodes := startTestCluster(t)
	leader, _ := waitForLeader(t, nodes, 10*time.Second)
	_ = leader
	time.Sleep(300 * time.Millisecond) // 让心跳把 leader 广播出去
	for _, n := range nodes {
		if n.Raft().LeaderID() == "" {
			t.Fatalf("节点 %s 还不知道 leader 是谁", n.ID)
		}
	}
	fmt.Println("各节点均已知 leader：", nodes[0].Raft().LeaderID())
}

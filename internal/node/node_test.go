package node

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/OL486/raftkv/internal/cluster"
	"github.com/OL486/raftkv/internal/raft"
)

// TestSingleNode_HTTP 走真实 HTTP：单节点集群应自己选自己当 leader，
// /kv/put 与 /kv/get 全程走 HTTP。
func TestSingleNode_HTTP(t *testing.T) {
	cfg := &cluster.Config{Nodes: []cluster.NodeConfig{
		{ID: "node1", Addr: "127.0.0.1:18301"},
	}}
	n, err := New(cfg, "node1", "")
	if err != nil {
		t.Fatal(err)
	}
	n.Start()
	defer func() { _ = n.Stop(context.Background()) }()

	// 单节点：1 票即多数派，很快自己当选。
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, isLeader := n.raft.GetState(); isLeader {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("5 秒内单节点没有当选 leader")
		}
		time.Sleep(20 * time.Millisecond)
	}

	client := &http.Client{Timeout: 2 * time.Second}
	// POST /kv/put
	body, _ := json.Marshal(map[string]string{"key": "foo", "value": "bar"})
	resp, err := client.Post("http://127.0.0.1:18301/kv/put", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT 返回 %s", resp.Status)
	}
	// GET /kv/get
	resp, err = client.Get("http://127.0.0.1:18301/kv/get?key=foo")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var gr struct {
		Value string `json:"value"`
		Found bool   `json:"found"`
	}
	if err := json.Unmarshal(data, &gr); err != nil || !gr.Found || gr.Value != "bar" {
		t.Fatalf("GET foo = %+v %v", gr, err)
	}
	// GET /debug/log
	resp, err = client.Get("http://127.0.0.1:18301/debug/log")
	if err != nil {
		t.Fatal(err)
	}
	data, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	var dbg raft.DebugInfo
	if err := json.Unmarshal(data, &dbg); err != nil {
		t.Fatal(err)
	}
	if dbg.Term < 1 || dbg.Role != "leader" || dbg.LogLen < 1 || dbg.CommitIndex < 1 {
		t.Fatalf("/debug/log 不对：%+v", dbg)
	}
}

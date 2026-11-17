package cluster

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cluster.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoad(t *testing.T) {
	path := writeTempConfig(t, `{"nodes":[
		{"id":"node1","addr":"127.0.0.1:8001"},
		{"id":"node2","addr":"127.0.0.1:8002"},
		{"id":"node3","addr":"127.0.0.1:8003"}
	]}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Nodes) != 3 {
		t.Fatalf("节点数 = %d, want 3", len(cfg.Nodes))
	}
	if ids := cfg.IDs(); len(ids) != 3 || ids[0] != "node1" || ids[2] != "node3" {
		t.Fatalf("IDs = %v", ids)
	}
	if addrs := cfg.Addrs(); addrs["node2"] != "127.0.0.1:8002" {
		t.Fatalf("Addrs = %v", addrs)
	}
	if n, ok := cfg.Get("node3"); !ok || n.Addr != "127.0.0.1:8003" {
		t.Fatalf("Get(node3) = %+v %v", n, ok)
	}
	if _, ok := cfg.Get("nodeX"); ok {
		t.Fatal("不存在的节点应返回 false")
	}
	if peers := cfg.Peers("node1"); len(peers) != 2 || peers[0].ID != "node2" {
		t.Fatalf("Peers(node1) = %+v", peers)
	}
}

func TestLoad_Errors(t *testing.T) {
	cases := []struct {
		name    string
		content string
	}{
		{"非法 JSON", `{bad`},
		{"没有节点", `{"nodes":[]}`},
		{"id 为空", `{"nodes":[{"id":"","addr":"127.0.0.1:8001"}]}`},
		{"addr 为空", `{"nodes":[{"id":"node1","addr":""}]}`},
		{"id 重复", `{"nodes":[{"id":"n","addr":"a"},{"id":"n","addr":"b"}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Load(writeTempConfig(t, tc.content)); err == nil {
				t.Fatalf("%s：应报错", tc.name)
			}
		})
	}
	t.Run("文件不存在", func(t *testing.T) {
		if _, err := Load(filepath.Join(t.TempDir(), "nope.json")); err == nil {
			t.Fatal("不存在的文件应报错")
		}
	})
}

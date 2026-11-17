// Package cluster 解析静态集群配置文件 cluster.json。
// M1 不支持动态加节点：改配置要改文件、重启集群（见规划不做清单）。
package cluster

import (
	"encoding/json"
	"fmt"
	"os"
)

// NodeConfig 是一个节点的 id 与监听地址。
type NodeConfig struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// Config 是一份 cluster.json 的内容。
type Config struct {
	Nodes []NodeConfig `json:"nodes"`
}

// Load 从 path 读一份集群配置。节点数少于 1 时报错。
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读集群配置失败：%w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("集群配置不是合法 JSON：%w", err)
	}
	if len(cfg.Nodes) == 0 {
		return nil, fmt.Errorf("集群配置里没有节点")
	}
	seen := make(map[string]bool, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		if n.ID == "" || n.Addr == "" {
			return nil, fmt.Errorf("集群配置里有 id 或 addr 为空的节点")
		}
		if seen[n.ID] {
			return nil, fmt.Errorf("集群配置里节点 id %q 重复", n.ID)
		}
		seen[n.ID] = true
	}
	return &cfg, nil
}

// Get 按 id 找节点配置。
func (c *Config) Get(id string) (NodeConfig, bool) {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return NodeConfig{}, false
}

// IDs 返回全部节点 id（顺序与配置文件一致）。
func (c *Config) IDs() []string {
	ids := make([]string, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		ids = append(ids, n.ID)
	}
	return ids
}

// Addrs 返回 id -> addr 的映射（Raft 拿它找对端地址）。
func (c *Config) Addrs() map[string]string {
	m := make(map[string]string, len(c.Nodes))
	for _, n := range c.Nodes {
		m[n.ID] = n.Addr
	}
	return m
}

// Peers 返回除 id 之外的全部节点（拉票与复制的目标）。
func (c *Config) Peers(id string) []NodeConfig {
	var out []NodeConfig
	for _, n := range c.Nodes {
		if n.ID != id {
			out = append(out, n)
		}
	}
	return out
}

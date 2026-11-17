// Command raftkv-cli：读写 raftkv 集群的客户端。
//
// 用法：
//
//	raftkv-cli --config cluster.json put foo bar   # 写
//	raftkv-cli --config cluster.json get foo        # 读（可能读旧）
//	raftkv-cli --config cluster.json get --linearizable foo  # 线性化读
//
// 客户端先随便连一个节点，收到 NotLeader 就按提示重定向到 leader；
// leader 挂了就重试下一个节点，直到超时报错。
// 注意 get 读的是所连节点的本地状态机，读 follower 可能读到旧数据；
// 要线性化的最新值用 get --linearizable（走 Raft 日志的读命令）。
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/OL486/raftkv/internal/cluster"
)

// 客户端整体超时：超过这个时间还没写成功就报错退出。
const clientTimeout = 5 * time.Second

func main() {
	configPath := flag.String("config", "cluster.json", "集群配置文件路径")
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		usage()
		os.Exit(1)
	}

	cfg, err := cluster.Load(*configPath)
	if err != nil {
		fatal(err)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(clientTimeout)

	switch args[0] {
	case "put":
		if len(args) != 3 {
			fmt.Fprintln(os.Stderr, "用法：raftkv-cli put <key> <value>")
			os.Exit(1)
		}
		if err := doPut(client, cfg, args[1], args[2], deadline); err != nil {
			fatal(err)
		}
		fmt.Println("OK")
	case "get":
		// get [--linearizable] <key>：--linearizable 走 /kv/read（日志读命令）。
		linearizable := false
		var rest []string
		for _, a := range args[1:] {
			if a == "--linearizable" {
				linearizable = true
			} else {
				rest = append(rest, a)
			}
		}
		if len(rest) != 1 {
			fmt.Fprintln(os.Stderr, "用法：raftkv-cli get [--linearizable] <key>")
			os.Exit(1)
		}
		var value string
		var err error
		if linearizable {
			value, err = doGetLinearizable(client, cfg, rest[0], deadline)
		} else {
			value, err = doGet(client, cfg, rest[0], deadline)
		}
		if err != nil {
			fatal(err)
		}
		fmt.Println(value)
	default:
		fmt.Fprintf(os.Stderr, "未知命令 %q\n", args[0])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "用法：")
	fmt.Fprintln(os.Stderr, "  raftkv-cli --config cluster.json put <key> <value>")
	fmt.Fprintln(os.Stderr, "  raftkv-cli --config cluster.json get [--linearizable] <key>")
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "raftkv-cli：", err)
	os.Exit(1)
}

type putResponse struct {
	OK     bool   `json:"ok"`
	Index  int    `json:"index,omitempty"`
	Error  string `json:"error,omitempty"`
	Leader string `json:"leader,omitempty"`
}

type getResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Found bool   `json:"found"`
}

// doPut 向集群写一个 key。按顺序试节点，收到 NotLeader 就跳去 leader，
// 节点不通就试下一个，直到成功或超时。
func doPut(client *http.Client, cfg *cluster.Config, key, value string, deadline time.Time) error {
	nodes := cfg.Nodes
	next := 0 // 下一个要试的节点下标
	for time.Now().Before(deadline) {
		n := nodes[next%len(nodes)]
		next++
		body, _ := json.Marshal(map[string]string{"key": key, "value": value})
		resp, err := client.Post("http://"+n.Addr+"/kv/put", "application/json", bytes.NewReader(body))
		if err != nil {
			continue // 节点不通，试下一个
		}
		var pr putResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&pr)
		resp.Body.Close()
		if decodeErr != nil {
			continue
		}
		if resp.StatusCode == http.StatusOK && pr.OK {
			return nil
		}
		if pr.Error == "not_leader" && pr.Leader != "" {
			// 按提示重定向到 leader。
			for i, cand := range nodes {
				if cand.ID == pr.Leader {
					next = i
					break
				}
			}
			continue
		}
		// 超时或其他错误：等一小会儿再试下一个节点。
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("put %s 超时：集群里没有可用的 leader", key)
}

// doGet 从集群读一个 key。按顺序试节点，第一个返回 found 的为准；
// 所有节点都说没有，才算真的没有（单个 follower 可能还没追上日志）。
func doGet(client *http.Client, cfg *cluster.Config, key string, deadline time.Time) (string, error) {
	nodes := cfg.Nodes
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			resp, err := client.Get("http://" + n.Addr + "/kv/get?key=" + key)
			if err != nil {
				continue
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				continue
			}
			var gr getResponse
			if err := json.Unmarshal(body, &gr); err != nil {
				continue
			}
			if gr.Found {
				return gr.Value, nil
			}
		}
		// 这一轮都没找到：可能是 follower 还没追上，等一小会儿再试。
		time.Sleep(100 * time.Millisecond)
	}
	return "", fmt.Errorf("key %q 不存在（或集群不可用）", key)
}

// readResponse 是 /kv/read 的回复形状：成功时是 key/value/found，
// 不是 leader 时是 error=not_leader + leader（与 putResponse 同款）。
type readResponse struct {
	Key    string `json:"key"`
	Value  string `json:"value"`
	Found  bool   `json:"found"`
	Error  string `json:"error,omitempty"`
	Leader string `json:"leader,omitempty"`
}

// doGetLinearizable 是线性化读：POST /kv/read，走 Raft 日志的读命令，
// 返回读命令在日志里应用那一刻的值。不是 leader 时按 NotLeader
// 重定向（与 doPut 同款）；key 不存在时返回错误。
func doGetLinearizable(client *http.Client, cfg *cluster.Config, key string, deadline time.Time) (string, error) {
	nodes := cfg.Nodes
	next := 0
	for time.Now().Before(deadline) {
		n := nodes[next%len(nodes)]
		next++
		body, _ := json.Marshal(map[string]string{"key": key})
		resp, err := client.Post("http://"+n.Addr+"/kv/read", "application/json", bytes.NewReader(body))
		if err != nil {
			continue // 节点不通，试下一个
		}
		var rr readResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&rr)
		resp.Body.Close()
		if decodeErr != nil {
			continue
		}
		if resp.StatusCode == http.StatusOK {
			if !rr.Found {
				return "", fmt.Errorf("key %q 不存在", key)
			}
			return rr.Value, nil
		}
		if rr.Error == "not_leader" && rr.Leader != "" {
			// 按提示重定向到 leader。
			for i, cand := range nodes {
				if cand.ID == rr.Leader {
					next = i
					break
				}
			}
			continue
		}
		// 超时或其他错误：等一小会儿再试下一个节点。
		time.Sleep(100 * time.Millisecond)
	}
	return "", fmt.Errorf("线性化读 key %q 超时：集群里没有可用的 leader", key)
}

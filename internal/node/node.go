// Package node 把 Raft、KV 状态机与 HTTP 服务拼成一个完整节点。
// cmd/server 只是读配置、调 New、调 Start；集成测试也直接用这个包，
// 测的就是真实走 HTTP 的那条路。
package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/OL486/raftkv/internal/cluster"
	"github.com/OL486/raftkv/internal/kv"
	"github.com/OL486/raftkv/internal/raft"
)

// putWaitTimeout 是 /kv/put 等一条日志提交的最长等待时间。
// 超时不代表写入丢了——它可能稍后才提交，客户端应重试或换节点查。
const putWaitTimeout = 3 * time.Second

// Node 是一个 Raft 节点：Raft 实例 + KV 状态机 + HTTP 服务。
type Node struct {
	ID    string
	raft  *raft.Raft
	store *kv.Store
	srv   *http.Server

	mu      sync.Mutex
	waiters map[int]chan struct{} // 等某条日志被应用的 waiter（下标 -> 通知 channel）

	applyCh  chan raft.ApplyMsg
	done     chan struct{}
	doneOnce sync.Once
}

// New 按配置建一个节点（不启动）。dataDir 是持久化目录：
//   - 非空：日志与元数据落到该目录（FilePersister），重启可恢复；
//   - 为空：用内存实现（仅测试用，进程退出即丢数据）。
//
// 建节点时会把持久状态恢复出来，并把 1..lastApplied 的已提交日志
// 按序重放到 KV 状态机（只到 lastApplied——之后的条目等新 leader
// 的心跳带 LeaderCommit 过来后，再由 apply 循环正常应用）。
func New(cfg *cluster.Config, id string, dataDir string) (*Node, error) {
	self, ok := cfg.Get(id)
	if !ok {
		return nil, fmt.Errorf("集群配置里没有节点 %q", id)
	}

	var persister raft.Persister
	if dataDir == "" {
		persister = &raft.MemoryPersister{}
	} else {
		fp, err := raft.NewFilePersister(dataDir)
		if err != nil {
			return nil, err
		}
		persister = fp
	}

	applyCh := make(chan raft.ApplyMsg, 100)
	r, err := raft.NewRaft(id, cfg.IDs(), cfg.Addrs(), raft.NewHTTPTransport(), persister, applyCh)
	if err != nil {
		return nil, err
	}
	n := &Node{
		ID:      id,
		raft:    r,
		store:   kv.NewStore(),
		waiters: make(map[int]chan struct{}),
		applyCh: applyCh,
		done:    make(chan struct{}),
	}
	// 启动恢复：重放已应用的日志。重放失败说明磁盘上的命令已损坏，
	// 拒绝启动比带着错数据跑要安全。
	logEntries, lastApplied := r.RestoreInfo()
	if lastApplied > len(logEntries)-1 {
		return nil, fmt.Errorf("恢复的状态不一致：lastApplied=%d，但日志只有 %d 条用户条目", lastApplied, len(logEntries)-1)
	}
	for i := 1; i <= lastApplied; i++ {
		if err := n.store.Apply(logEntries[i].Command); err != nil {
			return nil, fmt.Errorf("重放日志 %d 失败：%w", i, err)
		}
	}
	if lastApplied > 0 {
		log.Printf("节点 %s 从磁盘恢复：重放了 %d 条已提交日志", id, lastApplied)
	}
	mux := http.NewServeMux()
	raft.RegisterRaftHandlers(mux, r)
	mux.HandleFunc("/kv/put", n.handlePut)
	mux.HandleFunc("/kv/get", n.handleGet)
	mux.HandleFunc("/debug/log", n.handleDebugLog)
	n.srv = &http.Server{Addr: self.Addr, Handler: mux}
	return n, nil
}

// Start 启动 Raft（选举计时、apply 循环）与 HTTP 服务（后台 goroutine）。
func (n *Node) Start() {
	go n.consumeApply()
	n.raft.Run()
	go func() {
		if err := n.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("节点 %s 的 HTTP 服务退出：%v", n.ID, err)
		}
	}()
}

// Stop 停掉 Raft 的后台 goroutine 与 HTTP 服务。多次调用是安全的。
func (n *Node) Stop(ctx context.Context) error {
	n.doneOnce.Do(func() { close(n.done) })
	n.raft.Stop()
	return n.srv.Shutdown(ctx)
}

// Raft 返回底层的 Raft 实例（测试与调试用）。
func (n *Node) Raft() *raft.Raft { return n.raft }

// Store 返回 KV 状态机（测试用）。
func (n *Node) Store() *kv.Store { return n.store }

// consumeApply 是 apply 循环的消费端：把已提交命令按序交给状态机，
// 再通知等这条日志的 /kv/put。
func (n *Node) consumeApply() {
	for {
		select {
		case msg := <-n.applyCh:
			// 状态机出错只记日志：日志里的命令都是合法的，
			// 出错即是 bug，不应让整个节点停下。
			if err := n.store.Apply(msg.Command); err != nil {
				log.Printf("节点 %s 应用日志 %d 失败：%v", n.ID, msg.Index, err)
			}
			n.mu.Lock()
			if ch, ok := n.waiters[msg.Index]; ok {
				close(ch)
				delete(n.waiters, msg.Index)
			}
			n.mu.Unlock()
		case <-n.done:
			return
		}
	}
}

// waitApplied 等下标 index 的日志被应用（或超时）。
// 注册 waiter 与“已经应用过了”的检查在同一把锁里做，不会漏通知。
func (n *Node) waitApplied(index int, timeout time.Duration) bool {
	ch := make(chan struct{})
	n.mu.Lock()
	if n.raft.LastApplied() >= index {
		n.mu.Unlock()
		return true
	}
	n.waiters[index] = ch
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.waiters, index)
		n.mu.Unlock()
	}()
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	}
}

// ---- HTTP handler ----

type putRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type putResponse struct {
	OK    bool   `json:"ok"`
	Index int    `json:"index,omitempty"`
	Error string `json:"error,omitempty"`
	// 不是 leader 时告诉客户端真正的 leader 是谁，客户端照此重定向。
	Leader string `json:"leader,omitempty"`
}

// handlePut 接客户端写入：leader 把命令追加进日志、等提交后返回成功；
// 不是 leader 时返回 503 + NotLeader（含 LeaderID）。
func (n *Node) handlePut(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "只接受 POST", http.StatusMethodNotAllowed)
		return
	}
	var pr putRequest
	if err := json.NewDecoder(req.Body).Decode(&pr); err != nil {
		http.Error(w, "请求体不是合法 JSON："+err.Error(), http.StatusBadRequest)
		return
	}
	if pr.Key == "" {
		http.Error(w, "key 不能为空", http.StatusBadRequest)
		return
	}
	index, _, isLeader := n.raft.Start(kv.EncodePut(pr.Key, pr.Value))
	if !isLeader {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(putResponse{Error: "not_leader", Leader: n.raft.LeaderID()})
		return
	}
	if !n.waitApplied(index, putWaitTimeout) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGatewayTimeout)
		_ = json.NewEncoder(w).Encode(putResponse{Error: "timeout"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(putResponse{OK: true, Index: index})
}

// handleGet 读本地状态机。注意：读到的数据可能是旧的
// （本节点 lastApplied 落后于 leader 的 commitIndex），见 kv.Store.Get。
func (n *Node) handleGet(w http.ResponseWriter, req *http.Request) {
	key := req.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "缺少 key 参数", http.StatusBadRequest)
		return
	}
	value, found := n.store.Get(key)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Key   string `json:"key"`
		Value string `json:"value"`
		Found bool   `json:"found"`
	}{Key: key, Value: value, Found: found})
}

// handleDebugLog 打印当前 term、角色、日志长度、commitIndex、lastApplied。
func (n *Node) handleDebugLog(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(n.raft.Debug())
}

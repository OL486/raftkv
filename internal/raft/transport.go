package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Transport 是 Raft 节点之间发 RPC 的抽象。RPC 是异步的：
// 发出去就返回，回复到了调 cb（可能在另一个 goroutine 里，
// 也可能——模拟器里——在虚拟时钟的事件循环里同步调）。
// cb 收到回复后必须自己判断“这次回复还作不作数”
// （比如任期已经变了），因为慢回复可能在重试之后才到。
// 消息丢了时 cb 永远不会被调用（与真实网络的超时等价，
// 只是没有错误值）：调用方靠下一轮心跳 / 选举重试兜底，
// 不要在 cb 之外等回复。
type Transport interface {
	RequestVote(addr string, args *RequestVoteArgs, cb func(*RequestVoteReply, error))
	AppendEntries(addr string, args *AppendEntriesArgs, cb func(*AppendEntriesReply, error))
	InstallSnapshot(addr string, args *InstallSnapshotArgs, cb func(*InstallSnapshotReply, error))
}

// HTTPTransport 用 HTTP + JSON 发 RPC（选它不选 gRPC 的理由：
// curl 就能看，调试成本最低；性能不是本项目目标）。
// addr 是 "host:port" 形式。
type HTTPTransport struct {
	client *http.Client
	// snapshotClient 是 InstallSnapshot 专用的 client：快照数据
	// 可能有好几 MB（10 万条 KV 的全量），150ms 的 RPC 超时
	// 不够一次传完——它不是选举路径上的消息，超时放宽到 10 秒。
	// 超时了 leader 下一轮心跳会重试，不影响正确性。
	snapshotClient *http.Client
}

// NewHTTPTransport 建一个 HTTP 传输。单次 RPC 超时 150ms：
// 选举超时最短 150ms，RPC 不能比它还慢，否则一次选举里发不完票。
func NewHTTPTransport() *HTTPTransport {
	return &HTTPTransport{
		client:         &http.Client{Timeout: 150 * time.Millisecond},
		snapshotClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// RequestVote 发投票请求。回复到了调 cb（在新起的 goroutine 里）。
func (t *HTTPTransport) RequestVote(addr string, args *RequestVoteArgs, cb func(*RequestVoteReply, error)) {
	go func() {
		reply, err := postJSON[RequestVoteArgs, RequestVoteReply](t.client, "http://"+addr+"/raft/requestVote", *args)
		cb(reply, err)
	}()
}

// AppendEntries 发日志复制/心跳请求。回复到了调 cb（在新起的 goroutine 里）。
func (t *HTTPTransport) AppendEntries(addr string, args *AppendEntriesArgs, cb func(*AppendEntriesReply, error)) {
	go func() {
		reply, err := postJSON[AppendEntriesArgs, AppendEntriesReply](t.client, "http://"+addr+"/raft/appendEntries", *args)
		cb(reply, err)
	}()
}

// InstallSnapshot 发快照安装请求。回复到了调 cb（在新起的 goroutine 里）。
// 快照数据可能很大，用 snapshotClient（10 秒超时），不用 150ms 的 client。
func (t *HTTPTransport) InstallSnapshot(addr string, args *InstallSnapshotArgs, cb func(*InstallSnapshotReply, error)) {
	go func() {
		reply, err := postJSON[InstallSnapshotArgs, InstallSnapshotReply](t.snapshotClient, "http://"+addr+"/raft/installSnapshot", *args)
		cb(reply, err)
	}()
}

// postJSON 把 req 编成 JSON POST 过去，再把响应体解成 Resp。
func postJSON[Req any, Resp any](client *http.Client, url string, req Req) (*Resp, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(req); err != nil {
		return nil, fmt.Errorf("编码 RPC 请求失败：%w", err)
	}
	resp, err := client.Post(url, "application/json", &buf)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("RPC %s 返回 %s", url, resp.Status)
	}
	var out Resp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("解码 RPC 响应失败：%w", err)
	}
	return &out, nil
}

// RegisterRaftHandlers 把三个 Raft endpoint 挂到 mux 上。
// handler 只做“收包、调 Raft、回包”，不直接碰状态机；
// term 校验在 Raft.Handle* 里做（比自己 term 小的请求直接拒绝并返回自己的 term）。
func RegisterRaftHandlers(mux *http.ServeMux, r *Raft) {
	mux.HandleFunc("/raft/requestVote", func(w http.ResponseWriter, req *http.Request) {
		var args RequestVoteArgs
		if err := json.NewDecoder(req.Body).Decode(&args); err != nil {
			http.Error(w, "请求体不是合法 JSON："+err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, r.HandleRequestVote(&args))
	})
	mux.HandleFunc("/raft/appendEntries", func(w http.ResponseWriter, req *http.Request) {
		var args AppendEntriesArgs
		if err := json.NewDecoder(req.Body).Decode(&args); err != nil {
			http.Error(w, "请求体不是合法 JSON："+err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, r.HandleAppendEntries(&args))
	})
	mux.HandleFunc("/raft/installSnapshot", func(w http.ResponseWriter, req *http.Request) {
		var args InstallSnapshotArgs
		if err := json.NewDecoder(req.Body).Decode(&args); err != nil {
			http.Error(w, "请求体不是合法 JSON："+err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, r.HandleInstallSnapshot(&args))
	})
}

// writeJSON 把 v 编成 JSON 写回（Content-Type 一并设好）。
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

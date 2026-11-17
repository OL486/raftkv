package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Transport 是 Raft 节点之间发 RPC 的抽象。M1 只有 HTTP + JSON 一种实现；
// 模拟器（M3）会换成进程内 channel 实现，届时 raft.go 不动。
type Transport interface {
	RequestVote(addr string, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(addr string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// HTTPTransport 用 HTTP + JSON 发 RPC（选它不选 gRPC 的理由：
// curl 就能看，调试成本最低；性能不是本项目目标）。
// addr 是 "host:port" 形式。
type HTTPTransport struct {
	client *http.Client
}

// NewHTTPTransport 建一个 HTTP 传输。单次 RPC 超时 150ms：
// 选举超时最短 150ms，RPC 不能比它还慢，否则一次选举里发不完票。
func NewHTTPTransport() *HTTPTransport {
	return &HTTPTransport{client: &http.Client{Timeout: 150 * time.Millisecond}}
}

// RequestVote 发投票请求。
func (t *HTTPTransport) RequestVote(addr string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return postJSON[RequestVoteArgs, RequestVoteReply](t.client, "http://"+addr+"/raft/requestVote", *args)
}

// AppendEntries 发日志复制/心跳请求。
func (t *HTTPTransport) AppendEntries(addr string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return postJSON[AppendEntriesArgs, AppendEntriesReply](t.client, "http://"+addr+"/raft/appendEntries", *args)
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

// RegisterRaftHandlers 把两个 Raft endpoint 挂到 mux 上。
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
}

// writeJSON 把 v 编成 JSON 写回（Content-Type 一并设好）。
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

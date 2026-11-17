// Package kv 是 Raft 之上的 KV 状态机：纯函数式逻辑，不碰网络、不碰磁盘，
// 方便在模拟器里单测（M3）。命令经 Raft 日志提交后按序 Apply。
package kv

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Command 是写进 Raft 日志的命令。Op 有 "put"、"delete" 与 "read"；
// "get" 是只读操作，不进日志（见 Store.Get 的注释）。
// "read" 是走日志的线性化读：状态机应用到它时原样返回当前值
// （应用本身是 no-op，不改数据），线性化点就是它在日志里
// 提交/应用的位置；M4 的线性一致性检查只用这条路径读。
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// EncodePut 把一次 Put 编成日志命令。
func EncodePut(key, value string) []byte {
	b, _ := json.Marshal(Command{Op: "put", Key: key, Value: value})
	return b
}

// EncodeDelete 把一次 Delete 编成日志命令。
func EncodeDelete(key string) []byte {
	b, _ := json.Marshal(Command{Op: "delete", Key: key})
	return b
}

// EncodeRead 把一次线性化读编成日志命令。
func EncodeRead(key string) []byte {
	b, _ := json.Marshal(Command{Op: "read", Key: key})
	return b
}

// ApplyResult 是 Apply 返回的应用结果（JSON）：
// put/delete 返回 {"ok":true}；read 返回 {"value": "...", "found": true/false}。
type ApplyResult struct {
	OK    bool   `json:"ok,omitempty"`
	Value string `json:"value,omitempty"`
	Found bool   `json:"found,omitempty"`
}

// DecodeApplyResult 把 Apply 的返回值解回来。
func DecodeApplyResult(b []byte) (ApplyResult, error) {
	var r ApplyResult
	if err := json.Unmarshal(b, &r); err != nil {
		return r, fmt.Errorf("应用结果不是合法 JSON：%w", err)
	}
	return r, nil
}

// DecodeCommand 把日志命令解回来。
func DecodeCommand(b []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(b, &c); err != nil {
		return c, fmt.Errorf("命令不是合法 JSON：%w", err)
	}
	return c, nil
}

// Store 是 KV 状态机：一个带锁的 map。
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewStore 建一个空状态机。
func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}

// Apply 执行一条已提交的日志命令（Raft 的 apply 循环按序调用），
// 返回应用结果（JSON，见 ApplyResult）。
// "read" 是 no-op：只返回应用那一刻的值，不改数据——
// 调用方靠这个返回值拿到读的线性化点处的值；
// 直接读 Store.Get 可能读到更晚的值（之后又有 Put 应用了），
// 那就不是这条读命令的线性化结果了。
// 未知 Op 返回错误——日志里不应出现这种命令，出现即是 bug。
func (s *Store) Apply(cmd []byte) ([]byte, error) {
	c, err := DecodeCommand(cmd)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch c.Op {
	case "put":
		s.data[c.Key] = c.Value
		b, _ := json.Marshal(ApplyResult{OK: true})
		return b, nil
	case "delete":
		delete(s.data, c.Key)
		b, _ := json.Marshal(ApplyResult{OK: true})
		return b, nil
	case "read":
		v, ok := s.data[c.Key]
		b, _ := json.Marshal(ApplyResult{Value: v, Found: ok})
		return b, nil
	default:
		return nil, fmt.Errorf("未知命令 op=%q", c.Op)
	}
}

// Get 读本地状态机。不进日志，所以读 follower 是允许的，
// 但读到的数据可能是旧的（该节点的 lastApplied 落后于 leader 的 commitIndex）；
// 要读线性化的最新值，走日志的读命令（EncodeRead + /kv/read，见 M4），
// 而不是这里——ReadIndex 读优化明确不做，见规划。
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Len 返回当前 key 的个数（测试与调试用）。
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// All 返回全量 key/value 的一份拷贝（模拟器比较各节点状态机用）。
// 返回的是拷贝，调用方改了也不会影响状态机。
func (s *Store) All() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

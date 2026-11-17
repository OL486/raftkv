// Package kv 是 Raft 之上的 KV 状态机：纯函数式逻辑，不碰网络、不碰磁盘，
// 方便在模拟器里单测（M3）。命令经 Raft 日志提交后按序 Apply。
package kv

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Command 是写进 Raft 日志的命令。Op 目前只有 "put" 与 "delete"；
// "get" 是只读操作，不进日志（见 Store.Get 的注释）。
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

// Apply 执行一条已提交的日志命令（Raft 的 apply 循环按序调用）。
// 未知 Op 返回错误——日志里不应出现这种命令，出现即是 bug。
func (s *Store) Apply(cmd []byte) error {
	c, err := DecodeCommand(cmd)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch c.Op {
	case "put":
		s.data[c.Key] = c.Value
	case "delete":
		delete(s.data, c.Key)
	default:
		return fmt.Errorf("未知命令 op=%q", c.Op)
	}
	return nil
}

// Get 读本地状态机。不进日志，所以读 follower 是允许的，
// 但读到的数据可能是旧的（该节点的 lastApplied 落后于 leader 的 commitIndex）；
// 要读最新数据必须走日志（ReadIndex 读优化，明确不做，见规划）。
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

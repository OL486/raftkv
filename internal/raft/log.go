// Package raft 实现 Raft 共识算法（论文图 2）。
package raft

import "sync"

// LogEntry 是 Raft 日志里的一条记录。Term 是写入这条记录时 leader 的任期，
// Command 是序列化后的状态机命令（见 internal/kv.Command 的 JSON）。
type LogEntry struct {
	Term    int    `json:"term"`
	Command []byte `json:"command"`
}

// PersistentState 是 Raft 论文要求“在回复 RPC 之前必须落盘”的全部状态，
// 再加一个 LastApplied：已应用到状态机的最大日志下标。
// 重启时只把 1..LastApplied 的日志重放到状态机——LastApplied 永远 ≤
// commitIndex，所以重放的都是已提交的条目，绝不会把未提交的尾部日志
// 喂给状态机；LastApplied 之后的已提交条目等新 leader 的心跳带
// LeaderCommit 过来后再由 apply 循环正常应用。
// M2 用文件实现（FilePersister，见 persist_file.go）；内存实现只留给测试。
type PersistentState struct {
	CurrentTerm int        `json:"currentTerm"`
	VotedFor    string     `json:"votedFor"`
	Log         []LogEntry `json:"log"`
	LastApplied int        `json:"lastApplied"`
}

// Persister 是持久状态的存储抽象。Raft 核心只调这两个方法，
// 不关心状态存在内存还是磁盘。
type Persister interface {
	// Save 原子地保存一份持久状态。调用方保证 Save 返回前状态已稳固
	// （文件实现里要写文件 + fsync；内存实现只是复制一份）。
	Save(PersistentState) error
	// Load 读回最近一次 Save 的状态。从未 Save 过时返回零值与 nil 错误。
	Load() (PersistentState, error)
}

// MemoryPersister 是 Persister 的内存实现，仅供测试使用：
// 进程退出即丢数据；生产路径（cmd/server）用 FilePersister。
type MemoryPersister struct {
	mu    sync.Mutex
	state PersistentState
	saved bool
}

// Save 把状态复制一份存在内存里。
func (m *MemoryPersister) Save(s PersistentState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = PersistentState{
		CurrentTerm: s.CurrentTerm,
		VotedFor:    s.VotedFor,
		Log:         append([]LogEntry(nil), s.Log...),
		LastApplied: s.LastApplied,
	}
	m.saved = true
	return nil
}

// Load 读回最近一次 Save 的状态（同样复制一份，防调用方改写内部数据）。
func (m *MemoryPersister) Load() (PersistentState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.saved {
		return PersistentState{}, nil
	}
	return PersistentState{
		CurrentTerm: m.state.CurrentTerm,
		VotedFor:    m.state.VotedFor,
		Log:         append([]LogEntry(nil), m.state.Log...),
		LastApplied: m.state.LastApplied,
	}, nil
}

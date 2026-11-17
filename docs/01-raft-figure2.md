# Raft 论文图 2 对照表

本文档逐项对照 Raft 论文（Diego Ongaro & John Ousterhout, "In Search of an
Understandable Consensus Algorithm"）图 2 的规则，说明 raftkv 的实现状态。
M1 实现的部分打 ✓，M2 落盘后持久状态打 ✓（文件版），没做的标出去向（M3–M5）。

## 状态

| 图 2 条目 | 实现 | 状态 |
|---|---|---|
| 持久状态 `currentTerm` | `raft.go` 的 `currentTerm`，每次改动后 `persistLocked()`；M2 起落盘到 `--data/meta`（`persist_file.go`） | ✓（M2 文件版） |
| 持久状态 `votedFor` | 同上；投票前必须先落盘（见 `docs/02-persistence.md`） | ✓（M2 文件版） |
| 持久状态 `log[]`（每条 `{Term, Command}`） | `raft.go` 的 `log`，`log[0]` 是哨兵条目（Term 0），下标从 1 开始；M2 起落盘到 `--data/raft.log`（追加写 + fsync） | ✓（M2 文件版） |
| 易失状态 `commitIndex`、`lastApplied` | `raft.go` 的同名字段；`lastApplied` 额外落盘进 `meta`，用于启动重放划清边界（重启时 `commitIndex` 恢复为 `lastApplied`） | ✓ |
| Leader 易失状态 `nextIndex[]`、`matchIndex[]` | `raft.go` 的同名 map，当选 leader 时初始化 | ✓ |

## RequestVote RPC

| 图 2 条目 | 实现 | 状态 |
|---|---|---|
| 参数 `Term, CandidateID, LastLogIndex, LastLogTerm` | `RequestVoteArgs`（`raft.go`） | ✓ |
| 返回 `Term, VoteGranted` | `RequestVoteReply` | ✓ |
| 接收方：`Term < currentTerm` 时拒绝 | `HandleRequestVote` 开头即判断 | ✓ |
| 接收方：`Term > currentTerm` 时更新任期、转 follower | `stepDownLocked`（任期前进、票清空、重置选举计时） | ✓ |
| 接收方：本任期没投过票（或投的就是此人），且候选人日志不比自己旧，就投票 | 先比 `LastLogTerm` 再比 `LastLogIndex`；投票后重置选举计时 | ✓ |
| 候选人：任期 +1、投自己、并行发 RequestVote、拿到多数票转 leader | `startElection` + `becomeLeaderLocked` | ✓ |
| 候选人：收到合法 leader 的 AppendEntries 就退回 follower | `HandleAppendEntries` 里同任期也退回 follower（`votedFor` 不动，见 `raft.go` 注释） | ✓ |
| 候选人：选举超时就开始新一轮选举 | 选举计时 goroutine 超时后再次调 `startElection` | ✓ |

## AppendEntries RPC

| 图 2 条目 | 实现 | 状态 |
|---|---|---|
| 参数 `Term, LeaderID, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit` | `AppendEntriesArgs`；`Entries` 为空即心跳 | ✓ |
| 返回 `Term, Success` | `AppendEntriesReply` | ✓ |
| 接收方：`Term < currentTerm` 时拒绝 | `HandleAppendEntries` 开头即判断 | ✓ |
| 接收方：`log[PrevLogIndex].Term != PrevLogTerm` 时拒绝 | 一致性检查（含 `PrevLogIndex` 超出日志长度的情况） | ✓ |
| 接收方：已有条目与新条目冲突时截断并覆盖；缺的追加 | `HandleAppendEntries` 的合入循环；重复发送同一批条目是幂等的 | ✓ |
| 接收方：`LeaderCommit > commitIndex` 时 `commitIndex = min(LeaderCommit, lastLogIndex)` | 同上 | ✓ |
| Leader：收到客户端命令就追加进本地日志 | `Start` | ✓ |
| Leader：`lastLogIndex >= nextIndex[follower]` 时发 AppendEntries | `replicateTo`（每 follower 一个复制循环） | ✓ |
| Leader：成功时更新 `nextIndex`/`matchIndex` | 同上 | ✓ |
| Leader：失败时 `nextIndex` 递减重试 | 一次回退一格（M1 求简单正确；按冲突位置快速回退是优化，见规划风险 2） | ✓（M1 版） |
| Leader：`N > commitIndex`、多数派 `matchIndex >= N`、且 `log[N].Term == currentTerm` 时提交 | `advanceCommitIndexLocked`（取中位数；旧任期条目不靠计数提交） | ✓ |

## 通用规则

| 图 2 条目 | 实现 | 状态 |
|---|---|---|
| `commitIndex > lastApplied` 时按序应用到状态机 | `runApplyLoop`（独立 goroutine，`lastApplied` 只在这里前进） | ✓ |
| RPC 请求或响应里的任期比自己大时，更新任期并转 follower | 各 handler 与各 RPC 回复处理处 | ✓ |
| Follower 超时未收到心跳就参选 | 选举计时 goroutine（随机 150–300ms，见下） | ✓ |
| Leader 当选后立即发一轮空 AppendEntries（确立领导地位） | `becomeLeaderLocked` 里立刻唤醒各复制循环 | ✓ |

## 计时参数（M1 实测值）

| 参数 | 值 | 说明 |
|---|---|---|
| 选举超时 | 随机 150–300ms | 论文建议值；随机化避免同时参选、反复分票 |
| 心跳间隔 | 50ms | 论文建议值 |
| 单次 RPC 超时 | 150ms | 不能比最短选举超时还长，否则一次选举里发不完票 |
| apply 兜底检查 | 10ms | 正常由 commit 推进唤醒，ticker 只是兜底 |

选主稳定性（M1 实测）：三节点空跑 30 秒，leader 不应频繁切换；
实测次数见 README。

## 本轮没做的图 2 之外的内容

| 内容 | 去向 |
|---|---|
| 持久化落盘（`Save` 写文件 + fsync，重启恢复） | M2 已完成：`FilePersister`（`internal/raft/persist_file.go`），格式与 fsync 点见 `docs/02-persistence.md` |
| InstallSnapshot RPC 与日志压缩 | M5 |
| Pre-Vote（防分区后旧 leader 搅局） | 明确不做（见规划） |
| ReadIndex 读优化 | 明确不做；读 follower 可能读到旧数据，已在文档写明 |
| 动态成员变更 | 明确不做；改配置要改文件、重启集群 |

# 确定性模拟器与测试（M3）

M3 是这个项目的成败关键：Raft 的 bug 藏在时序与故障的组合里，
靠“起三节点跑一跑”撞不出来。模拟器把三个 Raft 实例放进同一个进程，
用虚拟时钟 + 模拟网络驱动它们，再用故障注入器随机地分区、杀节点、
重启——跑 1000 步，所有不变式全程成立，且同一种子跑两次结果完全相同。

## 怎么跑

```bash
make sim     # 故障矩阵（9 个场景 × 种子 1/2/3）+ 一次 1000 步随机长跑（种子 42）
make check   # 全量：vet + gofmt + go test -race（含模拟器的确定性、性能测试）
```

模拟器测试都在 `internal/sim/`，数据目录用 `t.TempDir()`，
跑完自动清理，不留文件在工作目录里。

## 结构

| 文件 | 职责 |
|---|---|
| `internal/raft/clock.go` | `Clock` 接口（`Now` / `AfterFunc` / `NewTicker`）+ 真实实现 `RealClock`（包 `time`）。`raft.go` 只调接口，不直接碰 `time` 的定时器 |
| `internal/raft/transport.go` | `Transport` 接口改成**异步回调式**：`RequestVote(addr, args, cb)` / `AppendEntries(addr, args, cb)`。`HTTPTransport` 每 RPC 起一个 goroutine，回调里回包；消息丢了就不调回调（等下一轮心跳/选举重试，与真实网络超时等价） |
| `internal/sim/virtual_clock.go` | `VirtualClock`：统一事件队列，定时器回调与网络消息投递都排进去，按（到期时间，创建顺序）执行；只有 `Advance(d)` 调用时时间才走 |
| `internal/sim/network.go` | 模拟网络：实现 `raft.Transport`，发消息时按延迟区间（默认 0–5ms）排投递事件；分区与丢包在发送时判定；`Partition(a, b)` / `SetPartition` / `Heal` |
| `internal/sim/sim.go` | `Sim`：三个节点 + 驱动 + 客户端操作（`Put` / `Get` / `Kill` / `Restart` / `Partition` / `Heal` / `Advance` / `Settle`），以及已提交前缀的水位线 |
| `internal/sim/fault.go` | `FaultDriver`：随机故障驱动器（写 / 分区 / 恢复 / 杀节点 / 重启），每步结束检查不变式 |
| `internal/sim/invariants.go` | 三个不变式的检查（见下） |
| `internal/raft/raft.go`（改） | 重构：选举定时器从 goroutine 改成 `AfterFunc` 回调链（带代数防旧回调），每 follower 的复制从 goroutine 改成 `replicateOnce` + 定时器链；`NewRaft` 加 `WithClock` / `WithRand` 选项，现有调用方不动 |
| `internal/kv/store.go`（改） | 加只读 `All() map[string]string` 快照，模拟器拿它比对状态机 |

## 重构说明：M1/M2 的行为没变

M3 的重构只换了“时间”和“传输”的来源，没改 Raft 的决策逻辑：

- 选举超时、复制、心跳的**触发条件与处理函数**与原来逐行对应，
  只是定时器从 `time.AfterFunc` / goroutine + channel 换成了 `Clock` 接口。
- `Transport` 从同步返回改成异步回调：`HTTPTransport` 收到回复后调回调，
  与原来“收到回复后继续处理”是一回事；消息丢了不调回调，等下一轮重试。
- `NewRaft` 的默认选项就是原来的行为（`RealClock` + 时间种子 RNG），
  `cmd/server` 与所有旧测试一字没改。
- 验证：M1/M2 的全部旧测试继续全绿（见下“实测记录”），
  `scripts/demo.sh` 继续通过。

重构时修了一个真实的竞争：原来用 `newTestRaft` 建节点但不调 `Run()` 的
单元测试，会让 handler 里的 `time.AfterFunc` 定时器在测试结束后才触发，
与已结束的测试对象竞争（`-race` 抓到）。现在 `armElectionTimerLocked` /
`triggerReplicateLocked` / `scheduleReplicateLocked` 在 `!r.running` 时
直接返回——没 `Run()` 的节点不排定时器，语义上也正确。

## 三个不变式（每次检查都跑）

检查前先 `drain`：把各节点 apply 循环里攒着的已提交日志都应用完，
再读快照——否则读到的只是“还没来得及应用”的中间态。

| # | 不变式 | 怎么查 |
|---|---|---|
| (a) | 同一任期最多一个 leader | 活节点里，同一任期的 leader 不超过 1 个 |
| (b) | 已提交的日志条目永不丢失 | 维护一条“已提交前缀”水位线（历史上任何节点 `commitIndex` 达到过的最长前缀，只增不减）；每个活节点的已提交前缀（下标 ≤ 它自己的 `commitIndex`）必须与水位线逐条相同（term + 命令） |
| (c) | 状态机按序应用 | 每个活节点的已应用序列必须是下标 1, 2, 3, … 严格递增、无跳号，每条应用的命令与该节点日志同一位置的条目相同，且应用条数 == `lastApplied` |

(b) 的两个关键点（都是 M3 调试时踩出来的，检查写错了会误报）：

- **水位线只能从 `commitIndex` 达到当前最大的那个节点取**。从“日志够长”
  的节点取会拿到未提交的旧条目，污染水位线。
- **比较只到各节点的 `commitIndex`**。落后节点的日志里可能有一条
  旧 leader 留下的未提交条目，与水位线不同——这是 Raft 允许的，
  迟早会被新 leader 截掉。拿全日志去比会误判成安全破坏。

## 故障矩阵（`TestFaultMatrix`）

9 个场景 × 种子 1/2/3 = 27 次运行。每个场景收尾都是：
恢复连通、重启所有死节点、等已提交日志收敛，再断言不变式 +
所有活节点 KV 一致 + 已提交日志一致。

| 场景 | 内容 |
|---|---|
| baseline/无故障写10条 | 对照组 |
| partition/单节点孤立 | 孤立 node2，多数派写 5 条，孤立节点读不到，恢复后全一致 |
| partition/2对1分裂（leader被孤立） | 孤立旧 leader，多数派选新 leader、写 5 条，恢复后全一致 |
| crash/leader崩 | 杀 leader，写 5 条，重启旧 leader，恢复后全一致 |
| crash/follower崩 | 杀 follower，写 5 条，重启，恢复后全一致 |
| crash/连续崩两个 | 只剩一个节点时写必须失败；重启后恢复 |
| restart/leader立即重启 | 杀 leader 后立即重启，写 5 条，恢复后全一致 |
| restart/follower隔5秒重启 | 杀 follower，隔 5 秒虚拟时间再重启（期间集群继续写），恢复后全一致 |
| combined/分区+杀leader+重启+写 | 分区孤立 leader → 多数派写 → 杀被孤立的 leader → 继续写 → 恢复 |

另外三个网络级测试：丢包 100% 时选不出 leader；分区后多数派
1 秒内选出新 leader；50ms 延迟下 100ms 内选不出 leader、5 秒内能选出并写一条。

## 随机故障长跑（`TestRandomFaultLong_1000Steps`）

种子 42，1000 步。驱动器每步随机选一个操作（写 40%、分区 15%、
恢复 10%、杀节点 15%、重启 20%，无效操作自动换成“推进时间”），
每步推进 50–200ms 虚拟时间，再排空、检查三个不变式。
种子 42 的一次实测：写 386（成功 194 / 失败 192——集群经常没多数派，
写失败是正常结果）、分区 103 / 恢复 88、杀 142 / 重启 142，
最终已提交日志 220 条、KV 220 条。

## 确定性（`TestDeterminism_SameSeedTwice`）

同一个种子（42）跑两次 1000 步，最终已提交日志（逐条 term + 命令）
与最终 KV 必须完全相同。实测：**完全相同**。leader 历史（逐任期的
leader 身份）也完全相同（4820 次切换），所以没有任何残留的非确定性——
断言本来只要求“最终已提交日志 + 最终 KV + 不变式”，实测比要求更强。

确定性靠四点保证：

1. **单线程事件队列**：虚拟时钟里定时器回调与网络消息投递是同一条
   队列，按（到期时间，创建顺序）执行，与 goroutine 调度无关。
   apply 循环与复制回调只在事件里做“读 channel / 发消息”的动作，
   不参与决策排序。
2. **随机性全来自带种子的 RNG**：每个节点一个 RNG（选举超时用，
   种子由主种子、节点下标、重启次数派生），网络一个 RNG（延迟、丢包），
   驱动器一个 RNG（操作选择）。不用全局随机源。
3. **消息投递是同步的**：投递事件执行时直接调目标节点的 handler，
   再调发送方的回调——全在 `Advance` 的调用栈里，没有“先放 channel
   等别的 goroutine 取”的环节。
4. **虚拟时钟的锁只保护队列结构**：`Advance` 执行回调时不持锁，
   回调里调 `AfterFunc` 不会死锁；锁不参与事件排序。

## 性能（`TestRandomFaultLong_Performance`）

1000 步随机长跑（种子 99）必须在 10 秒真实时间内跑完。
实测 **3.0 秒**（写 415，成功 232 / 失败 183）。

## 模拟器里的 Put：两个语义

- **提交后会等各活节点追上**（最多 500ms 虚拟时间）：`Put` 返回后，
  在所有活节点上都能读到这条写。被分区隔离的节点追不上，
  预算用完直接返回成功（写本身已提交，隔离节点以后追上）。
- **写到被孤立的旧 leader 会重试**：分区刚发生时，旧 leader 还自认
  leader、任期低。`Put` 先写它，2 秒虚拟时间内没提交（健康集群提交
  只要 ~10–50ms），就重新找 leader——换人了就在新 leader 上再写一次
  （旧 leader 那条未提交的记录重进集群时会被截掉，不进最终日志）；
  没换人就继续等满 10 秒。同一个 key 写两次是幂等的，不会弄脏 KV。

## 已知的边界（不是 bug）

- **选举超时假设往返时间远小于 150ms**。选举超时是 150–300ms，
  默认网络延迟 0–5ms。如果把延迟调到单向 500ms（往返 1 秒），
  候选人在选票回来之前就超时重选，选主会活锁——这是参数假设，
  不是模拟器的 bug。延迟测试用 50ms 单向延迟（往返 100ms，
  在选举超时之内）。
- **follower 的未提交旧尾巴不会马上被截掉**。leader 只发自己有的条目：
  follower 日志比 leader 长出来的那几条（旧 leader 留下的），
  空心跳不会截掉它们，要等下一次写（leader 日志长过它们）才会被覆盖。
  所以“各节点全日志相等”不是 Raft 的保证——收敛检查只比已提交前缀。
  这也是 `waitLogsEqual` / `assertAllLogsEqual` 只比已提交前缀的原因。
- **新 leader 不能用“数人头”提交旧任期日志**（M2 已记录，见
  `docs/02-persistence.md`）：提交水位线可能停在旧任期条目之前，
  直到一条当前任期条目进来才一起提交。这是论文图 2 的要求，不是 bug。
- **任期数字会涨得很高**。长跑里被孤立的节点每 150–300ms 就重选一次，
  1000 步下来任期到四千多——正常现象，任期只增不减。

## M3 实测记录（2026-11-17，本机）

- `go vet ./...` 全绿；`gofmt -l .` 为空；`make check` 全绿。
- `go test ./... -count=1 -race`：**39 个顶层测试全绿**（M1/M2 的 28 个
  一个不少 + 模拟器 11 个：虚拟时钟 4 个、网络 3 个、故障矩阵 1 个
  （27 个子测试）、随机长跑 1 个、确定性 1 个、性能 1 个）；
  `go vet` 全绿；`gofmt -l` 为空。
- `make sim` 通过：故障矩阵 27/27 + 1000 步长跑（种子 42）。
- `scripts/demo.sh` 仍通过（重构没改真实路径行为）。
- 确定性：种子 42 跑两次，最终已提交日志 220 条、最终 KV 完全相同，
  leader 历史也完全相同（4820 次切换）。
- 性能：1000 步 3.0 秒（上限 10 秒）。

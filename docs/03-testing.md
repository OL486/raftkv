# 确定性模拟器与测试（M3）

M3 是这个项目的成败关键：Raft 的 bug 藏在时序与故障的组合里，
靠“起三节点跑一跑”撞不出来。模拟器把三个 Raft 实例放进同一个进程，
用虚拟时钟 + 模拟网络驱动它们，再用故障注入器随机地分区、杀节点、
重启——跑 1000 步，所有不变式全程成立，且同一种子跑两次结果完全相同。

## 怎么跑

```bash
make sim     # 故障矩阵（9 个场景 × 种子 1/2/3）+ 一次 1000 步随机长跑（种子 42）
make check   # 全量：vet + gofmt + go test -race（含模拟器的确定性、性能测试）
# 线性一致性检查（M4，见下）：
go test ./internal/linearizability/ -count=1 -race -v
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
| `internal/sim/concurrent.go` | M4：并发驱动 `RunConcurrent`——3 个客户端并发发 Put/线性化读，确定性的协作式调度，产出带虚拟时间戳的操作历史 |
| `internal/linearizability/linearizability.go` | M4：Porcupine 模型检查（KV 模型按 key 分区 + `Check` 入口） |
| `internal/linearizability/linearizability_test.go` | M4：模拟器 20 种子、真实集群 smoke、反例测试 |
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

## 线性一致性检查（M4）

Raft 保证了“已提交日志在各节点一致”，但这不等于“客户端看到的
操作历史是线性一致的”：读到旧值、写丢了又出现，日志层面都看不出来。
M4 用 Porcupine（`github.com/anishathalye/porcupine` v1.3.1，
测试依赖）做模型检查：3 个客户端并发发 Put/读，记下每个操作
调用/返回的时间，验证整段历史存在一个与真实时间相容的顺序执行。

### 前提：读必须是线性化的，检查才有意义

M1 的 `GET /kv/get` 只读本节点、不进日志：读 follower 可能读到
旧数据——这种读天生不是线性化的，拿它去跑 Porcupine 会误报。
所以 M4 加了一条线性化读路径，老路径不动（它保持快速、可能读旧，
文档已写明）：

| | `GET /kv/get`（老路径） | `POST /kv/read`（M4，线性化读） |
|---|---|---|
| 怎么读 | 直接读本节点状态机 | 把 `Read{Key}` 命令追加进 Raft 日志，等它被应用后返回该时刻的值 |
| 线性化点 | 没有 | 该命令在日志里提交/应用的位置（状态机应用到它时原样返回当前值，本身是 no-op，不改数据） |
| 读谁 | 任意节点（常读到旧值） | 只有 leader 接受；不是 leader 返回 503 `not_leader` + LeaderID（和 `/kv/put` 同款） |
| 代价 | 一次本地读 | 一次完整 Raft 提交（和 Put 一样贵） |
| 用途 | 快速、容忍旧值的读 | 需要“读到最新已提交值”的地方、线性一致性检查 |

实现点：

- KV 命令加 `Read{Key}`（`internal/kv/store.go` 的 `Command` 编解码
  加 `"read"`，`Apply` 返回 `([]byte, error)`：Put/Delete 返回
  `{"ok":true}`，Read 返回 `{"value":"...","found":true/false}`）。
  `Apply` 的签名变了，所有调用方（node、sim、kv 测试）都跟着改了。
- 服务端 `POST /kv/read`（`internal/node/node.go`）：leader 追加
  Read 命令、等应用后返回**应用时刻的值**——不是等完再重读一次
  store（那样会读到之后才提交的写，线性化点就错了）。为此节点
  维护 `waiters map[int]chan []byte`（带结果的等待）与
  `results map[int][]byte` 缓存（最近 1024 条已应用结果）：
  刚追加的 Read 命令可能在 waiter 注册之前就已应用完，
  此时直接从缓存取应用时刻的结果。
- CLI：`get --linearizable <key>` 走 `/kv/read`，默认走老路
  （`cmd/cli/main.go`，`--linearizable` 是 get 的子命令 flag）。
- 模拟器：`Sim.Read` 走同样的 Read 日志命令
  （`internal/sim/sim.go`，复用 `tryPut`/`waitVisible` 的状态机；
  Read 是状态机 no-op，重试它安全），`Sim.AppliedResult(index)` 取
  各节点记录的应用时刻结果；检查器只用这条路径读。

### 模型（`internal/linearizability/linearizability.go`）

状态就是 `map[string]string`：

- `Put(k, v)`：置值，返回成功；失败的 Put（`PutOutput{OK:false}`）
  模型按“没生效”处理——但这只是兜底：失败的 Put 实际上可能
  日后提交（重试的命令已在日志里），那时“没生效”的建模会误报。
  所以 `Check` 会先拒绝含失败操作的历史，调用方必须保证
  测试里没有失败的操作（主测试里断言了这一点）。
- `Read(k)`：返回当前值或不存在。
- **按 key 分区**（`Partition`）：不同 key 的操作互不影响，
  历史线性一致当且仅当每个 key 的投影都线性一致——Porcupine
  文档里给 KV 举的例子。分区后每 key 的操作数不多，
  指数级的检查才跑得动（实测 1000 条操作检查只要 ~18ms）。
- `Step` 必须是纯函数：Put 时拷贝一份 map，不改传入的状态。

检查入口 `Check(history)` 调 `porcupine.CheckOperationsVerbose`
（超时 60 秒，超时按失败处理并写明是超时不是 Illegal）。
失败时诊断分两层：Porcupine 找到的**最大部分线性化**
（`PartialLinearizationsOperations()`，最小反例的上下文，
只显示前 3 组候选）+ **完整历史**（客户端 id、Call/Return
时间戳、操作与返回值），测试失败时用 `t.Fatalf` 完整打印出来。

### 并发驱动（`internal/sim/concurrent.go`）

模拟器是确定性的，所以并发不能用真 goroutine（会破坏 M3 的
确定性）：`Sim.RunConcurrent(seed, perClient [][]ConcurrentOp)`
用单驱动协作式调度——每 10ms 虚拟时间一个量子，轮流推进每个
客户端的状态机：

```
concFindLeader → concWaitCommit → concWaitVisible → 下一个操作
```

- 每个客户端两个操作之间随机想 0–50ms（由种子决定），
  这样操作区间大量重叠（实测 99.9% 的操作与别的重叠）。
- `concWaitCommit` 的重试逻辑与 `Sim.Put` 完全一样：
  5 秒找 leader 预算；第一次等提交 2 秒，超时换 leader 重试，
  重试再等满 10 秒；失败的操作会记下来并返回 error——
  测试必须先断言没有失败操作再检查（失败的 Put 可能日后提交，
  模型按“没生效”处理会误报）。
- `concWaitVisible`：等提交的水位线被**所有活节点**应用完
  （与 `Sim.Put` 的可见性保证一致），最多 500ms 虚拟时间；
  超时也算成功返回（写本身已提交），但这时读别的客户端
  不保证可见——测试里读操作同样走 `waitVisible`，所以
  历史里的读看到的值一定是“已提交且各活节点可见”的。
- 30 分钟虚拟时间安全上限：到点还没跑完直接失败
  （防驱动 bug 导致无限循环）。
- `OpRecord`：`ClientID` / `Kind` / `Key` / `Value` / `Found` /
  `OK` / `Call` / `Return`（虚拟时间，纳秒数）。虚拟时钟
  只往前走，Call 在调用时读、Return 在返回时读，
  所以“先返回的操作一定先于后调用的操作”这一真实顺序
  在虚拟时间戳上同样成立——Porcupine 可以直接用它当时间戳
  （`FromSimRecords` 转换）。

真实集群的 smoke 测试用真实时间（`time.Now().UnixNano()`）：
只要单调且反映真实调用/返回顺序即可。

### 测试（`internal/linearizability/linearizability_test.go`）

| 测试 | 内容 |
|---|---|
| `TestLinearizability_Sim_20Seeds` | 主测试：3 客户端并发 1000 次 Put/线性化读（Put 60%、读 40%，key 空间 10 个，Put 的值全局唯一 `v<seed>-<client>-<seq>`，这样读到旧值一定能被发现），20 个不同种子，Porcupine 全部通过；每种子计时，总耗时断言 < 5 分钟；另有重叠率断言（与别的重叠的操作必须 > 50%，实测 99.9%——防驱动悄悄退化成串行、检查变空洞） |
| `TestLinearizability_RealCluster` | 真实三进程（复用 M2 崩溃测试的手法，`go build` 到 `t.TempDir()`，端口 18501–18503），3 个客户端 goroutine 并发发 198 次 Put/线性化读（每操作 10 秒重试上限），同样过 Porcupine 检查——证明真实集群也是线性一致的，不只模拟器里是 |
| `TestChecker_Counterexample` / `TestChecker_StaleReadValue` | 反例：Put 返回成功之后才开始的读却读不到 / 读到旧值，检查器必须报不通过（验证检查器真能抓到问题） |
| `TestChecker_ConcurrentButLinearizable` / `TestModel_SequentialHistory` | 健全性检查：重叠但可线性化的历史、顺序历史必须通过 |

### 失败时怎么读

检查不通过时测试输出是这样的结构：

```
历史不是线性一致的。
分区 0 的最大部分线性化（共 N 组候选）：
  候选 0： Put("k1"="v…") -> … Read("k1") -> …
完整历史（1000 条，Call/Return 为纳秒时间戳）：
  #0 客户端0 [123, 456] Put("k1"="v…") -> {OK:true}
  ...
```

读法：先看“最大部分线性化”——它是 Porcupine 能排出来的
最长前缀，断在哪个操作附近，问题大概率就在那附近；
再按 Call/Return 时间戳在完整历史里定位那几个操作的
真实重叠关系（哪个 Put 在读返回之后才提交、哪个读
发生在哪个 Put 返回之后）。

### M4 实测记录（2026-11-17，本机）

- `go vet ./...` 全绿；`gofmt -l .` 为空；`make check` 与 `make sim`
  通过；`scripts/demo.sh` 仍通过。
- `go test ./... -count=1 -race`：**46 个顶层测试全绿**（M3 的 39 个
  一个不少 + M4 新增 7 个：检查器 6 个 + `TestStore_ReadCommand` 1 个）。
- 模拟器 20 种子：每种子 3 客户端 × 1000 次操作（Put 60% / 线性化读
  40%，10 个 key），**20/20 线性一致**；每种子约 1.9 秒，
  总耗时 **38.7 秒**（上限 5 分钟）；操作重叠率 99.9%。
  耗时大头在模拟器：单次 1000 操作的 Porcupine 检查只要 ~18ms。
- 真实集群 smoke：3 客户端并发 198 次操作，**线性一致**，
  检查耗时 1.4ms。
- 反例：两个人工非线性历史（读不到已提交的值、读到旧值）
  都被检查器正确拒绝；重叠但可线性化的历史、顺序历史都通过。
- 手动验证（临时 `cmd/manual-m4-check`，跑完已删）：
  种子 99 的 3 客户端 1000 操作：0 失败、998/1000 重叠、
  检查通过（17.8ms）；反例检查报不通过。
  CLI 手动验证：`get --linearizable` 读到最新值、读不存在的 key
  退出 1 并提示不存在、直接调 follower 的 `/kv/read` 返回
  `not_leader` + leader id。

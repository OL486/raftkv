# 持久化、快照与重启恢复（M2/M5）

Raft 论文图 2 要求：`currentTerm`、`votedFor`、`log[]` 这三项持久状态，
必须在“回复 RPC 或继续下一步之前”落盘。M2 把它们落在 `--data`
目录下的两个文件里，重启后恢复；M5 又加了一个快照文件，
日志长到阈值就拍快照、截断日志（见“快照与日志压缩”一节）。

## 文件与格式

数据目录（`--data` 参数，如 `./data1`）里只有三个文件：

| 文件 | 内容 | 写方式 |
|---|---|---|
| `raft.log` | 日志：每条 entry 一段，追加写 | 写 + fsync |
| `meta` | 元数据：`currentTerm`、`votedFor`、`lastApplied` | 原子写（临时文件 + fsync + 改名 + fsync 目录） |
| `snapshot` | 快照：状态机全量 + `lastIncludedIndex` / `lastIncludedTerm` | 原子写（同 meta 的手法，见快照一节） |

### `raft.log`：一条 entry 的编码

```text
[term varint][命令长度 varint][命令字节][CRC32 大端 4 字节]
```

- `term`：写入这条日志时 leader 的任期，`encoding/binary` 的 Uvarint。
- 命令长度：命令字节数，Uvarint。
- 命令字节：状态机命令的原始字节（目前是 KV 命令的 JSON，见 `kv.Command`）。
- CRC32：IEEE 多项式，覆盖前面三段（不含 CRC 自身），大端存 4 字节。
- 日志下标 0 是一条哨兵 entry（`term=0`、空命令），也按同样格式落盘，
  这样 Load 读回来的日志与内存里的一模一样，下标从 1 开始的约定不被打破。

### `meta`：JSON，一行

```json
{"currentTerm":3,"votedFor":"node2","lastApplied":42}
```

`lastApplied` 是“已应用到状态机的最大日志下标”，见“重启重放”一节。

## 写顺序与 fsync 点

`Save()` 的顺序固定是**先日志、后 meta**，两部分都落盘后才返回。
调用点都在 `raft.go` 的 `persistLocked()`（每次改动持久状态后都调），
调用时持有 Raft 的 `mu`，所以同一时刻只有一个 Save 在跑：

| 触发点 | 落盘内容 | 为什么必须先落盘再继续 |
|---|---|---|
| `Start`（leader 接到写） | 新 entry 进日志 | 回复客户端“写成功”之前，日志必须已稳固，否则崩溃会丢已确认的写 |
| `HandleAppendEntries`（follower 收到新 entry / 冲突截断） | 日志追加或重写 | 回复 `Success=true` 之前，日志必须已稳固，否则 leader 会以为多数派已存下它 |
| `startElection`（任期 +1、投自己一票） | term + votedFor 进 meta | 参选前必须先把“本任期已投票”落盘，否则崩溃重启后同一任期可能投两次票 |
| `HandleRequestVote`（投给别人） | votedFor 进 meta | 同上：投票前必须先落盘 |
| `stepDownLocked`（任期变大） | term 进 meta | 任期是持久状态的一部分 |
| `runApplyLoop`（每应用一条） | lastApplied 进 meta（`SaveMeta`，只写 meta 文件） | 见“重启重放” |

性能：每条日志 fsync 一次、每次应用都写一次 meta——这是设计，
不是 bug。M2 不做性能要求（写吞吐的基准是 M4 之后的事）；
换来的是崩溃语义简单：Save 返回 = 稳固。
M5 又补了一点：apply 循环只改 `lastApplied`、不改日志，
所以它走 `SaveMeta`（`Persister` 接口的专用方法），
不再每次都把整条日志比一遍、拷一遍——100k 条的测试里，
这是把耗时从 65 秒降到 27 秒的主要原因（见快照一节末尾）。

### 持久化失败是致命错误

`persistLocked()` 里 Save 一旦返回错误，节点直接 `log.Fatalf` 退出
（fail-stop）。一个落不了盘的节点不能继续服务——它可能回复了
“写成功”却没存下这条写。重启后从磁盘恢复，能恢复多少就是多少，
绝不会把“已确认”的写丢了又装作没事。

## 日志只追加，meta 只写小文件

`Save()` 拿到的是完整的新状态，但文件实现不重写整个文件，
而是拿新状态跟“上次成功落盘”的快照做差分：

- 新日志是已落盘日志的严格扩展 → 只把新增的 entry 追加到 `raft.log` 尾；
- 日志被截断 / 冲突改写（新日志在某个下标处与已落盘的不同）→
  整个日志文件重写：写临时文件 `raft.log.tmp` → fsync → 改名覆盖
  `raft.log` → fsync 目录。原文件在改名前一直完好，
  崩溃后要么还是旧文件、要么已经是完整的新文件，
  绝不会留下一个写一半的日志文件；
- meta 只有 `currentTerm` / `votedFor` / `lastApplied` 变了才重写
  （同样是临时文件 + fsync + 改名 + fsync 目录）。

比较是全量逐条比的（O(已落盘条数)）。M2 的量级下可忽略；
日志变长后由 M5 的快照兜底——快照会把日志截断到 5000 条以内，
所以每次 Save 的比较/拷贝也被截在 5000 条以内（见快照一节）。
apply 循环的 Save 更进一步：它根本不走这条路（`SaveMeta` 只写 meta）。

## 崩溃写一半的处理

`Load()` 按顺序读 entry，逐条验 CRC；遇到以下任何一种情况，
都把文件截断到最后一条完整 entry 处，丢弃写一半的尾巴，**不报错**：

- 长度不够（varint 或命令字节没写完就断了）；
- 命令长度超过剩余字节数；
- CRC 对不上。

崩溃写一半是预期内的事（kill -9 可以发生在写盘的任何时刻），
不是数据损坏。截断后文件与“最后一次成功 Save 的状态”一致，
下次 Save 会按差分把缺的补上。

真正的数据损坏（meta 文件不是合法 JSON、日志第一条不是哨兵、
`lastApplied` 指向不存在的下标）则返回错误，节点拒绝启动——
宁可 loud fail，也不带着猜错的状态跑。

## 重启重放

启动流程（`node.New`）：

1. `Load()` 读回 term、votedFor、日志、lastApplied；
   `NewRaft` 恢复它们，并把 `commitIndex` 也设为 `lastApplied`——
   被应用过的条目一定是已提交的，设回去是安全的。
   （有快照时先恢复快照、再读快照之后的日志，见快照一节
   “重启加载顺序与 meta 的语义”。）
2. node 层把 `1..lastApplied` 的日志按序重放到 KV 状态机
   （`lastApplied` 设到该位置；有快照时是先恢复快照、
   再重放 `snapshotIndex+1..lastApplied`，快照里的不重放）。
3. 再启动选举计时、apply 循环与 HTTP 服务。

**只重放到 `lastApplied`，而不是整条日志**：`lastApplied` 永远
≤ `commitIndex`，所以重放的都是已提交的条目；`lastApplied`
之后的条目（已提交但还没应用、或者根本还没提交）绝不能在
启动时喂给状态机——它们等新 leader 的心跳带 `LeaderCommit`
过来后，再由 apply 循环正常应用。

`lastApplied` 的落盘点在 apply 循环里：**先写 meta、再投递给状态机**
（顺序不能反）。状态机执行完、HTTP 回了 200，客户端就认为这条写
稳固了；如果先投递后落盘，kill -9 落在两者之间时，重启后
`lastApplied` 还是旧值，日志里的那条虽然在，却因图 2 的提交规则
（新 leader 不能用数人头的方式提交旧任期条目）迟迟进不了状态机，
客户端刚读到的写就“丢”了——M5 测试时真实抓到过一次
（`TestCrashRestart_WriteInFlight` 在 `-race` 整套跑时失败，
根因就是这个窗口）。先落盘的话，崩溃在落盘与投递之间时，
重启把这条重放一遍（Put/Delete 都是幂等的），不会丢、也不会多应用。

## “数据在”的两种含义：日志里有 vs 状态机里有

这是 M2 测试时抓到的一个真问题（见 `internal/raft/crash_test.go`
的注释），值得单独写清楚——它不是持久化的 bug，是 Raft 本身的语义：

**新 leader 不能用“数人头”的方式提交旧任期的日志。**
图 2 的提交规则要求 `log[N].term == currentTerm`：
leader 只能推进本任期的条目，旧任期的条目是随着本任期条目的
提交被“间接”提交的。

后果：一次 leader 变更后，如果新当选的 leader 自己
`commitIndex` 还没追上（例如它当选时手里的日志是全的，
但 `commitIndex` 还停在旧任期），它的 `commitIndex` 会一直
卡住，直到有了一条本任期的新写——旧条目虽然已经躺在所有
节点的日志文件里（持久化没丢任何东西），却迟迟进不了各节点的
状态机，`get` 读不到它们。

所以崩溃测试里“重启后读到数据”的断言，必须先经当前 leader
写一条探针（本任期的新条目），把前缀一起提交了，再查各节点的
状态机。没有这一步，测试会在“谁当选”上抖动——
当选的是原来 `commitIndex` 已追上的节点就过，
当选的是还没追上的节点就卡住。这不是实现不稳定，
是断言写错了：它假设了 Raft 没有的保证。

## 快照与日志压缩（M5）

日志只增不减撑不久：写 10 万条后 `raft.log` 就是 10 万条的量，
启动全量读回、每次 Save 全量比较都越来越贵。
M5 的做法是标准的 Raft 快照（论文 §7）：已应用的日志前缀
可以丢掉——它的效果已经在状态机里，快照就是状态机的全量。

### 触发阈值与保留量

- **阈值**：`SnapshotThreshold = 5000`（`internal/raft/snapshot.go`）。
  每个节点自己在 apply 循环里检查：自上次快照以来已应用的条目
  达到 5000，就拍一次快照（`TakeSnapshot`）。
  拍快照的是每个节点自己，不是只有 leader——follower 应用到
  5000 条一样拍，日志不会在 follower 身上无限长。
- **保留量**：拍完只留快照之后的条目。触发条件是“已应用 − 快照位置
  ≥ 5000”，而应用是逐条推进的，所以触发那一刻保留量恰好是 0；
  之后新到的条目再慢慢攒起来，到下一个 5000 再拍。
  稳态下保留日志在 0–4999 条之间（验收要求 < 5000）。
- **为什么是 5000，不是规划里的“日志 > 10000 才拍”**：
  规划原案是“日志 > 10000 且 commitIndex − snapshotIndex > 5000 时拍”，
  但它自己的验收是“100k 写后日志 < 5000”——按原案拍完最多还剩
  10000 条，验收过不了。所以阈值改成“自上次快照已应用 5000 条就拍”，
  保留量由构造保证 ≤ 4999。数字写死在常量里，没做成可配：
  5000 条日志约 235KB，拍一次快照的 JSON 编码 + 写盘在 100k 的量级下
  是几十毫秒，摊到每条写上可忽略；阈值再小只会让快照更频繁，
  没意义。
- **只压缩到 `lastApplied` 为止**：绝不压缩还没应用的条目。
  M4 的 `/kv/read` 依赖“应用时刻的结果”——一条已提交但还没应用的
  日志，读命令要等它应用后才返回结果；如果快照把它压缩掉了，
  读就永远等不到结果。所以触发点在 apply 循环里（应用完一条才检查），
  不在提交处。

### 快照文件格式

`snapshot` 文件与 `raft.log` 是分开的两个文件：

```text
[index varint][term varint][数据长度 varint][数据字节][CRC32 大端 4 字节]
```

- `index` / `term`：`lastIncludedIndex` / `lastIncludedTerm`（快照覆盖到的位置）。
- 数据字节：状态机全量——KV 的 `map[string]string` 的 JSON
  （`encoding/json` 编 map 时按键排序，所以同一份状态编出来的字节
  是确定的；模拟器的确定性对比与快照比较都靠这一点）。
- CRC32：IEEE 多项式，覆盖前面三段（与 `raft.log` 的 entry 编码同款手法）。
- 写快照文件是原子写：临时文件 + fsync + 改名 + fsync 目录（沿用 meta 的手法）。
  损坏的快照（CRC 对不上、长度不够、截断一半）加载时一定能发现，
  拒绝启动（fail-stop），绝不静默用空状态跑——见下文“崩溃窗口”。

### 日志基底（log base）：下标是绝对的，切片不是从 0 开始的

这是 M5 最容易错的地方，改完要逐个检查：

- 内存里 `log[0]` 永远是快照位置的条目：从未拍过快照时它是哨兵
  （下标 0，`term=0`）；拍过快照后它是占位条目
  （下标 = `snapshotIndex`，`term=snapshotTerm`，空命令）。
  所以 `log[i]` 的绝对下标是 `snapshotIndex + i`，
  `lastLogIndex = snapshotIndex + len(log) - 1`，
  `Debug().LogLen = len(log) - 1`（保留的条目数，不含占位条目）。
- 所有按下标取日志的地方都按“基底偏移”改过：
  AppendEntries 一致性检查（`PrevLogIndex` 落在快照里时跳过检查，
  恰好是快照位置时验快照任期）、`nextIndex` 回退下限
  （`snapshotIndex+1`，再往下就是快照里）、commit 计算（中位数取的是
  绝对下标）、apply 循环取命令（`log[lastApplied-snapshotIndex]`）、
  调试输出（`LogLen` 是保留条数）。
- `PersistentState` 里不存 `SnapshotIndex` / `SnapshotTerm`——
  它们由 Load 从“日志首条目 + 快照文件”推导出来
  （`deriveSnapshotBase`），存两份只会让两者有机会各说各话。

### InstallSnapshot（论文 §7）

leader 发现某个 follower 的 `nextIndex <= snapshotIndex`
（落后程度已超出 leader 保留的日志）时，不再逐条发 AppendEntries，
改发 `InstallSnapshot`（HTTP+JSON，`/raft/installSnapshot`，
超时 10 秒——快照数据可能有几 MB，150ms 的选举超时不够用）。

follower 安装快照：

- 只接受比本地新的快照（`LastIncludedIndex > snapshotIndex`），
  旧快照直接忽略（回成功，不用重复安装）；
  空数据拒绝安装；低任期拒绝。
- 截断日志：保留快照位置之后、且与快照同一分支的条目——
  本地该下标的任期与快照任期一致时才保留之后的部分，
  否则全丢（那些条目在分叉的分支上，不可能已提交）。
- `commitIndex` 推进到快照位置；`lastApplied` **不**在这里直接推——
  状态机恢复快照必须走 `applyCh` 按序进行，所以挂一个待投递的快照，
  让 apply 循环先把快照交给状态机，再应用它之后的日志。
  如果这段日志本地早已应用过（`lastApplied` 已超过快照位置），
  状态机里已经有这份数据，就不用再恢复一次，只压缩日志。
- 快照落盘（`SaveSnapshot` + `persistLocked`）成功后才回成功——
  leader 收到成功才敢把 `matchIndex` 推进到快照位置。

### 快速回退（ConflictIndex）：快照追赶等不起逐格回退

这是 M5 实现时补上的、规划里没写但绕不过去的一步。
M1 的 `nextIndex` 回退是一次一格：follower 落后 9000 条时，
新 leader 的 `nextIndex` 从 9501 起，要 4500 次心跳（225 秒）
才掉进快照区间——真实集群的追赶测试根本等不起。
所以 `AppendEntriesReply` 加了 `ConflictIndex`
（Raft 论文 §5.3 的标准优化）：

- `PrevLogIndex` 超出 follower 日志：提示 = `lastLogIndex + 1`；
- `PrevLogIndex` 处任期冲突：提示 = 该冲突任期连续段的首下标；
- 快照任期对不上：提示 = 快照位置（leader 看到它改发快照）。
- leader 按提示直接跳 `nextIndex`（只往小里跳，过期的回复不能
  把它往前推）；没有提示的老版本对端，退回逐格回退。
- 分区期间落后节点的任期会不断膨胀（收不到心跳、反复参选），
  恢复后旧 leader 先退位、新 leader 选出来，靠这套提示一两轮
  心跳就能定位到快照位置——模拟器与真实集群的追赶测试都靠它。

### 重启加载顺序与 meta 的语义

启动流程（`node.New` / `NewRaft`）在 M2 的三步之前，先多两步：

1. `Load()` 读回日志与 meta，同时从“日志首条目 + 快照文件”
   推导快照基底（`deriveSnapshotBase`）：
   - 日志首条目是哨兵（`term=0`）→ 基底是 0，快照文件即使存在
     也是孤儿（崩溃窗口的残留），忽略它；
   - 日志首条目是占位条目（`term>0`）→ 快照文件必须存在，
     且它的 `LastIncludedTerm` 必须等于占位条目的任期，
     否则拒绝启动（fail-stop）——数据丢了，不能静默空跑。
2. 快照位置 > 0 时，先从快照文件恢复状态机（`LoadSnapshot`），
   再重放快照之后的日志——只到 `lastApplied`（与 M2 同款规则，
   快照里已有的不重放）。
3. `meta` 的 `lastApplied` 与快照的 `lastIncludedIndex` 对齐：
   `lastApplied` 永远 ≥ `snapshotIndex`（快照只拍已应用的条目），
   恢复时如果 `lastApplied < snapshotIndex`（快照已落盘、
   apply 循环还没来得及把 `lastApplied` 写进 meta 就崩溃了），
   `NewRaft` 把 `lastApplied` 钳制到 `snapshotIndex`——
   快照本身就是“已应用到快照位置”的状态，不用再重放一遍。
   谁为准：**快照文件为准**（它是状态机的全量），meta 的
   `lastApplied` 只是“重放到哪里”的游标。

### 崩溃窗口（都按“宁可拒绝启动，不静默错跑”处理）

| 窗口 | 结果 |
|---|---|
| 快照文件写完、日志还没压缩就崩溃 | 快照文件是孤儿，Load 忽略它，基底仍是 0 |
| 日志已压缩、meta 还没写就崩溃 | meta 的旧 `lastApplied` ≤ 新日志覆盖范围（压缩只丢前缀），Load 校验通过 |
| 快照文件损坏（CRC / 长度不对） | Load / LoadSnapshot 报错，节点拒绝启动 |
| 日志已压缩、快照文件丢了 | Load 报错，节点拒绝启动 |
| 安装快照中途崩溃（快照文件已写、日志没压缩） | 同第一行：孤儿快照，忽略 |
| `TakeSnapshot` 期间崩溃（快照文件写完、日志重写一半） | 日志重写是原子的（临时文件 + 改名），要么旧日志、要么新日志，绝不写一半 |

### 实测记录（2026-11-17，本机）

- 模拟器 100k 条流水线写：27 秒；写完三节点快照下标都是 100000，
  保留日志 0 条，磁盘 `raft.log` 6 字节（只剩占位条目），
  `snapshot` 文件 2,200,013 字节（10 万条 KV 的全量）。
  没加 `SaveMeta` 之前同一测试要 65 秒——apply 循环每次全量
  Save 的比较 + 拷贝是主因，见上文“写顺序与 fsync 点”。
- 模拟器 6000 条顺序写（正常客户端路径）：8.6 秒，快照下标 5000，
  保留日志 < 5000。
- 真实集群 12000 条 HTTP 逐条写：20.9 秒；快照下标 10000，
  保留日志 2000 条，磁盘 `raft.log` 112,006 字节；
  kill -9 全部节点再重启，随机抽 100 个 key 线性化读全部正确。
- 落后 follower 追赶（模拟器 + 真实集群）：隔离一个 follower，
  多数派写 9000/9500 条，恢复后 follower 靠 InstallSnapshot 追上
  （模拟器观测到 2 次快照投递；真实集群观测到 leader 日志
  “发快照”与 follower 日志“安装快照”两行）。

## 与 Raft 论文的对照

| 论文要求 | 本实现 |
|---|---|
| 持久状态：`currentTerm`、`votedFor`、`log[]`，回复 RPC 前落盘 | `raft.log` + `meta`，`persistLocked()` 全覆盖（见上表） |
| 崩溃后持久状态不丢 | 日志追加 + fsync；meta 原子写；写一半的尾巴截断丢弃 |
| 投票前持久化 `votedFor` | `HandleRequestVote` 投之前先 `persistLocked()` |
| 日志冲突时截断 | `HandleAppendEntries` 截断后 `persistLocked()`，文件实现走重写路径 |
| 论文没要求、但实现加了的 | `lastApplied` 也落盘（进 meta），用于启动重放时划清“已应用”的边界 |

## 已知限制

- 每条写都 fsync、每次应用都写 meta，写吞吐不高——
  这是 M2 的设计取舍，性能优化不在本轮。
  （M5 的 `SaveMeta` 只是把 apply 路径的浪费砍掉，不是通用优化。）
- `Pre-Vote`、动态成员变更、ReadIndex 读优化明确不做（见规划）。

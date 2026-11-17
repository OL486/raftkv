# 持久化与重启恢复（M2）

Raft 论文图 2 要求：`currentTerm`、`votedFor`、`log[]` 这三项持久状态，
必须在“回复 RPC 或继续下一步之前”落盘。M2 把它们落在 `--data`
目录下的两个文件里，重启后恢复。

## 文件与格式

数据目录（`--data` 参数，如 `./data1`）里只有两个文件：

| 文件 | 内容 | 写方式 |
|---|---|---|
| `raft.log` | 日志：每条 entry 一段，追加写 | 写 + fsync |
| `meta` | 元数据：`currentTerm`、`votedFor`、`lastApplied` | 原子写（临时文件 + fsync + 改名 + fsync 目录） |

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
| `runApplyLoop`（每应用一条） | lastApplied 进 meta | 见“重启重放” |

性能：每条日志 fsync 一次、每次应用都写一次 meta——这是设计，
不是 bug。M2 不做性能要求（写吞吐的基准是 M4 之后的事）；
换来的是崩溃语义简单：Save 返回 = 稳固。

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
日志变长后由 M5 的快照兜底（快照会截断日志）。

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
2. node 层把 `1..lastApplied` 的日志按序重放到 KV 状态机
   （`lastApplied` 设到该位置）。
3. 再启动选举计时、apply 循环与 HTTP 服务。

**只重放到 `lastApplied`，而不是整条日志**：`lastApplied` 永远
≤ `commitIndex`，所以重放的都是已提交的条目；`lastApplied`
之后的条目（已提交但还没应用、或者根本还没提交）绝不能在
启动时喂给状态机——它们等新 leader 的心跳带 `LeaderCommit`
过来后，再由 apply 循环正常应用。

`lastApplied` 的落盘点在 apply 循环里：每投递一条给状态机，
就写一次 meta。注意“投递”不等于“状态机已执行”——
崩溃在两者之间时，重启会把这条重放一遍；
Put/Delete 都是幂等的，多应用一条是安全的。

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

## 与 Raft 论文的对照

| 论文要求 | 本实现 |
|---|---|
| 持久状态：`currentTerm`、`votedFor`、`log[]`，回复 RPC 前落盘 | `raft.log` + `meta`，`persistLocked()` 全覆盖（见上表） |
| 崩溃后持久状态不丢 | 日志追加 + fsync；meta 原子写；写一半的尾巴截断丢弃 |
| 投票前持久化 `votedFor` | `HandleRequestVote` 投之前先 `persistLocked()` |
| 日志冲突时截断 | `HandleAppendEntries` 截断后 `persistLocked()`，文件实现走重写路径 |
| 论文没要求、但实现加了的 | `lastApplied` 也落盘（进 meta），用于启动重放时划清“已应用”的边界 |

## 已知限制

- 日志只增不减，直到 M5 的快照来截断它；写 10 万条后
  `raft.log` 就是 10 万条的量，启动时全量读回。
- 每条写都 fsync、每次应用都写 meta，写吞吐不高——
  这是 M2 的设计取舍，性能优化不在本轮。
- 快照与 `InstallSnapshot` 是 M5 的事；`Pre-Vote`、
  动态成员变更、ReadIndex 读优化明确不做（见规划）。

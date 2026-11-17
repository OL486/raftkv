# raftkv

三节点 Raft 共识算法 + 构建在它上面的 KV 存储（Go，零外部依赖，标准库
`net/http` + JSON 做传输）。

客户端写一条 Put，要经过选主、日志复制、多数派提交、按序应用到状态机，
三个节点的 KV 内容最终一致；杀掉一个节点（甚至杀掉 leader）服务不中断。

## 当前状态：M4（线性一致性检查）

能跑的：

- 选主（随机选举超时 150–300ms，心跳 50ms）
- 日志复制（AppendEntries + 一致性检查 + 冲突截断，一次回退一格）
- 提交（matchIndex 中位数，限当前任期条目）
- KV 的 Put / Get（Delete 已在状态机与命令层实现，HTTP/CLI 暂不暴露）
- `curl` 看集群状态：`GET /debug/log`
- **持久化与重启恢复**：`--data` 目录下 `raft.log`（追加写 + fsync，
  每条 `[term varint][长度 varint][命令][CRC32]`）与 `meta`
  （`currentTerm` / `votedFor` / `lastApplied` 的 JSON，原子写）；
  kill -9 后重启，term 与日志全在，数据不丢；写一半的尾巴自动截断丢弃。
  详见 `docs/02-persistence.md`。
- **确定性模拟器**：三个 Raft 实例跑在同一个进程里，虚拟时钟 +
  模拟网络（可编程丢包、延迟、分区）+ 随机故障注入（分区、杀节点、
  重启、写请求）；每步检查三个不变式（同一任期最多一个 leader、
  已提交日志永不丢失、状态机按序应用）；同一种子跑两次结果完全相同。
  详见 `docs/03-testing.md`，跑 `make sim`。
- **线性化读与线性一致性检查**：`POST /kv/read` 把读命令写进 Raft
  日志、等应用后返回应用时刻的值（只有 leader 接受）；
  `internal/linearizability` 用 Porcupine 做模型检查——
  模拟器上 3 客户端并发 1000 次 Put/线性化读，20 个种子全部
  线性一致；真实三节点集群的 smoke 测试也通过。
  两条读路径的语义差异见 `docs/03-testing.md` 的 M4 一节。

不能跑的（按规划，后面里程碑做）：

- 快照与日志压缩（M5）：日志只增不减，写 10 万条后 `raft.log` 就是
  10 万条的量，启动时全量读回；每条写都 fsync、每次应用都写 meta，
  写吞吐不高——这是 M2 的设计取舍。
- 动态加节点、Pre-Vote、ReadIndex 读优化、TLS、WATCH、跨 key 事务：
  明确不做，见 `~/workspace/tech-project-plans.md` 项目 3 的不做清单。

## 快速开始

```bash
# 起三节点集群（前台运行，Ctrl-C 停掉全部节点）
make run

# 另开一个终端：
/tmp/raftkv-bin/raftkv-cli --config /tmp/raftkv-run/cluster.json put foo bar
/tmp/raftkv-bin/raftkv-cli --config /tmp/raftkv-run/cluster.json get foo
# → bar

# 看某个节点的 Raft 状态
curl -s localhost:8001/debug/log
# {"id":"node1","term":3,"role":"leader","logLen":2,"commitIndex":2,"lastApplied":2,"leader":"node1"}
```

想看完整流程（含杀 leader、选新 leader、kill -9 后重启恢复）直接跑演示：

```bash
./scripts/demo.sh
```

## 手动起三个节点

```bash
make build   # 二进制在 /tmp/raftkv-bin，不留在工作目录里
/tmp/raftkv-bin/raftkv-server --id node1 --config cluster.json --data ./data1 &
/tmp/raftkv-bin/raftkv-server --id node2 --config cluster.json --data ./data2 &
/tmp/raftkv-bin/raftkv-server --id node3 --config cluster.json --data ./data3 &
```

`--data` 是节点的持久化目录（M2 已真正生效）：里面有 `raft.log`
（日志）与 `meta`（term / votedFor / lastApplied）。**同一个节点
重启必须用同一个 `--data` 目录**，否则它会以为自己是全新节点。
不传 `--data` 时日志只放内存，重启丢数据（启动时打印警告）。
`cluster.json` 是静态配置，三个节点写死在里面（改配置要改文件、重启集群）。

### 杀 leader

```bash
# 先看谁是 leader
for p in 8001 8002 8003; do curl -s localhost:$p/debug/log; echo; done
# 杀掉 leader 那个进程；5 秒内应选出新 leader，写入继续成功
kill <leader 的 PID>
```

### kill -9 后重启恢复

```bash
# 杀掉全部三个节点（-9 也行），再用同一个 --data 目录逐个重启：
kill -9 <node1 的 PID> <node2 的 PID> <node3 的 PID>
/tmp/raftkv-bin/raftkv-server --id node1 --config cluster.json --data ./data1 &
/tmp/raftkv-bin/raftkv-server --id node2 --config cluster.json --data ./data2 &
/tmp/raftkv-bin/raftkv-server --id node3 --config cluster.json --data ./data3 &
# 重启后 /debug/log 的 term 与 logLen 与杀之前一致（term 只会变大），
# 数据全在；重启后写入继续正常。
```

## HTTP 接口

| 方法与路径 | 说明 |
|---|---|
| `POST /kv/put` `{"key","value"}` | 只有 leader 接受；不是 leader 返回 503 `{"error":"not_leader","leader":"nodeX"}`；等提交后返回 `{"ok":true,"index":N}`，3 秒没提交返回 504 |
| `GET /kv/get?key=foo` | 读本节点的本地状态机，返回 `{"key","value","found"}`。**注意：读 follower 可能读到旧数据**（本节点 `lastApplied` 落后于 leader 的 `commitIndex`）；要读最新已提交数据请用下面的 `/kv/read` |
| `POST /kv/read` `{"key"}` | **线性化读**：把 `Read{Key}` 命令追加进 Raft 日志，等它被应用后返回应用时刻的值 `{"ok":true,"key","value","found"}`。只有 leader 接受；不是 leader 返回 503 `not_leader` + leader id；3 秒没应用返回 504。代价与 Put 相当（一次完整提交） |
| `GET /debug/log` | `{"id","term","role","logLen","commitIndex","lastApplied","leader"}`，`logLen` 是用户日志条数（不含哨兵条目） |
| `POST /raft/requestVote` | Raft 投票 RPC（JSON） |
| `POST /raft/appendEntries` | Raft 日志复制 / 心跳 RPC（JSON） |

## 客户端

```bash
raftkv-cli --config cluster.json put foo bar
raftkv-cli --config cluster.json get foo
# 线性化读：走 Raft 日志，返回应用时刻的值（贵，但保证读到最新已提交值）
raftkv-cli --config cluster.json get --linearizable foo
```

客户端先随便连一个节点，收到 `NotLeader` 就按提示重定向到 leader；
leader 挂了就重试下一个节点，5 秒还没成功就报错退出。

## 选举参数

| 参数 | 值 |
|---|---|
| 选举超时 | 随机 150–300ms |
| 心跳间隔 | 50ms |
| 单次 RPC 超时 | 150ms |
| `/kv/put` 等提交超时 | 3s |
| CLI 整体超时 | 5s |

选主稳定性：三节点空跑 30 秒，leader 不应频繁切换（M1 实测见下）。

## 目录结构

```text
raftkv/
├── cmd/server/main.go        # 节点启动：读配置、建节点、起 HTTP、等信号停机
├── cmd/cli/main.go           # put/get 客户端，自动找 leader
├── cluster.json              # 三节点静态配置示例
├── internal/
│   ├── raft/
│   │   ├── raft.go           # 核心：选举、复制、提交、apply 循环（论文图 2）
│   │   ├── clock.go          # Clock 接口 + RealClock（M3：时间抽象，模拟用虚拟时钟）
│   │   ├── log.go            # 持久状态类型 + Persister 接口 + 内存实现
│   │   ├── persist_file.go   # FilePersister：raft.log + meta 的文件实现
│   │   ├── persist_file_test.go  # 文件级测试（round-trip、CRC 截断、原子写）
│   │   ├── crash_test.go     # 真实进程崩溃测试（kill -9 后重启恢复）
│   │   └── transport.go      # HTTP + JSON 传输（M3：异步回调式）、term 校验
│   ├── sim/                  # M3 确定性模拟器：虚拟时钟、模拟网络、故障注入、不变式
│   │   └── concurrent.go     # M4 并发驱动：3 客户端并发 Put/线性化读，产出操作历史
│   ├── linearizability/      # M4 Porcupine 模型检查：KV 模型（按 key 分区）+ Check 入口
│   ├── kv/store.go           # KV 状态机：Put/Delete/Get/Read（纯逻辑，不碰网络/磁盘）
│   ├── cluster/config.go     # 静态配置解析
│   └── node/node.go          # 节点组装：Raft + KV + HTTP 服务 + put 等待逻辑
├── docs/
│   ├── 01-raft-figure2.md    # 对照论文图 2 的实现逐项说明
│   ├── 02-persistence.md     # 持久化格式、fsync 点、崩溃处理、重启重放
│   └── 03-testing.md         # M3：模拟器用法、不变式、故障矩阵、确定性说明
└── scripts/
    ├── demo.sh               # M1 + M2 全流程演示
    └── run-cluster.sh        # 起三节点集群（make run 用）
```

## 检查与测试

```bash
make check   # vet + gofmt 检查 + go test -race
make test    # go test ./... -count=1 -race（并发是本项目的命，race 必须开）
make sim     # 模拟器长跑：故障矩阵（9 场景 × 3 种子）+ 1000 步随机长跑
```

## M1 实测记录（2026-11-17，本机）

- `go test ./... -count=1 -race`：**13 个顶层测试全绿**（39 个含子测试）：
  单元测试（投票规则、日志冲突截断、commit 中位数推进、Start 权限、
  候选人收到心跳退回 follower）+ 单节点 HTTP 测试 + 三节点集成测试
  （选主 → 写入 → 杀 leader → 选新 leader → 继续写入 → `/debug/log`）。
- 三节点实测（真实三进程，`scripts/demo.sh` 与手动验证一致）：
  CLI put 成功，三节点 get 都返回写入值；杀 leader 后 **0.27 秒**选出新
  leader（要求 5 秒内），写入继续成功；`/debug/log` 能看到 term 与日志长度。
- 30 秒空跑选主次数：**0 次变化**（148 次采样，leader 稳定在同一节点同一任期）。
- 额外验证：节点被杀期间写入的数据，节点重启后能追上（空日志通过
  AppendEntries 补齐；M1 无持久化，重启后内存是空的，靠复制追上）。

## M2 实测记录（2026-11-17，本机）

- `go test ./... -count=1 -race`：**28 个顶层测试全绿**（M1 的 13 个 +
  文件级 13 个 + 真实进程崩溃测试 2 个）；`go vet` 全绿；`gofmt -l` 为空。
- 文件级测试（`internal/raft/persist_file_test.go`）：日志 round-trip
  （写 N 条 → Load 读回逐条一致）、meta 原子写、CRC 损坏 / 写一半的
  尾部 entry 被截断丢弃、votedFor 跨重启不丢、差分追加（只追加新条目，
  不重写整个文件）、截断改写走临时文件 + 改名、Save 失败后下次 Save
  自愈重写。
- 真实进程崩溃测试（`internal/raft/crash_test.go`，真实三进程、
  各用独立 `--data` 目录）：
  - 写 100 条 → 逐个 `kill -9` 再重启（先 follower 后 leader），
    重启后三个节点 `term` 与杀之前一致（只增不减）、`logLen` 都是 100、
    `lastApplied` 都是 100，100 条全能 get 到；重启后写 k100 继续成功。
  - 杀 leader 瞬间的写入：写死 leader 的地址返回连接拒绝（明确失败，
    重启后读不到）；写存活节点要么成功（重启后能读到）、要么返回
    `not_leader`（重启后读不到）——没有“返回成功但重启后丢了”或
    “返回失败但重启后出现了”的情况。
  - 测试里发现的一个 Raft 语义（不是 bug，见 `docs/02-persistence.md`
    与测试注释）：新 leader 不能用“数人头”提交旧任期日志，
    所以断言“重启后读到数据”之前必须先经当前 leader 写一条探针，
    把前缀一起提交；否则测试会在“谁当选”上抖动。
- 手动验证（真实三进程，独立 `--data` 目录，`/tmp/raftkv-manual/verify100.sh`）：
  写 100 条，三节点 `term=1`、`logLen=100`、`commitIndex=100`、
  `lastApplied=100`；杀 leader 的同时连发 10 条写，10 条都返回 OK
  （CLI 自动重试到新 leader），重启后 10 条全在；逐个 `kill -9` 重启后
  三节点 `term=2`、`logLen=111`、`lastApplied=111`，100 条逐个核对
  （300 次 get）一条不缺；重启后 put/get 全部正常，日志长度单调递增
  （100 → 111，无回退、无半条日志）。
- `scripts/demo.sh` 已扩展 M2 段：写 10 条 → kill -9 全部节点 →
  逐个重启 → 数据全在、term 与日志长度一致、重启后写入正常。

## M3 实测记录（2026-11-17，本机）

- `go test ./... -count=1 -race`：**39 个顶层测试全绿**（M1/M2 的 28 个
  一个不少 + 模拟器 11 个：虚拟时钟 4 个、网络 3 个、故障矩阵 1 个
  （27 个子测试）、随机长跑 1 个、确定性 1 个、性能 1 个）；
  `go vet` 全绿；`gofmt -l` 为空；`make check` 与 `make sim` 通过；
  `scripts/demo.sh` 仍通过（重构没改真实路径行为）。
- 故障矩阵（`TestFaultMatrix`）：9 个场景 × 种子 1/2/3 = **27/27 通过**，
  包括分区（单节点孤立、2 对 1 分裂）、崩溃（leader、follower、连续两个）、
  重启（立即、隔 5 秒）与混合场景（分区 + 杀 leader + 重启 + 写）。
- 随机故障长跑（种子 42，1000 步）：写 386（成功 194 / 失败 192）、
  分区 103 / 恢复 88、杀 142 / 重启 142，三个不变式**全程成立**，
  最终已提交日志 220 条、KV 220 条。
- 确定性（种子 42，1000 步跑两次）：最终已提交日志 220 条、
  最终 KV **完全相同**；leader 历史也完全相同（4820 次切换）——
  没有残留的非确定性。
- 性能：1000 步随机长跑 **3.0 秒**（上限 10 秒）。
- 模拟器调试时发现并修好的两个检查 bug（不是 Raft 的 bug，详见
  `docs/03-testing.md`）：不变式 (b) 的水位线必须从 `commitIndex`
  最大的节点取、比较只到各节点的 `commitIndex`；收敛检查只比
  已提交前缀（follower 的未提交旧尾巴不会马上被截掉，是标准 Raft 行为）。
- 重构时修了一个真实竞争：不调 `Run()` 的测试节点也会排
  `time.AfterFunc` 定时器，测试结束后回调与已结束的对象竞争
  （`-race` 抓到）；现在没 `Run()` 的节点不排定时器。

## M4 实测记录（2026-11-17，本机）

- `go vet ./...` 全绿；`gofmt -l .` 为空；`make check` 与 `make sim`
  通过；`scripts/demo.sh` 仍通过。
- `go test ./... -count=1 -race`：**46 个顶层测试全绿**（M3 的 39 个
  一个不少 + M4 新增 7 个：检查器 6 个 + `TestStore_ReadCommand` 1 个）。
- 模拟器 20 种子（`TestLinearizability_Sim_20Seeds`）：每种子 3 客户端
  并发 1000 次操作（Put 60% / 线性化读 40%，10 个 key，Put 值全局唯一），
  **20/20 线性一致**；每种子约 1.9 秒，总耗时 **38.7 秒**
  （测试内断言上限 5 分钟）；操作重叠率 99.9%（测试内断言 > 50%）。
  耗时大头在模拟器：单次 1000 操作的 Porcupine 检查只要 ~18ms。
- 真实集群 smoke（`TestLinearizability_RealCluster`）：真实三进程，
  3 客户端并发 198 次操作，**线性一致**，检查耗时 1.4ms。
- 反例：两个人工非线性历史（Put 返回后读不到、读到旧值）都被
  检查器正确拒绝；重叠但可线性化的历史、顺序历史都通过。
- 依赖：Porcupine `github.com/anishathalye/porcupine` **v1.3.1**
  （测试依赖，进 go.mod/go.sum；`go get` + `go mod tidy`，与 go 1.22
  无冲突）。详见 `docs/03-testing.md` 的 M4 一节。

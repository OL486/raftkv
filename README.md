# raftkv

三节点 Raft 共识算法 + 构建在它上面的 KV 存储（Go，零外部依赖，标准库
`net/http` + JSON 做传输）。

客户端写一条 Put，要经过选主、日志复制、多数派提交、按序应用到状态机，
三个节点的 KV 内容最终一致；杀掉一个节点（甚至杀掉 leader）服务不中断。

## 当前状态：M1（三节点 Raft 跑通）

能跑的：

- 选主（随机选举超时 150–300ms，心跳 50ms）
- 日志复制（AppendEntries + 一致性检查 + 冲突截断，一次回退一格）
- 提交（matchIndex 中位数，限当前任期条目）
- KV 的 Put / Get（Delete 已在状态机与命令层实现，HTTP/CLI 暂不暴露）
- `curl` 看集群状态：`GET /debug/log`

不能跑的（按规划，后面里程碑做）：

- **重启丢数据**：M1 的日志只放内存，不落盘；进程退出即丢数据。
  持久化与重启恢复是 M2 的事（存储已抽象成 `Persister` 接口，
  见 `internal/raft/log.go`，M2 换文件实现时 `raft.go` 不动）。
- 快照与日志压缩（M5）、确定性模拟器（M3）、线性一致性检查（M4）。
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

想看完整流程（含杀 leader、选新 leader）直接跑演示：

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

`--data` 在 M1 只是占位参数：接受但不用，M2 落盘时才用它。
`cluster.json` 是静态配置，三个节点写死在里面（改配置要改文件、重启集群）。

### 杀 leader

```bash
# 先看谁是 leader
for p in 8001 8002 8003; do curl -s localhost:$p/debug/log; echo; done
# 杀掉 leader 那个进程；5 秒内应选出新 leader，写入继续成功
kill <leader 的 PID>
```

## HTTP 接口

| 方法与路径 | 说明 |
|---|---|
| `POST /kv/put` `{"key","value"}` | 只有 leader 接受；不是 leader 返回 503 `{"error":"not_leader","leader":"nodeX"}`；等提交后返回 `{"ok":true,"index":N}`，3 秒没提交返回 504 |
| `GET /kv/get?key=foo` | 读本节点的本地状态机，返回 `{"key","value","found"}`。**注意：读 follower 可能读到旧数据**（本节点 `lastApplied` 落后于 leader 的 `commitIndex`）；要读最新数据必须走日志，ReadIndex 明确不做 |
| `GET /debug/log` | `{"id","term","role","logLen","commitIndex","lastApplied","leader"}`，`logLen` 是用户日志条数（不含哨兵条目） |
| `POST /raft/requestVote` | Raft 投票 RPC（JSON） |
| `POST /raft/appendEntries` | Raft 日志复制 / 心跳 RPC（JSON） |

## 客户端

```bash
raftkv-cli --config cluster.json put foo bar
raftkv-cli --config cluster.json get foo
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
│   │   ├── log.go            # 持久状态类型 + Persister 接口 + 内存实现
│   │   └── transport.go      # HTTP + JSON 传输、term 校验
│   ├── kv/store.go           # KV 状态机：Put/Delete/Get（纯逻辑，不碰网络/磁盘）
│   ├── cluster/config.go     # 静态配置解析
│   └── node/node.go          # 节点组装：Raft + KV + HTTP 服务 + put 等待逻辑
├── docs/01-raft-figure2.md   # 对照论文图 2 的实现逐项说明
└── scripts/
    ├── demo.sh               # M1 全流程演示
    └── run-cluster.sh        # 起三节点集群（make run 用）
```

## 检查与测试

```bash
make check   # vet + gofmt 检查 + go test -race
make test    # go test ./... -count=1 -race（并发是本项目的命，race 必须开）
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

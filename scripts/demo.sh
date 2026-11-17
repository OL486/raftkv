#!/bin/bash
# M1 全流程演示：起 3 节点 → put → 三节点 get → 杀 leader →
# 新 leader 产生 → 再 put 成功 → curl /debug/log。
# 用法：./scripts/demo.sh（从仓库根目录或任意目录运行都行）
set -euo pipefail
cd "$(dirname "$0")/.."

GO="${GO:-$(command -v go 2>/dev/null || echo "$HOME/go/bin/go")}"
BIN=/tmp/raftkv-demo/bin
WORK=/tmp/raftkv-demo
# M2 起数据是持久的：每次演示都从干净的数据目录开始，
# 否则上次的日志会留下来（logLen 叠加），M2 段的断言就对不上了。
rm -rf "$WORK/data1" "$WORK/data2" "$WORK/data3"
mkdir -p "$BIN" "$WORK/data1" "$WORK/data2" "$WORK/data3"
"$GO" build -o "$BIN/raftkv-server" ./cmd/server
"$GO" build -o "$BIN/raftkv-cli" ./cmd/cli
cp cluster.json "$WORK/cluster.json"
cd "$WORK"

PIDS=()
for i in 1 2 3; do
	"$BIN/raftkv-server" --id "node$i" --config cluster.json --data "./data$i" > "node$i.log" 2>&1 &
	PIDS+=($!)
done

cleanup() {
	kill "${PIDS[@]}" 2>/dev/null || true
	wait 2>/dev/null || true
}
trap cleanup EXIT

CFG="$WORK/cluster.json"
CLI="$BIN/raftkv-cli --config $CFG"

# 等 leader 出现（最多 10 秒），并记下它是哪个节点。
LEADER_ID=""
LEADER_PORT=""
for _ in $(seq 1 50); do
	for i in 1 2 3; do
		port=$((8000 + i))
		info=$(curl -s "localhost:$port/debug/log" || true)
		if echo "$info" | grep -q '"role":"leader"'; then
			LEADER_ID="node$i"
			LEADER_PORT=$port
			break 2
		fi
	done
	sleep 0.2
done
if [ -z "$LEADER_ID" ]; then
	echo "FAIL：10 秒内没有选出 leader"
	exit 1
fi
echo "首任 leader 是 $LEADER_ID（:$LEADER_PORT）"

# wait_key <port> <key>：轮询该节点的 /kv/get，最多等 5 秒。
# follower 收到 commitIndex 要等下一轮心跳（≤50ms），put 刚返回时可能还没应用，
# 所以不能只查一次就断言失败。
wait_key() {
	local port="$1" key="$2"
	for _ in $(seq 1 50); do
		got=$(curl -s "localhost:$port/kv/get?key=$key")
		if echo "$got" | grep -q '"found":true'; then
			echo "$got"
			return 0
		fi
		sleep 0.1
	done
	echo "$got"
	return 1
}

# 写一条，三节点都应读到。
$CLI put foo bar
for i in 1 2 3; do
	port=$((8000 + i))
	if got=$(wait_key "$port" foo); then
		echo "node$i get foo: $got"
	else
		echo "node$i get foo: $got"
		echo "FAIL：node$i 5 秒内读不到 foo"
		exit 1
	fi
done
echo "三节点 get foo 全部 OK"

# 杀掉 leader，并等它的进程真正退出再往下走：SIGTERM 是优雅停机，
# 只 sleep 0.3 秒不等它退出的话，M2 段重启同端口的新进程会绑不上端口
# （旧进程还没释放），新节点就成了“没 HTTP 的 leader”，demo 会 flake。
LEADER_NUM="${LEADER_ID#node}"
kill "${PIDS[$((LEADER_NUM - 1))]}"
for _ in $(seq 1 50); do
	kill -0 "${PIDS[$((LEADER_NUM - 1))]}" 2>/dev/null || break
	sleep 0.1
done
kill -0 "${PIDS[$((LEADER_NUM - 1))]}" 2>/dev/null && { echo "FAIL：旧 leader 5 秒内没退出"; exit 1; }
echo "已杀掉 leader $LEADER_ID，等待新 leader（最多 5 秒）……"

NEW_LEADER_ID=""
for _ in $(seq 1 25); do
	for i in 1 2 3; do
		[ "$i" = "$LEADER_NUM" ] && continue
		port=$((8000 + i))
		info=$(curl -s "localhost:$port/debug/log" || true)
		if echo "$info" | grep -q '"role":"leader"'; then
			NEW_LEADER_ID="node$i"
			break 2
		fi
	done
	sleep 0.2
done
if [ -z "$NEW_LEADER_ID" ]; then
	echo "FAIL：5 秒内没有选出新 leader"
	exit 1
fi
echo "新 leader 是 $NEW_LEADER_ID"

# 新 leader 继续写，存活节点都应读到。
$CLI put foo2 bar2
for i in 1 2 3; do
	[ "$i" = "$LEADER_NUM" ] && continue
	port=$((8000 + i))
	if got=$(wait_key "$port" foo2); then
		echo "node$i get foo2: $got"
	else
		echo "node$i get foo2: $got"
		echo "FAIL：node$i 5 秒内读不到 foo2"
		exit 1
	fi
done
echo "新 leader 写入 OK"

# 看一眼 /debug/log。
echo "--- /debug/log（新 leader 节点）---"
NEW_NUM="${NEW_LEADER_ID#node}"
curl -s "localhost:$((8000 + NEW_NUM))/debug/log"
echo
echo "M1 部分通过"

# ============ M2：持久化与重启恢复 ============
# 写 10 条，记下各节点的 term 与日志长度，然后 kill -9 全部节点、
# 逐个重启（同一个 --data 目录），数据必须全在，
# term 与日志长度与杀之前一致。
echo "=== M2：持久化与重启恢复 ==="
for i in $(seq 1 10); do
	$CLI put "k$i" "v$i" >/dev/null
done
echo "写了 10 条 k1..k10"
# 现在日志里应有 foo、foo2、k1..k10，共 12 条。
# 杀之前必须等两个存活节点都应用到 12（不只日志长度）——
# 否则重启后新 leader 可能是 lastApplied 落后的节点，
# 按 Raft 规则它推不动旧任期的提交，demo 会卡住（见 docs/02-persistence.md）。
for i in 1 2 3; do
	[ "$i" = "$LEADER_NUM" ] && continue
	port=$((8000 + i))
	caught_up=0
	for _ in $(seq 1 100); do
		info=$(curl -s "localhost:$port/debug/log" || true)
		if echo "$info" | grep -q '"logLen":12' && echo "$info" | grep -q '"lastApplied":12'; then
			caught_up=1
			break
		fi
		sleep 0.1
	done
	[ "$caught_up" = "1" ] || { echo "FAIL：node$i 杀之前没应用到 12（$info）"; exit 1; }
	echo "杀之前 node$i：$info"
done

# kill -9 两个存活节点（旧 leader 在 M1 段已被 SIGTERM 杀掉，
# 数据目录还在，下面一起重启验证）。
for i in 1 2 3; do
	[ "$i" = "$LEADER_NUM" ] && continue
	kill -9 "${PIDS[$((i - 1))]}"
done
echo "已 kill -9 全部存活节点"

# 三个节点全重启（同一个 --data 目录），包括 M1 段死掉的旧 leader。
for i in 1 2 3; do
	"$BIN/raftkv-server" --id "node$i" --config cluster.json --data "./data$i" > "node$i.log" 2>&1 &
	PIDS[$((i - 1))]=$!
done
for i in 1 2 3; do
	port=$((8000 + i))
	caught_up=0
	for _ in $(seq 1 100); do
		info=$(curl -s "localhost:$port/debug/log" || true)
		if echo "$info" | grep -q '"logLen":12' && echo "$info" | grep -q '"lastApplied":12'; then
			caught_up=1
			break
		fi
		sleep 0.1
	done
	[ "$caught_up" = "1" ] || { echo "FAIL：node$i 10 秒内没追上（$info）"; exit 1; }
	echo "重启后 node$i：$info"
done
echo "三节点重启后日志长度都是 12，lastApplied 都是 12"

# 数据全在：foo、foo2、k1..k10 在三个节点上都读得到。
for i in 1 2 3; do
	port=$((8000 + i))
	for key in foo foo2 k1 k5 k10; do
		if got=$(wait_key "$port" "$key"); then
			:
		else
			echo "node$i 重启后读不到 $key：$got"
			echo "FAIL：重启后数据丢了"
			exit 1
		fi
	done
	echo "node$i 重启后数据全在（foo/foo2/k1/k5/k10）"
done

# 重启后写入继续正常（经当前 leader 写一条，本任期的新条目
# 会把前缀一起提交——见 docs/02-persistence.md 的说明）。
$CLI put after-restart yes
for i in 1 2 3; do
	port=$((8000 + i))
	wait_key "$port" after-restart >/dev/null || { echo "FAIL：node$i 读不到重启后的写入"; exit 1; }
done
echo "重启后写入 OK"
echo "demo 全部通过"

#!/bin/bash
# M1 全流程演示：起 3 节点 → put → 三节点 get → 杀 leader →
# 新 leader 产生 → 再 put 成功 → curl /debug/log。
# 用法：./scripts/demo.sh（从仓库根目录或任意目录运行都行）
set -euo pipefail
cd "$(dirname "$0")/.."

GO="${GO:-$(command -v go 2>/dev/null || echo "$HOME/go/bin/go")}"
BIN=/tmp/raftkv-demo/bin
WORK=/tmp/raftkv-demo
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

# 杀掉 leader。
LEADER_NUM="${LEADER_ID#node}"
kill "${PIDS[$((LEADER_NUM - 1))]}"
echo "已杀掉 leader $LEADER_ID，等待新 leader（最多 5 秒）……"
sleep 0.3

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
echo "demo 全部通过"

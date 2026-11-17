#!/bin/bash
# 起一个三节点 raftkv 集群（前台运行，Ctrl-C 停掉全部节点）。
# 用法：make run 或 ./scripts/run-cluster.sh
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=/tmp/raftkv-bin
mkdir -p "$BIN"
GO="${GO:-$(command -v go 2>/dev/null || echo "$HOME/go/bin/go")}"
"$GO" build -o "$BIN/raftkv-server" ./cmd/server

WORK=/tmp/raftkv-run
mkdir -p "$WORK/data1" "$WORK/data2" "$WORK/data3"
cp cluster.json "$WORK/cluster.json"
cd "$WORK"

"$BIN/raftkv-server" --id node1 --config cluster.json --data ./data1 > node1.log 2>&1 &
P1=$!
"$BIN/raftkv-server" --id node2 --config cluster.json --data ./data2 > node2.log 2>&1 &
P2=$!
"$BIN/raftkv-server" --id node3 --config cluster.json --data ./data3 > node3.log 2>&1 &
P3=$!

cleanup() {
	echo "正在停掉三个节点……"
	kill $P1 $P2 $P3 2>/dev/null || true
	wait $P1 $P2 $P3 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "三节点集群已启动："
echo "  node1  127.0.0.1:8001  (PID $P1)"
echo "  node2  127.0.0.1:8002  (PID $P2)"
echo "  node3  127.0.0.1:8003  (PID $P3)"
echo "日志在 $WORK/node{1,2,3}.log；Ctrl-C 停掉全部节点。"
echo "试一试："
echo "  $BIN/raftkv-cli --config $WORK/cluster.json put foo bar"
echo "  $BIN/raftkv-cli --config $WORK/cluster.json get foo"
echo "  curl -s localhost:8001/debug/log"
wait

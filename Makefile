# raftkv 的 Makefile。
#
# Go 工具链在 ~/go/bin/go（不在默认 PATH 里），这里先找 PATH 里的 go，
# 找不到就用 ~/go/bin/go；gofmt 同理。

GO := $(shell command -v go 2>/dev/null || echo $(HOME)/go/bin/go)
GOFMT := $(shell command -v gofmt 2>/dev/null || echo $(HOME)/go/bin/gofmt)

.PHONY: check test vet fmt build run clean sim

# 默认目标：完整检查（vet + gofmt 检查 + 带 -race 的全量测试）。
check: vet fmt test

# vet：静态检查。
vet:
	$(GO) vet ./...

# fmt：gofmt 干净才算过。
fmt:
	@test -z "$$($(GOFMT) -l .)" || (echo "gofmt 发现未格式化文件："; $(GOFMT) -l .; exit 1)

# test：全量测试，必须开 -race（并发是本项目的命）。
test:
	$(GO) test ./... -count=1 -race

# sim：模拟器长跑——故障矩阵（9 个场景 × 种子 1/2/3）
# 加一次 1000 步随机故障长跑（种子 42）。确定性与性能测试
# 在 make check 的全量测试里跑。
sim:
	$(GO) test ./internal/sim/ -count=1 -race -run 'TestFaultMatrix|TestRandomFaultLong_1000Steps' -v

# build：编译两个二进制到 /tmp/raftkv-bin（不留在工作目录里）。
build:
	mkdir -p /tmp/raftkv-bin
	$(GO) build -o /tmp/raftkv-bin/raftkv-server ./cmd/server
	$(GO) build -o /tmp/raftkv-bin/raftkv-cli ./cmd/cli

# run：起一个三节点集群（前台运行，Ctrl-C 停掉全部节点）。
run: build
	./scripts/run-cluster.sh

# clean：删编译产物与演示数据。
clean:
	rm -rf /tmp/raftkv-bin /tmp/raftkv-demo /tmp/raftkv-run

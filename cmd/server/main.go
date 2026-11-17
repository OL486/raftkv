// Command raftkv-server：启动一个 Raft 节点。
//
// 用法：
//
//	raftkv-server --id node1 --config cluster.json --data ./data1
//
// --data 在 M1 只是占位参数：接受但不用（日志只放内存），M2 落盘时才用它。
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/OL486/raftkv/internal/cluster"
	"github.com/OL486/raftkv/internal/node"
)

func main() {
	id := flag.String("id", "", "本节点在 cluster.json 里的 id（如 node1）")
	configPath := flag.String("config", "cluster.json", "集群配置文件路径")
	dataDir := flag.String("data", "", "数据目录（M1 占位参数，接受但不用；M2 落盘时用）")
	flag.Parse()

	if *id == "" {
		log.Fatal("--id 必填：本节点在 cluster.json 里的 id（如 node1）")
	}
	cfg, err := cluster.Load(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	n, err := node.New(cfg, *id, *dataDir)
	if err != nil {
		log.Fatal(err)
	}
	n.Start()
	self, _ := cfg.Get(*id)
	log.Printf("节点 %s 启动，监听 %s（--data %q 在 M1 不用，重启会丢数据）", *id, self.Addr, *dataDir)

	// 等 Ctrl-C 或 kill 再优雅停机。
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("节点 %s 收到停机信号，正在停机……", *id)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := n.Stop(ctx); err != nil {
		log.Printf("节点 %s 停机时出错：%v", *id, err)
	}
}

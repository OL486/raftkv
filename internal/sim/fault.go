package sim

import (
	"fmt"
	"time"
)

// FaultDriver 是随机故障驱动器：给定种子，生成一串操作
// （分区、杀节点、重启、写请求），跑 N 步，每步检查不变式。
// 全部随机选择都从驱动器自己的 RNG 取数（种子由主种子派生），
// 所以同样的种子、同样的步数，操作序列完全相同。
type FaultDriver struct {
	sim  *Sim
	step int
	// keys 是写过的 key（写请求的 key 按步数编号，唯一）。
	keys []string
	// stats 是跑完后看的统计（测试日志里打印）。
	stats DriverStats
}

// DriverStats 是驱动器跑完后的统计。
type DriverStats struct {
	Steps      int
	Writes     int // 发起的写请求数
	WriteOK    int // 提交成功的写
	WriteFail  int // 没提交的写（集群多数派不可用时是正常结果）
	Partitions int // 分区次数
	Heals      int // 恢复次数
	Kills      int // 杀节点次数
	Restarts   int // 重启次数
}

// NewFaultDriver 建一个驱动器（sim 必须已 Start）。
func NewFaultDriver(sim *Sim) *FaultDriver {
	return &FaultDriver{sim: sim}
}

// Stats 返回当前的统计。
func (d *FaultDriver) Stats() DriverStats { return d.stats }

// Step 执行一步随机操作，再推进一小段虚拟时间、排空、检查不变式。
// 操作按权重随机选：写 40%、分区 15%、恢复 10%、杀节点 15%、重启 20%——
// 无效的操作（比如没有死节点可重启）会自动换成“推进时间”。
func (d *FaultDriver) Step() error {
	d.step++
	sim := d.sim
	rng := sim.driverRng
	roll := rng.Intn(100)
	switch {
	case roll < 40:
		// 写：key 按步数编号，保证唯一。
		key := fmt.Sprintf("k%d", d.step)
		value := fmt.Sprintf("v%d", d.step)
		d.stats.Writes++
		if _, err := sim.Put(key, value); err != nil {
			d.stats.WriteFail++
		} else {
			d.stats.WriteOK++
			d.keys = append(d.keys, key)
		}
	case roll < 55:
		// 分区：随机挑两个活着的节点，把其中一个孤立出来。
		// 活节点不足 2 个时跳过这步（死节点已不在网络里，分区没有意义）。
		alive := aliveIDs(sim)
		if len(alive) < 2 {
			break
		}
		id := alive[rng.Intn(len(alive))]
		other := alive[(rng.Intn(len(alive)-1)+indexOf(alive, id)+1)%len(alive)]
		sim.Partition(id, other)
		d.stats.Partitions++
	case roll < 65:
		// 恢复全连通。
		sim.Heal()
		d.stats.Heals++
	case roll < 80:
		// 杀节点：随机挑一个活着的。
		alive := aliveIDs(sim)
		if len(alive) == 0 {
			break
		}
		id := alive[rng.Intn(len(alive))]
		if err := sim.Kill(id); err != nil {
			return err
		}
		d.stats.Kills++
	default:
		// 重启：随机挑一个死了的。
		dead := deadIDs(sim)
		if len(dead) == 0 {
			break
		}
		id := dead[rng.Intn(len(dead))]
		if err := sim.Restart(id); err != nil {
			return err
		}
		d.stats.Restarts++
	}
	// 每步推进 50–200ms 虚拟时间（随机），再排空、检查不变式。
	sim.Advance(time.Duration(50+rng.Intn(151)) * time.Millisecond)
	if err := sim.CheckInvariants(); err != nil {
		return fmt.Errorf("第 %d 步后不变式失败：%w", d.step, err)
	}
	d.stats.Steps = d.step
	return nil
}

// Run 跑 n 步，任何一步的不变式失败都立刻返回错误。
func (d *FaultDriver) Run(n int) error {
	for i := 0; i < n; i++ {
		if err := d.Step(); err != nil {
			return err
		}
	}
	return nil
}

// Keys 返回写成功过的 key（测试核对数据用）。
func (d *FaultDriver) Keys() []string {
	return append([]string(nil), d.keys...)
}

// aliveIDs 返回活着的节点 id（按 sim.ids 的顺序）。
func aliveIDs(sim *Sim) []string {
	var out []string
	for _, id := range sim.ids {
		if sim.Alive(id) {
			out = append(out, id)
		}
	}
	return out
}

// deadIDs 返回死了的节点 id（按 sim.ids 的顺序）。
func deadIDs(sim *Sim) []string {
	var out []string
	for _, id := range sim.ids {
		if !sim.Alive(id) {
			out = append(out, id)
		}
	}
	return out
}

// indexOf 返回 id 在 ids 里的下标（找不到返回 -1）。
func indexOf(ids []string, id string) int {
	for i, x := range ids {
		if x == id {
			return i
		}
	}
	return -1
}

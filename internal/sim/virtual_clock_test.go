package sim

import (
	"testing"
	"time"
)

// 虚拟时钟的单元测试：不建 Raft 节点，只测时钟本身。
func TestVirtualClock_Order(t *testing.T) {
	c := NewVirtualClock()
	var fired []string
	c.AfterFunc(300*time.Millisecond, func() { fired = append(fired, "300") })
	c.AfterFunc(100*time.Millisecond, func() { fired = append(fired, "100") })
	c.AfterFunc(200*time.Millisecond, func() { fired = append(fired, "200") })
	// 同一时间点的两个定时器，按创建顺序触发。
	c.AfterFunc(200*time.Millisecond, func() { fired = append(fired, "200b") })

	c.Advance(150 * time.Millisecond)
	if len(fired) != 1 || fired[0] != "100" {
		t.Fatalf("Advance(150ms) 后应只触发 100ms 的定时器，实际 %v", fired)
	}
	c.Advance(100 * time.Millisecond)
	if len(fired) != 3 || fired[1] != "200" || fired[2] != "200b" {
		t.Fatalf("Advance 到 250ms 后应触发 200/200b，实际 %v", fired)
	}
	c.Advance(100 * time.Millisecond)
	if len(fired) != 4 || fired[3] != "300" {
		t.Fatalf("Advance 到 350ms 后应触发全部，实际 %v", fired)
	}
	if got := c.Now(); got != 350*time.Millisecond {
		t.Fatalf("Now() 应为 350ms，实际 %v", got)
	}
}

func TestVirtualClock_Stop(t *testing.T) {
	c := NewVirtualClock()
	fired := false
	tm := c.AfterFunc(100*time.Millisecond, func() { fired = true })
	if !tm.Stop() {
		t.Fatal("第一次 Stop 应返回 true")
	}
	if tm.Stop() {
		t.Fatal("第二次 Stop 应返回 false")
	}
	c.Advance(500 * time.Millisecond)
	if fired {
		t.Fatal("已停止的定时器不应触发")
	}
	// 0 延时的定时器在本次 Advance 里执行。
	fired = false
	c.AfterFunc(0, func() { fired = true })
	c.Advance(1 * time.Millisecond)
	if !fired {
		t.Fatal("0 延时定时器应在本次 Advance 里执行")
	}
}

func TestVirtualClock_Ticker(t *testing.T) {
	c := NewVirtualClock()
	tk := c.NewTicker(100 * time.Millisecond)
	drain := func() int {
		n := 0
		for {
			select {
			case <-tk.C():
				n++
			default:
				return n
			}
		}
	}
	// 每推进 100ms 读一次：每次恰好 1 个 tick。
	// （如果 Advance 期间不读，channel 满了之后的 tick 会被丢掉——
	// 与 time.Ticker 的语义相同，见 NewTicker 的文档。）
	for i := 0; i < 3; i++ {
		c.Advance(100 * time.Millisecond)
		if got := drain(); got != 1 {
			t.Fatalf("第 %d 个 100ms 内 ticker 应触发 1 次，实际 %d", i+1, got)
		}
	}
	tk.Stop()
	c.Advance(500 * time.Millisecond)
	if got := drain(); got != 0 {
		t.Fatalf("Stop 后 ticker 不应再触发，实际 %d", got)
	}
}

// 回调里再排定时器：新事件的到期时间也在本次 Advance 的目标内，
// 应在同一次 Advance 里执行（这是驱动逻辑依赖的行为）。
func TestVirtualClock_NestedAfterFunc(t *testing.T) {
	c := NewVirtualClock()
	var fired []string
	c.AfterFunc(100*time.Millisecond, func() {
		fired = append(fired, "a")
		c.AfterFunc(50*time.Millisecond, func() { fired = append(fired, "b") })
	})
	c.Advance(200 * time.Millisecond)
	if len(fired) != 2 || fired[0] != "a" || fired[1] != "b" {
		t.Fatalf("嵌套定时器应都在本次 Advance 里执行，实际 %v", fired)
	}
}

package raft

import "time"

// Clock 是 Raft 核心对“时间”的依赖抽象。Raft 只调这个接口，
// 不直接碰 time 包的定时器——这样模拟器（M3）才能把时间换成
// 虚拟时钟（internal/sim.VirtualClock），让选举超时不再依赖真实时间。
//
// 两个实现：
//   - RealClock：time 包的直接封装，生产路径用它；
//   - VirtualClock（internal/sim）：只有 Advance(d) 推进时间，
//     到期的定时器按（到期时间，创建顺序）同步触发。
type Clock interface {
	// Now 返回从时钟起点开始经过的时间。真实时钟是进程启动以来的
	// 流逝时间；虚拟时钟是 Advance 累计推进的时间。
	Now() time.Duration
	// AfterFunc 在 d 之后调用 f 一次，返回可停止的 Timer。
	// 真实实现就是 time.AfterFunc；虚拟实现把回调排进事件队列。
	AfterFunc(d time.Duration, f func()) Timer
	// NewTicker 返回一个每 d 触发一次的 Ticker（只用于 apply 循环
	// 的兜底检查；选举与复制走 AfterFunc 回调链，不用 ticker）。
	NewTicker(d time.Duration) Ticker
}

// Timer 是 Clock.AfterFunc 返回的单次定时器。
type Timer interface {
	// Stop 阻止回调执行。回调已执行或已被停止时返回 false。
	Stop() bool
}

// Ticker 是 Clock.NewTicker 返回的周期定时器。
// C 里的每个值代表一次触发（无缓冲积压语义与 time.Ticker 相同：
// 接收方来不及取时，触发会被丢掉一个，而不是无限攒着）。
type Ticker interface {
	C() <-chan struct{}
	Stop()
}

// RealClock 是 Clock 的真实实现：直接包 time 包。
type RealClock struct {
	start time.Time
}

// NewRealClock 建一个真实时钟（起点记为调用时刻）。
func NewRealClock() *RealClock {
	return &RealClock{start: time.Now()}
}

// Now 返回自时钟创建以来经过的真实时间。
func (c *RealClock) Now() time.Duration {
	return time.Since(c.start)
}

// AfterFunc 用 time.AfterFunc 实现。
func (c *RealClock) AfterFunc(d time.Duration, f func()) Timer {
	return &realTimer{t: time.AfterFunc(d, f)}
}

// NewTicker 用 time.NewTicker 实现（外加一个转发 goroutine，
// 把 time.Time 的 tick 转成 struct{}）。
func (c *RealClock) NewTicker(d time.Duration) Ticker {
	t := &realTicker{
		ticker: time.NewTicker(d),
		ch:     make(chan struct{}, 1),
		done:   make(chan struct{}),
	}
	go func() {
		for {
			select {
			case <-t.ticker.C:
				select {
				case t.ch <- struct{}{}:
				default:
					// 接收方来不及取：丢掉这次触发（与 time.Ticker 一致）。
				}
			case <-t.done:
				return
			}
		}
	}()
	return t
}

// realTimer 是 time.Timer 的 Timer 适配。
type realTimer struct {
	t *time.Timer
}

// Stop 阻止回调执行（time.Timer.Stop 的语义）。
func (t *realTimer) Stop() bool {
	return t.t.Stop()
}

// realTicker 是 time.Ticker 的 Ticker 适配。
type realTicker struct {
	ticker *time.Ticker
	ch     chan struct{}
	done   chan struct{}
}

// C 返回触发 channel。
func (t *realTicker) C() <-chan struct{} { return t.ch }

// Stop 停掉 ticker 与转发 goroutine。
func (t *realTicker) Stop() {
	t.ticker.Stop()
	close(t.done)
}

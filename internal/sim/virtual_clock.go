package sim

import (
	"sort"
	"sync"
	"time"

	"github.com/OL486/raftkv/internal/raft"
)

// clockEvent 是事件队列里的一项：到 at 时执行 fn。
// stopped 表示已被 Stop 掉（Advance 跳过它）；fired 表示已执行过。
// 读写都受 VirtualClock.mu 保护。
type clockEvent struct {
	at      time.Duration
	seq     int64
	fn      func()
	stopped bool
	fired   bool
}

// virtualTimer 是 VirtualClock.AfterFunc 返回的 Timer 句柄。
type virtualTimer struct {
	clock *VirtualClock
	ev    *clockEvent
}

// Stop 阻止回调执行。回调已执行或已被停止时返回 false。
func (t *virtualTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if t.ev.fired || t.ev.stopped {
		return false
	}
	t.ev.stopped = true
	return true
}

// virtualTicker 是 VirtualClock.NewTicker 返回的 Ticker。
// stopped 与 timer 受自己的 mu 保护：fire() 在 Advance 的 goroutine 里跑，
// Stop() 在 apply 循环的 goroutine 里跑（循环退出时调），两者会并发。
type virtualTicker struct {
	clock    *VirtualClock
	interval time.Duration
	ch       chan struct{}
	mu       sync.Mutex
	timer    raft.Timer
	stopped  bool
}

// C 返回触发 channel。
func (t *virtualTicker) C() <-chan struct{} { return t.ch }

// Stop 停掉 ticker（不再触发）。
func (t *virtualTicker) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopped = true
	if t.timer != nil {
		t.timer.Stop()
		t.timer = nil
	}
}

// fire 是 ticker 的一次触发：非阻塞地往 channel 里塞一个值
// （接收方来不及取就丢掉，与 time.Ticker 的语义相同），
// 再排好下一次触发（按上一次的到期时间 + interval，不漂移）。
// 在 Advance 的调用栈里执行。
func (t *virtualTicker) fire() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return
	}
	select {
	case t.ch <- struct{}{}:
	default:
	}
	t.timer = t.clock.AfterFunc(t.interval, t.fire)
}

// VirtualClock 是 raft.Clock 的虚拟实现：时间只有 Advance(d)
// 调用时才往前走。内部是一条统一事件队列——定时器回调与网络
// 消息投递都排进这条队列，按（到期时间，创建顺序）执行。
// 因此同一个 Advance 序列产生完全相同的事件执行顺序。
//
// 并发：事件队列受 mu 保护，任何 goroutine 都可以调 Now/AfterFunc/
// NewTicker/Stop；Advance 在执行回调时不持锁（回调里调 AfterFunc
// 是安全的，不会死锁）。决策本身只在 Advance 的调用栈里发生，
// 所以锁不影响确定性——它只保护队列结构，不参与事件排序。
type VirtualClock struct {
	mu     sync.Mutex
	now    time.Duration
	seq    int64
	events []*clockEvent
}

// NewVirtualClock 建一个从 0 开始的虚拟时钟。
func NewVirtualClock() *VirtualClock {
	return &VirtualClock{}
}

// Now 返回虚拟时间（自创建以来 Advance 累计推进的时间）。
func (c *VirtualClock) Now() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// AfterFunc 在 d 之后调用 f 一次，返回可停止的 Timer。
// d 为 0 时回调排在当前时间，会在本次 Advance 里执行。
func (c *VirtualClock) AfterFunc(d time.Duration, f func()) raft.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	ev := &clockEvent{at: c.now + d, seq: c.seq, fn: f}
	c.seq++
	c.insertLocked(ev)
	return &virtualTimer{clock: c, ev: ev}
}

// NewTicker 返回一个每 d 触发一次的 Ticker。
func (c *VirtualClock) NewTicker(d time.Duration) raft.Ticker {
	t := &virtualTicker{clock: c, interval: d, ch: make(chan struct{}, 1)}
	t.mu.Lock()
	t.timer = c.AfterFunc(d, t.fire)
	t.mu.Unlock()
	return t
}

// Post 在 delay 之后执行 f 一次（网络层用它排消息投递，
// 与 AfterFunc 相同，只是不需要返回 Timer 句柄）。
func (c *VirtualClock) Post(delay time.Duration, f func()) {
	c.AfterFunc(delay, f)
}

// Advance 把虚拟时间推进 d，并按顺序执行所有到期的事件。
// 到期的定义是事件时间 <= 推进后的时间；回调里新排的、
// 到期时间也 <= 目标时间的事件，会在同一次 Advance 里执行。
// 执行顺序是（到期时间，创建顺序），与创建时的 goroutine 调度无关。
// 执行回调时不持锁，所以回调里可以安全地调 Now/AfterFunc。
func (c *VirtualClock) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.now + d
	c.mu.Unlock()
	for {
		ev := c.nextDue(target)
		if ev == nil {
			break
		}
		c.mu.Lock()
		c.now = ev.at
		ev.fired = true
		c.mu.Unlock()
		ev.fn()
	}
	c.mu.Lock()
	c.now = target
	c.mu.Unlock()
}

// Pending 返回队列里还没执行、也没被停止的事件数（测试与调试用）。
func (c *VirtualClock) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, ev := range c.events {
		if !ev.stopped && !ev.fired {
			n++
		}
	}
	return n
}

// insertLocked 把事件按（时间，序号）插进有序队列。调用方必须持 mu。
func (c *VirtualClock) insertLocked(ev *clockEvent) {
	i := sort.Search(len(c.events), func(i int) bool {
		e := c.events[i]
		return e.at > ev.at || (e.at == ev.at && e.seq > ev.seq)
	})
	c.events = append(c.events, nil)
	copy(c.events[i+1:], c.events[i:])
	c.events[i] = ev
}

// nextDue 取出最早的、到期时间 <= target 且未被停止的事件
// （同时把它从队列里删掉）；没有时返回 nil。
func (c *VirtualClock) nextDue(target time.Duration) *clockEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.events) > 0 {
		ev := c.events[0]
		c.events = c.events[1:]
		if ev.stopped || ev.fired {
			continue
		}
		if ev.at > target {
			// 还没到期：放回去，等下次 Advance。
			c.events = append([]*clockEvent{ev}, c.events...)
			return nil
		}
		return ev
	}
	return nil
}

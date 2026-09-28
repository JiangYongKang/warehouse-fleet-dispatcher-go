package dispatcher

import "sync/atomic"

// Clock 是确定性的逻辑时钟：所有调度时间均以 tick 表示，
// 重放同一批操作会得到完全一致的时间窗，避免墙钟带来的不确定性。
type Clock struct {
	tick atomic.Int64
}

// NewClock 创建逻辑时钟。
func NewClock() *Clock { return &Clock{} }

// Now 返回当前 tick。
func (c *Clock) Now() int64 { return c.tick.Load() }

// Advance 将时钟推进到 t（不允许回退），返回推进后的 tick。
func (c *Clock) Advance(t int64) int64 {
	for {
		cur := c.tick.Load()
		if t <= cur {
			return cur
		}
		if c.tick.CompareAndSwap(cur, t) {
			return t
		}
	}
}

// Tick 时钟前进 1。
func (c *Clock) Tick() int64 { return c.tick.Add(1) }

// load/set 供快照恢复使用。
func (c *Clock) load(t int64) { c.tick.Store(t) }

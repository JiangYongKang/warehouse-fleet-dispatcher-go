package domain

// 调度相关的确定性参数（逻辑时间 tick）。
const (
	MaxAttempts         = 3
	DeadlockBackoff     = 3
	AgingPerTick        = 1 // 每等待 1 tick 的老化增益
	StarvationCap       = 10000
	NodeDwellTicks      = 1
	MaxKShortest        = 6
	HeartbeatTimeout    = 5
	DefaultHeartbeatGap = 2
	// BlockedAgingBonus 处于 blocked 状态的等待者每 tick 的额外老化，
	// 保证持续等待者最终能超过新到的高优先级任务。
	BlockedAgingBonus = 1
)

// EffectivePriority 计算有效优先级：基础优先级 + 等待老化，
// 保证低优先级任务等待足够久后不会被长期饿死。
func EffectivePriority(base int, readySince, now int64) int {
	wait := now - readySince
	if wait < 0 {
		wait = 0
	}
	eff := base + int(wait)*AgingPerTick
	if eff > base+StarvationCap {
		eff = base + StarvationCap
	}
	return eff
}

// KeyNode / KeyEdge 生成预约资源键。
func KeyNode(id string) string { return "node:" + id }

// KeyEdge 有向边资源键。
func KeyEdge(u, v string) string { return "edge:" + u + ":" + v }

// KeyLane 不可分割单车道（双向互斥）资源键：内部对 u,v 排序。
func KeyLane(u, v string) string {
	if u > v {
		u, v = v, u
	}
	return "lane:" + u + ":" + v
}

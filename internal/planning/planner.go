package planning

import (
	"sort"

	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/domain"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/network"
)

// Planner 基于路网与当前状态，计算路径与首段可获取性，并检测死锁。
type Planner struct {
	graph *network.Graph
}

func NewPlanner(g *network.Graph) *Planner {
	return &Planner{graph: g}
}

// Graph 返回底层路网。
func (p *Planner) Graph() *network.Graph { return p.graph }

// EdgeResources 返回通过有向边 u->v 需要占用的全部资源（边 + 可能的单车道）。
func (p *Planner) EdgeResources(u, v string) []string {
	keys := []string{domain.KeyEdge(u, v)}
	if p.graph.IsLane(u, v) {
		keys = append(keys, domain.KeyLane(u, v))
	}
	return keys
}

// WindowsOverlap 半开时间窗重叠判定（端点相接不算重叠）。
func WindowsOverlap(a0, a1, b0, b1 int64) bool {
	return a0 < b1 && b0 < a1
}

// EffPrio 计算任务在 tick 时刻的有效优先级（含老化），供调度器复用。
func EffPrio(t *domain.Task, now int64) int {
	ready := t.SubmittedAt
	if t.Status == domain.TaskPreempted && t.BackoffUntil > ready {
		ready = t.BackoffUntil
	}
	wait := now - ready
	bonus := int64(0)
	// 已在等待中的任务获得额外老化（由调度器在 WaitingSince 之外体现）。
	_ = bonus
	if wait < 0 {
		wait = 0
	}
	eff := t.Priority + int(wait)*domain.AgingPerTick
	if cap := t.Priority + domain.StarvationCap; eff > cap {
		eff = cap
	}
	return eff
}

// ChoosePath 从 from 到 to 枚举 K 最短路，返回首段在 tick 时刻“可获取”
// （目标节点与边资源均无占用者）的路径；preempt 时允许越过严格更低有效
// 优先级的占用者。全部不可获取时返回 nil。
func (p *Planner) ChoosePath(st *domain.State, from, to, robotID string, tick int64, myPrio int, preempt bool) (path []string, victims []string, blocked bool) {
	paths := p.graph.KShortest(from, to, domain.MaxKShortest)
	if len(paths) == 0 {
		return nil, nil, false
	}
	victimSet := map[string]bool{}
	for idx, path := range paths {
		u, v := path[0], path[1]
		cost, ok := p.graph.EdgeCost(u, v)
		if !ok {
			continue
		}
		// 边窗 [tick, tick+cost)，目标节点窗 [tick+cost, +inf)
		resources := p.EdgeResources(u, v)
		resources = append(resources, domain.KeyNode(v))
		feasible := true
		blockedHere := false
		for _, key := range resources {
			end := tick + int64(cost)
			if key == domain.KeyNode(v) {
				end = domain.SentinelForever
			}
			for _, r := range st.Overlapping(key, tick, maxTick(tick+1, end), robotID) {
				hprio := holderPrio(st, r.RobotID, tick)
				if preempt && hprio < myPrio && r.RobotID != robotID {
					victimSet[r.RobotID] = true
					continue
				}
				feasible = false
				blockedHere = true
			}
		}
		if feasible {
			_ = idx
			vs := make([]string, 0, len(victimSet))
			for id := range victimSet {
				vs = append(vs, id)
			}
			sort.Strings(vs)
			return path, vs, false
		}
		_ = blockedHere
	}
	vs := make([]string, 0, len(victimSet))
	for id := range victimSet {
		vs = append(vs, id)
	}
	sort.Strings(vs)
	return nil, vs, true
}

func maxTick(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func holderPrio(st *domain.State, robotID string, tick int64) int {
	r := st.Robots[robotID]
	if r == nil || r.CurrentTask == "" {
		return -1 << 30 // 停车占用者视为最低
	}
	t := st.Tasks[r.CurrentTask]
	if t == nil {
		return -1 << 30
	}
	return EffPrio(t, tick)
}

// BuildWaitFor 在 tick 时刻根据“移动到下一段的首跳需求 vs 当前占用者”
// 构建 wait-for 图：edges[A] 包含 B 表示机器人 A 正在等待机器人 B 释放资源。
func (p *Planner) BuildWaitFor(st *domain.State, tick int64) map[string]map[string]string {
	edges := map[string]map[string]string{}
	for robotID, r := range st.Robots {
		if r.Status != domain.RobotBusy || r.CurrentTask == "" {
			continue
		}
		plan := st.Plans[r.CurrentTask]
		if plan == nil || plan.State != domain.PlanAtNode {
			continue
		}
		if plan.Idx+1 >= len(plan.Path) {
			continue
		}
		task := st.Tasks[r.CurrentTask]
		if task != nil && task.Status == domain.TaskPreempted {
			continue
		}
		u, v := plan.Path[plan.Idx], plan.Path[plan.Idx+1]
		waits := map[string]string{}
		for _, key := range append(p.EdgeResources(u, v), domain.KeyNode(v)) {
			if h, ok := st.HolderAt(key, tick, robotID); ok {
				waits[h.RobotID] = key
			}
		}
		if len(waits) > 0 {
			edges[robotID] = waits
		}
	}
	return edges
}

// Cycle wait-for 图上的一个环。
type Cycle struct {
	Robots []string
}

// DetectCycles 在有向 wait-for 图中找出全部简单环（DFS，结果确定性排序）。
func DetectCycles(edges map[string]map[string]string) []Cycle {
	var cycles []Cycle
	seenCycle := map[string]bool{}

	nodes := make([]string, 0, len(edges))
	for n := range edges {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)

	var dfs func(start, cur string, stack []string, onStack map[string]bool)
	dfs = func(start, cur string, stack []string, onStack map[string]bool) {
		nexts := edges[cur]
		keys := make([]string, 0, len(nexts))
		for k := range nexts {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, n := range keys {
			if n == start && len(stack) >= 1 {
				cyc := append(append([]string(nil), stack...), start)
				key := cycleKey(cyc)
				if !seenCycle[key] {
					seenCycle[key] = true
					cycles = append(cycles, Cycle{Robots: cyc[:len(cyc)-1]})
				}
				continue
			}
			if onStack[n] {
				continue
			}
			onStack[n] = true
			dfs(start, n, append(stack, n), onStack)
			onStack[n] = false
		}
	}
	for _, n := range nodes {
		dfs(n, n, []string{n}, map[string]bool{n: true})
	}
	sort.Slice(cycles, func(i, j int) bool { return cycleKey0(cycles[i].Robots) < cycleKey0(cycles[j].Robots) })
	return cycles
}

func cycleKey0(rs []string) string {
	sorted := append([]string(nil), rs...)
	sort.Strings(sorted)
	return joinSorted(sorted)
}

func cycleKey(cyc []string) string {
	return cycleKey0(cyc[:len(cyc)-1])
}

func joinSorted(rs []string) string {
	out := ""
	for i, r := range rs {
		if i > 0 {
			out += ","
		}
		out += r
	}
	return out
}

// DetectDeadlock 返回当前 wait-for 图中的全部环。
func (p *Planner) DetectDeadlock(st *domain.State, tick int64) []Cycle {
	return DetectCycles(p.BuildWaitFor(st, tick))
}

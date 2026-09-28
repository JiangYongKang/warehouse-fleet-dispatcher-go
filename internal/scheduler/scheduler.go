package scheduler

import (
	"sort"

	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/domain"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/planning"
)

// TaskTimeoutTicks 单个任务自开始执行起的最大允许时长，超时按失败回收重派。
const TaskTimeoutTicks = 200

// Dispatcher 负责任务分派、冲突消解、死锁打破与资源回收。
// 所有方法都假定调用方（Kernel）已持有串行锁。
type Dispatcher struct {
	planner *planning.Planner
}

func New(planner *planning.Planner) *Dispatcher {
	return &Dispatcher{planner: planner}
}

// Planner 暴露规划器（内核构造用）。
func (d *Dispatcher) Planner() *planning.Planner { return d.planner }

// schedPrio 调度优先级：基础优先级 + 提交后老化 + 阻塞等待额外老化。
// 数值越大越优先；等待者每 tick 多计 BlockedAgingBonus，保证不会被长期饿死。
func schedPrio(t *domain.Task, p *domain.Plan, now int64) int {
	eff := planning.EffPrio(t, now)
	if p != nil && p.State == domain.PlanAtNode && p.WaitingSince > 0 && now >= p.WaitingSince {
		eff += int(now-p.WaitingSince) * domain.BlockedAgingBonus
	}
	return eff
}

// AdvanceOne 推进一个逻辑 tick：
//  1. 心跳超时掉线回收；2. 任务执行超时回收；3. 清理过期窗；
//  4. 到达处理；5. 分派新任务；6. 逐段获取（等待/改道/抢占）；
//  7. 检测并打破死锁（必要时重复 6-7）。
func (d *Dispatcher) AdvanceOne(st *domain.State) {
	tick := st.Clock
	d.markOffline(st, tick)
	d.markTaskTimeout(st, tick)
	st.PrunePast(tick)
	d.resumeYielded(st, tick)
	d.processArrivals(st, tick)
	d.ScheduleOnce(st, tick)

	for guard := 0; guard < len(st.Robots)+2; guard++ {
		victims := d.BreakDeadlocks(st, tick)
		if len(victims) > 0 {
			// 让行退避到下一 tick 恢复；本轮先重新尝试其余机器人。
			continue
		}
		d.acquireAll(st, tick)
		if len(d.BreakDeadlocks(st, tick)) == 0 {
			break
		}
	}
}

// ---- 掉线与超时 ----

// resumeYielded 处理让行任务退避结束：重新建立从当前位置出发的计划，
// 并重新占用当前节点，之后由正常逐段获取流程继续执行。
func (d *Dispatcher) resumeYielded(st *domain.State, tick int64) {
	ids := make([]string, 0)
	for id, t := range st.Tasks {
		if t.Status == domain.TaskPreempted && t.BackoffUntil <= tick && t.AssignedTo != "" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		t := st.Tasks[id]
		r := st.Robots[t.AssignedTo]
		if r == nil || r.Status == domain.RobotOffline {
			continue
		}
		// 恢复时若已位于取货点，则视为已取货。
		if !t.PickedUp && r.Node == t.From {
			t.PickedUp = true
		}
		t.Status = domain.TaskAssigned
		t.BackoffUntil = 0
		r.Status = domain.RobotBusy
		r.CurrentTask = t.ID
		if t.StartedAt == 0 {
			t.StartedAt = tick
			t.Attempts++
		}
		path := d.legPath(t, r.Node)
		if path == nil {
			continue
		}
		// 已取货且当前节点就是终点：直接等待结果上报。
		if t.PickedUp && r.Node == t.To {
			r.Status = domain.RobotExecuting
			st.Plans[t.ID] = &domain.Plan{
				TaskID: t.ID, RobotID: r.ID, Path: []string{r.Node},
				Idx: 0, State: domain.PlanAtNode, EnteredAt: tick, ReleaseAt: tick,
			}
			st.AddDecision("yield_resume_zero", "robot="+r.ID+" task="+t.ID+" tick="+itoa(tick))
			continue
		}
		st.Plans[t.ID] = &domain.Plan{
			TaskID: t.ID, RobotID: r.ID,
			Path: path, Idx: indexOf(path, r.Node), State: domain.PlanAtNode,
			EnteredAt: tick, Mode: domain.PlanReroute,
		}
		st.Reservations = append(st.Reservations, domain.Reservation{
			RobotID: r.ID, TaskID: t.ID,
			Resource: domain.KeyNode(r.Node), Start: tick, End: domain.SentinelForever,
		})
		st.AddDecision("yield_resume", "robot="+r.ID+" task="+t.ID+
			" from="+r.Node+" picked="+boolStr(t.PickedUp)+
			" path="+joinPath(path)+" tick="+itoa(tick))
	}
}

func (d *Dispatcher) markOffline(st *domain.State, tick int64) {
	ids := make([]string, 0)
	for id, r := range st.Robots {
		if r.Status != domain.RobotOffline && tick-r.LastBeat > domain.HeartbeatTimeout {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		taskID := d.ReclaimRobot(st, id, tick, domain.CodeOffline, false)
		r := st.Robots[id]
		r.Status = domain.RobotOffline
		st.AddDecision("robot_offline",
			"robot="+id+" last_beat="+itoa(r.LastBeat)+" tick="+itoa(tick)+
				" reclaimed_task="+nz(taskID))
	}
}

func (d *Dispatcher) markTaskTimeout(st *domain.State, tick int64) {
	ids := make([]string, 0)
	for id, r := range st.Robots {
		if !r.Status.IsBusy() || r.CurrentTask == "" {
			continue
		}
		t := st.Tasks[r.CurrentTask]
		if t == nil || t.StartedAt == 0 {
			continue
		}
		if tick-t.StartedAt > TaskTimeoutTicks {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		taskID := d.ReclaimRobot(st, id, tick, domain.CodeTaskTimeout, true)
		st.AddDecision("task_timeout", "robot="+id+" task="+nz(taskID)+" tick="+itoa(tick))
	}
}

// ---- 到达 ----

func (d *Dispatcher) processArrivals(st *domain.State, tick int64) {
	type arr struct {
		robotID string
		taskID  string
		node    string
	}
	var arrs []arr
	for taskID, p := range st.Plans {
		if p.State != domain.PlanMoving || p.EdgeUntil != tick {
			continue
		}
		arrs = append(arrs, arr{p.RobotID, taskID, p.Path[p.Idx+1]})
	}
	sort.Slice(arrs, func(i, j int) bool { return arrs[i].robotID < arrs[j].robotID })
	for _, a := range arrs {
		r := st.Robots[a.robotID]
		p := st.Plans[a.taskID]
		t := st.Tasks[a.taskID]
		r.Node = a.node
		p.Idx++
		p.State = domain.PlanAtNode
		p.EnteredAt = tick
		p.WaitingSince = 0

		// 到达取货点（尚未取货）：打标，并重建送货段。
		if t != nil && !t.PickedUp && a.node == t.From {
			t.PickedUp = true
			st.AddDecision("pickup", "robot="+a.robotID+" task="+a.taskID+
				" node="+a.node+" tick="+itoa(tick))
			if a.node != t.To {
				leg := d.legPath(t, a.node)
				if leg != nil {
					p.Path = leg
					p.Idx = indexOf(leg, a.node)
					if p.Idx < 0 {
						p.Idx = 0
					}
					if p.Idx+1 < len(p.Path) {
						continue // 送货段还有多跳，后续 tick 继续移动
					}
				}
			}
		}

		// 路径走完：仅当已取货且位于终点才合法进入 executing。
		if p.Idx+1 >= len(p.Path) {
			if t != nil && t.PickedUp && a.node == t.To {
				r.Status = domain.RobotExecuting
				p.ReleaseAt = tick
				st.AddDecision("arrival", "robot="+a.robotID+" task="+a.taskID+
					" node="+a.node+" status=executing tick="+itoa(tick))
			} else {
				st.AddDecision("arrival_inconsistent", "robot="+a.robotID+
					" task="+a.taskID+" node="+a.node+
					" picked="+boolStr(t != nil && t.PickedUp)+" tick="+itoa(tick))
			}
		}
	}
}

// ---- 分派 ----

// ScheduleOnce 执行一轮分派：按有效优先级处理 pending 任务，派给满足条件的
// 空闲机器人（在线、位于任务起点、能力满足、依赖已完成）。
func (d *Dispatcher) ScheduleOnce(st *domain.State, tick int64) []string {
	var assigned []string
	tasks := pendingOrder(st, tick)
	for _, t := range tasks {
		// 依赖检查
		depState, code := depsStatus(st, t)
		if depState == depBlocked {
			st.AddDecision("dispatch_reject",
				"task="+t.ID+" reason="+string(code)+" tick="+itoa(tick))
			if code == domain.CodeDependencyFailed {
				d.failTask(st, t, code, tick)
			}
			continue
		}
		if depState == depWaiting {
			continue
		}
		if t.BackoffUntil > tick {
			continue
		}
		robot := d.pickRobot(st, t)
		if robot == nil {
			st.AddDecision("dispatch_wait",
				"task="+t.ID+" prio="+itoa(int64(t.Priority))+" eff="+itoa(int64(planning.EffPrio(t, tick)))+
					" from="+t.From+" reason=no_eligible_robot tick="+itoa(tick))
			continue
		}
		// 机器人已在终点（无论货物在哪都无需移动）：零里程直接进入 executing。
		if robot.Node == t.To {
			d.assignZeroLength(st, t, robot, tick)
			assigned = append(assigned, t.ID)
			continue
		}
		d.assign(st, t, robot, tick)
		assigned = append(assigned, t.ID)
	}
	return assigned
}

const (
	depReady   = 0
	depWaiting = 1
	depBlocked = 2
)

func depsStatus(st *domain.State, t *domain.Task) (int, domain.Code) {
	for _, dep := range t.Deps {
		dt := st.Tasks[dep]
		if dt == nil {
			return depBlocked, domain.CodeNotFound
		}
		if dt.Status == domain.TaskFailed {
			return depBlocked, domain.CodeDependencyFailed
		}
		if dt.Status != domain.TaskCompleted {
			return depWaiting, domain.CodeDependencyPending
		}
	}
	return depReady, ""
}

func (d *Dispatcher) pickRobot(st *domain.State, t *domain.Task) *domain.Robot {
	type cand struct {
		r    *domain.Robot
		cost int
	}
	var candidates []cand
	for _, r := range st.Robots {
		if r.Status != domain.RobotIdle {
			continue
		}
		if !capsSatisfy(r.Caps, t.Requires) {
			continue
		}
		// 机器人先从当前位置空驶到取货点 From，再载货前往 To；
		// 选总代价最小者（代价相同取 ID 最小）。
		leg1, c1 := d.planner.Graph().Shortest(r.Node, t.From)
		leg2, c2 := d.planner.Graph().Shortest(t.From, t.To)
		if leg1 == nil || leg2 == nil {
			continue
		}
		_ = c1
		_ = c2
		candidates = append(candidates, cand{r, pathCostOf(d.planner.Graph(), leg1) + pathCostOf(d.planner.Graph(), leg2)})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].cost != candidates[j].cost {
			return candidates[i].cost < candidates[j].cost
		}
		return candidates[i].r.ID < candidates[j].r.ID
	})
	if len(candidates) == 0 {
		return nil
	}
	return candidates[0].r
}

// pathCostOf 计算路径总边代价（不可达边返回极大值）。
func pathCostOf(g interface {
	EdgeCost(u, v string) (int, bool)
}, path []string) int {
	total := 0
	for i := 0; i+1 < len(path); i++ {
		c, ok := g.EdgeCost(path[i], path[i+1])
		if !ok {
			return 1 << 30
		}
		total += c
	}
	return total
}

// legTarget 返回当前子目标：已取货则前往终点 To，否则先去取货点 From。
func legTarget(t *domain.Task) string {
	if t.PickedUp {
		return t.To
	}
	return t.From
}

// legPath 返回从 robotNode 到当前子目标的最短节点路径。
func (d *Dispatcher) legPath(t *domain.Task, robotNode string) []string {
	p, _ := d.planner.Graph().Shortest(robotNode, legTarget(t))
	return p
}

func capsSatisfy(caps, requires []string) bool {
	set := map[string]bool{}
	for _, c := range caps {
		set[c] = true
	}
	for _, q := range requires {
		if !set[q] {
			return false
		}
	}
	return true
}

// assignZeroLength 处理零里程任务：机器人已位于取/送货点，直接等待结果上报。
func (d *Dispatcher) assignZeroLength(st *domain.State, t *domain.Task, r *domain.Robot, tick int64) {
	t.Status = domain.TaskAssigned
	t.AssignedTo = r.ID
	t.PickedUp = true
	if t.StartedAt == 0 {
		t.Attempts++
		t.StartedAt = tick
	}
	r.Status = domain.RobotExecuting
	r.CurrentTask = t.ID
	st.Plans[t.ID] = &domain.Plan{
		TaskID: t.ID, RobotID: r.ID,
		Path: []string{r.Node}, Idx: 0, State: domain.PlanAtNode,
		EnteredAt: tick, ReleaseAt: tick, Mode: domain.PlanDirect,
	}
	st.AddDecision("dispatch_assign_zero",
		"task="+t.ID+" robot="+r.ID+" node="+r.Node+" tick="+itoa(tick))
}

func (d *Dispatcher) assign(st *domain.State, t *domain.Task, r *domain.Robot, tick int64) {
	t.Status = domain.TaskAssigned
	t.AssignedTo = r.ID
	r.Status = domain.RobotBusy
	r.CurrentTask = t.ID
	// 当前只规划到“当前子目标”（取货点或终点），到达取货点再重建送货段。
	leg := d.legPath(t, r.Node)
	if leg == nil {
		// 从该位置不可达：退回 pending，不建立计划，等待可接手的位置/机器人。
		r.Status = domain.RobotIdle
		r.CurrentTask = ""
		t.AssignedTo = ""
		t.Status = domain.TaskPending
		st.AddDecision("assign_no_route", "task="+t.ID+" robot="+r.ID+
			" from="+r.Node+" requeued")
		return
	}
	if r.Node == t.From {
		t.PickedUp = true
		leg = d.legPath(t, r.Node)
	}
	st.Plans[t.ID] = &domain.Plan{
		TaskID: t.ID, RobotID: r.ID,
		Path: leg, Idx: 0, State: domain.PlanAtNode,
		EnteredAt: tick, Mode: domain.PlanDirect,
	}
	// 忙碌机器人持有当前节点（半永久），空闲机器人不占路网点位。
	st.Reservations = append(st.Reservations, domain.Reservation{
		RobotID: r.ID, TaskID: t.ID,
		Resource: domain.KeyNode(r.Node), Start: tick, End: domain.SentinelForever,
	})
	st.AddDecision("dispatch_assign",
		"task="+t.ID+" robot="+r.ID+" prio="+itoa(int64(t.Priority))+
			" from="+t.From+" to="+t.To+" path="+joinPath(leg)+" tick="+itoa(tick))
}

func pendingOrder(st *domain.State, tick int64) []*domain.Task {
	var out []*domain.Task
	for _, t := range st.Tasks {
		if (t.Status == domain.TaskPending || t.Status == domain.TaskPreempted) && t.AssignedTo == "" {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		pi, pj := planning.EffPrio(out[i], tick), planning.EffPrio(out[j], tick)
		if pi != pj {
			return pi > pj
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// ---- 逐段获取 ----

func (d *Dispatcher) acquireAll(st *domain.State, tick int64) {
	var movers []string
	for id, r := range st.Robots {
		if r.Status != domain.RobotBusy || r.CurrentTask == "" {
			continue
		}
		p := st.Plans[r.CurrentTask]
		if r.Status != domain.RobotBusy {
			continue
		}
		if p != nil && p.State == domain.PlanAtNode && p.Idx+1 < len(p.Path) {
			t := st.Tasks[r.CurrentTask]
			if t != nil && t.Status == domain.TaskPreempted && t.BackoffUntil > tick {
				continue
			}
			movers = append(movers, id)
		}
	}
	sort.Slice(movers, func(i, j int) bool {
		pi := moverKey(st, movers[i], tick)
		pj := moverKey(st, movers[j], tick)
		if pi != pj {
			return pi > pj
		}
		return movers[i] < movers[j]
	})
	for _, id := range movers {
		d.acquireOne(st, id, tick)
	}
}

func moverKey(st *domain.State, robotID string, tick int64) int {
	r := st.Robots[robotID]
	t := st.Tasks[r.CurrentTask]
	p := st.Plans[r.CurrentTask]
	return schedPrio(t, p, tick)
}

func (d *Dispatcher) acquireOne(st *domain.State, robotID string, tick int64) {
	r := st.Robots[robotID]
	t := st.Tasks[r.CurrentTask]
	p := st.Plans[r.CurrentTask]
	if r == nil || t == nil || p == nil || len(p.Path) == 0 || p.Idx >= len(p.Path) {
		return
	}
	if p.Idx+1 >= len(p.Path) {
		// 已到终点但尚未走到达流程：补登记。
		if r.Status == domain.RobotBusy {
			r.Node = p.Path[p.Idx]
			r.Status = domain.RobotExecuting
			p.State = domain.PlanAtNode
			p.ReleaseAt = tick
		}
		return
	}
	u, v := p.Path[p.Idx], p.Path[p.Idx+1]

	commit := func(uu, vv string) bool {
		return d.tryCommit(st, p, r, t, uu, vv, tick)
	}

	if commit(u, v) {
		return
	}
	// 冲突：先尝试改道（从当前节点到终点的其它首跳）。
	if d.tryReroute(st, p, r, t, tick, commit) {
		return
	}
	// 再尝试抢占严格更低调度优先级的占用者。
	if d.tryPreempt(st, p, r, t, u, v, tick) {
		return
	}
	// 只能等待。
	if p.Mode != domain.PlanWait {
		p.Mode = domain.PlanWait
	}
	if p.WaitingSince == 0 {
		p.WaitingSince = tick
	}
	holders := d.blame(st, u, v, tick, robotID)
	st.AddDecision("move_wait",
		"robot="+robotID+" task="+t.ID+" at="+u+" want="+v+
			" holders="+holders+" eff="+itoa(int64(schedPrio(t, p, tick)))+" tick="+itoa(tick))
}

func (d *Dispatcher) needed(st *domain.State, u, v string, tick, arrival int64) []domain.Reservation {
	cost, _ := d.planner.Graph().EdgeCost(u, v)
	_ = cost
	keys := d.planner.EdgeResources(u, v)
	reqs := make([]domain.Reservation, 0, len(keys)+1)
	for _, k := range keys {
		reqs = append(reqs, domain.Reservation{Resource: k, Start: tick, End: arrival})
	}
	reqs = append(reqs, domain.Reservation{Resource: domain.KeyNode(v), Start: arrival, End: domain.SentinelForever})
	return reqs
}

// blockers 返回阻挡该跳的占用预约（确定性排序）。
func (d *Dispatcher) blockers(st *domain.State, u, v string, tick int64, robotID string) []domain.Reservation {
	cost, _ := d.planner.Graph().EdgeCost(u, v)
	arrival := tick + int64(cost)
	var out []domain.Reservation
	for _, req := range d.needed(st, u, v, tick, arrival) {
		end := req.End
		if end == domain.SentinelForever {
			// 仅判定 tick 之后的占用；用足够大的有限窗查询。
			end = tick + 1
		}
		for _, hit := range st.Overlapping(req.Resource, max64(tick, req.Start), max64(req.Start+1, end), robotID) {
			out = append(out, hit)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Resource != out[j].Resource {
			return out[i].Resource < out[j].Resource
		}
		return out[i].RobotID < out[j].RobotID
	})
	return out
}

func (d *Dispatcher) blame(st *domain.State, u, v string, tick int64, robotID string) string {
	bs := d.blockers(st, u, v, tick, robotID)
	seen := map[string]bool{}
	s := ""
	for _, b := range bs {
		if seen[b.RobotID] {
			continue
		}
		seen[b.RobotID] = true
		if s != "" {
			s += ","
		}
		s += b.RobotID + "@" + b.Resource
	}
	if s == "" {
		s = "-"
	}
	return s
}

func (d *Dispatcher) tryCommit(st *domain.State, p *domain.Plan, r *domain.Robot, t *domain.Task, u, v string, tick int64) bool {
	if len(d.blockers(st, u, v, tick, r.ID)) > 0 {
		return false
	}
	cost, _ := d.planner.Graph().EdgeCost(u, v)
	arrival := tick + int64(cost)
	for _, key := range d.planner.EdgeResources(u, v) {
		st.Reservations = append(st.Reservations, domain.Reservation{
			RobotID: r.ID, TaskID: t.ID, Resource: key, Start: tick, End: arrival,
		})
	}
	st.Reservations = append(st.Reservations, domain.Reservation{
		RobotID: r.ID, TaskID: t.ID,
		Resource: domain.KeyNode(v), Start: arrival, End: domain.SentinelForever,
	})
	st.ReleaseNodeAtDeparture(r.ID, u, tick)
	p.State = domain.PlanMoving
	p.EdgeUntil = arrival
	p.EnteredAt = tick
	p.WaitingSince = 0
	wasWaiting := p.Mode == domain.PlanWait || p.Mode == domain.PlanReroute || p.Mode == domain.PlanPreempt
	p.Mode = domain.PlanDirect
	t.Status = domain.TaskRunning
	if t.StartedAt == 0 {
		t.StartedAt = tick
		t.Attempts++
	}
	st.AddDecision("move_go", "robot="+r.ID+" task="+t.ID+" edge="+u+"->"+v+
		" arrive="+itoa(arrival)+(map[bool]string{true: " after_wait", false: ""})[wasWaiting]+" tick="+itoa(tick))
	return true
}

func (d *Dispatcher) tryReroute(st *domain.State, p *domain.Plan, r *domain.Robot, t *domain.Task, tick int64, commit func(u, v string) bool) bool {
	from := p.Path[p.Idx]
	// 改道只针对当前子目标（取货点/终点），保证路径始终是合法简单路径。
	paths := d.planner.Graph().KShortest(from, legTarget(t), domain.MaxKShortest)
	oldFirst := ""
	if p.Idx+1 < len(p.Path) {
		oldFirst = p.Path[p.Idx+1]
	}
	for _, path := range paths {
		if len(path) < 2 || path[1] == oldFirst {
			continue
		}
		nv := path[1]
		if len(d.blockers(st, from, nv, tick, r.ID)) > 0 {
			continue
		}
		// 从当前节点起用该路径整体替换；Idx 定位到当前节点（path 必含 from）。
		p.Path = path
		p.Idx = indexOf(path, from)
		if p.Idx < 0 {
			p.Idx = 0
		}
		p.Mode = domain.PlanReroute
		st.AddDecision("move_reroute", "robot="+r.ID+" task="+t.ID+
			" at="+from+" target="+legTarget(t)+" old_next="+nz(oldFirst)+
			" new_next="+nv+" tick="+itoa(tick))
		return commit(from, nv)
	}
	return false
}

func (d *Dispatcher) tryPreempt(st *domain.State, p *domain.Plan, r *domain.Robot, t *domain.Task, u, v string, tick int64) bool {
	bs := d.blockers(st, u, v, tick, r.ID)
	if len(bs) == 0 {
		return false
	}
	myPrio := schedPrio(t, p, tick)
	// 选择可抢占者中调度优先级最低、ID 最小的一个。
	type vic struct {
		id   string
		prio int
	}
	var cands []vic
	seen := map[string]bool{}
	for _, b := range bs {
		br := st.Robots[b.RobotID]
		if br == nil || !br.Status.IsBusy() || b.RobotID == r.ID || seen[b.RobotID] {
			continue
		}
		bt := st.Tasks[br.CurrentTask]
		bp := st.Plans[br.CurrentTask]
		hp := schedPrio(bt, bp, tick)
		if hp >= myPrio {
			continue
		}
		seen[b.RobotID] = true
		cands = append(cands, vic{b.RobotID, hp})
	}
	if len(cands) == 0 {
		return false
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].prio != cands[j].prio {
			return cands[i].prio < cands[j].prio
		}
		return cands[i].id < cands[j].id
	})
	victim := cands[0].id
	taskID := d.ReclaimRobot(st, victim, tick, domain.CodeDeadlockVictim, false)
	p.Mode = domain.PlanPreempt
	p.Victims = []string{victim}
	st.AddDecision("move_preempt",
		"robot="+r.ID+" task="+t.ID+" preempts="+victim+" victim_task="+nz(taskID)+
			" eff="+itoa(int64(myPrio))+" tick="+itoa(tick))
	return d.tryCommit(st, p, r, t, u, v, tick)
}

// ---- 死锁 ----

// BreakDeadlocks 检测 wait-for 环并逐环抢占打破，返回被抢占的机器人 ID。
func (d *Dispatcher) BreakDeadlocks(st *domain.State, tick int64) []string {
	var victims []string
	for guard := 0; guard < len(st.Robots)+1; guard++ {
		cycles := d.planner.DetectDeadlock(st, tick)
		if len(cycles) == 0 {
			break
		}
		cyc := cycles[0]
		victim := pickVictim(st, cyc.Robots, tick)
		taskID := d.ReclaimRobot(st, victim, tick, domain.CodeDeadlockVictim, false)
		victims = append(victims, victim)
		st.AddDecision("deadlock_break",
			"cycle="+joinStrings(cyc.Robots)+" victim="+victim+" victim_task="+nz(taskID)+
				" tick="+itoa(tick))
	}
	return victims
}

func pickVictim(st *domain.State, robots []string, tick int64) string {
	victim := ""
	bestPrio := 0
	for _, id := range robots {
		r := st.Robots[id]
		if r == nil {
			continue
		}
		t := st.Tasks[r.CurrentTask]
		p := st.Plans[r.CurrentTask]
		pr := schedPrio(t, p, tick)
		if victim == "" || pr < bestPrio || (pr == bestPrio && id < victim) {
			victim, bestPrio = id, pr
		}
	}
	return victim
}

// ---- 回收 ----

// ReclaimRobot 回收机器人占有的预约与未完成任务。返回受影响的任务 ID。
// incAttempts 控制是否计入执行失败次数；被抢占任务带 backoff 回到 pending。
func (d *Dispatcher) ReclaimRobot(st *domain.State, robotID string, tick int64, reason domain.Code, incAttempts bool) string {
	r := st.Robots[robotID]
	if r == nil {
		return ""
	}
	taskID := r.CurrentTask
	st.ReleaseReservations(robotID)
	delete(st.Plans, taskID)
	r.CurrentTask = ""
	if reason != domain.CodeOffline {
		r.Status = domain.RobotIdle
	}
	if taskID == "" {
		return ""
	}
	t := st.Tasks[taskID]
	if t == nil || t.Status.IsTerminal() {
		return taskID
	}
	// 未取货时，取货点迁移到机器人最后所在位置（货物仍在机器人处则直送）。
	if !t.PickedUp {
		t.From = r.Node
	}
	if incAttempts && t.Attempts >= domain.MaxAttempts {
		d.failTask(st, t, reason, tick)
		st.AddDecision("reclaim_fail", "robot="+robotID+" task="+taskID+
			" attempts="+itoa(int64(t.Attempts))+" reason="+string(reason))
		return taskID
	}
	if reason == domain.CodeDeadlockVictim {
		// 让行：任务仍属于该机器人，但释放全部预约并退避，之后由它自己重新规划继续。
		path := d.legPath(t, r.Node)
		if path == nil {
			// 极端兜底：当前位置不可达则退回公共 pending。
			r.CurrentTask = ""
			t.AssignedTo = ""
			t.Status = domain.TaskPending
			st.AddDecision("reclaim_yield_no_route", "robot="+robotID+" task="+taskID+
				" from="+r.Node+" tick="+itoa(tick))
			return taskID
		}
		r.Status = domain.RobotBusy
		r.CurrentTask = taskID
		t.AssignedTo = robotID
		t.Status = domain.TaskPreempted
		t.BackoffUntil = tick + domain.DeadlockBackoff
		st.Plans[taskID] = &domain.Plan{
			TaskID: taskID, RobotID: robotID,
			Path: path, Idx: 0, State: domain.PlanAtNode,
			EnteredAt: tick, Mode: domain.PlanWait,
		}
		st.AddDecision("reclaim_yield", "robot="+robotID+" task="+taskID+
			" reason="+string(reason)+" resume_from="+r.Node+
			" backoff_until="+itoa(tick+domain.DeadlockBackoff)+" tick="+itoa(tick))
		return taskID
	}
	// 掉线/失败/超时：解除归属，任务从机器人最后位置重新等待分派。
	r.CurrentTask = ""
	t.AssignedTo = ""
	t.Status = domain.TaskPending
	t.StartedAt = 0
	st.AddDecision("reclaim", "robot="+robotID+" task="+taskID+" reason="+string(reason)+
		" attempts="+itoa(int64(t.Attempts))+" restart_from="+r.Node+" tick="+itoa(tick))
	return taskID
}

func (d *Dispatcher) failTask(st *domain.State, t *domain.Task, code domain.Code, tick int64) {
	t.Status = domain.TaskFailed
	t.EndedAt = tick
	t.FailedReasonCode = code
	if t.AssignedTo != "" {
		if r := st.Robots[t.AssignedTo]; r != nil {
			st.ReleaseReservations(r.ID)
			delete(st.Plans, t.ID)
			r.CurrentTask = ""
			if r.Status != domain.RobotOffline {
				r.Status = domain.RobotIdle
			}
		}
	}
	st.AddDecision("task_failed", "task="+t.ID+" reason="+string(code)+" tick="+itoa(tick))
}

// FailTask 供内核在结果上报失败时复用。
func (d *Dispatcher) FailTask(st *domain.State, t *domain.Task, code domain.Code, tick int64) {
	d.failTask(st, t, code, tick)
}

// ---- 小工具 ----

func itoa(i int64) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [24]byte
	n := len(b)
	for i > 0 {
		n--
		b[n] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		n--
		b[n] = '-'
	}
	return string(b[n:])
}

func nz(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func joinPath(p []string) string { return joinStrings(p) }

func joinStrings(rs []string) string {
	out := ""
	for i, x := range rs {
		if i > 0 {
			out += ","
		}
		out += x
	}
	return out
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func indexOf(path []string, node string) int {
	for i, n := range path {
		if n == node {
			return i
		}
	}
	return -1
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func safeTo(t *domain.Task) string {
	if t == nil {
		return "-"
	}
	return t.To
}

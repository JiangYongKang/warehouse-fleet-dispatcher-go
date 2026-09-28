package dispatcher

import "sort"

// execution 是一台机器人对某个已分派任务的执行进度。
// 不变量：机器人任意时刻只占用一个资源（某节点，或其所在的某条边）。
type execution struct {
	taskID    string
	route     []*Edge // 分派时确定的几何路径（robotNode -> From -> To）
	idx       int     // 下一条待进入的边在 route 中的下标
	onEdge    bool    // 当前是否停留在边上
	enterAt   int64   // 进入当前边的 tick；长度 L 的边在 enterAt+L 才可申请目的节点
	node      string  // !onEdge 时所在节点
	arrived   bool    // 是否已到达任务终点（等待上报结果）
	blockedBy string  // 上一拍造成阻塞的机器人 ID（空=未阻塞）
	waitUntil int64   // 死锁退避冷却：该 tick 之前不尝试进入下一资源（给对方让路）
	yields    int     // 本任务连续退避次数；冷却时长随次数递增，消除反复成环振荡
}

// wantedRes 返回机器人此刻想要进入的资源：
// 在节点上想要下一条边；在边上且已走完（now==enterAt+Length）想要目的节点；
// 仍在途则无申请目标。
func (x *execution) wantedRes(now int64) (Resource, bool) {
	if x.arrived {
		return Resource{}, false
	}
	if !x.onEdge {
		if x.idx >= len(x.route) {
			return Resource{}, false
		}
		return edgeRes(x.route[x.idx]), true
	}
	if now < x.enterAt+int64(x.route[x.idx].Length) {
		return Resource{}, false // 仍在途，本拍不申请新资源
	}
	return nodeRes(x.route[x.idx].To), true
}

// assignLocked 执行一拍：分派新任务 -> 推进运动 -> 检测并打破死锁。
// 仅当 now > lastTick 时执行运动；同一 tick 内重复调用只补做分派。
// 分派先于运动：新任务在被分派的同一拍即可从起点出发。
func (e *Engine) assignLocked(now int64) ([]string, error) {
	first := e.lastTick < 0
	assigned, err := e.assignNewLocked(now)
	if err != nil {
		return nil, err
	}
	if first || now > e.lastTick {
		e.tickMovementLocked(now)
		e.lastTick = now
	}
	if err := e.detectAndBreakLocked(now); err != nil {
		return nil, err
	}
	return assigned, nil
}

// tickMovementLocked 分两拍推进所有在线机器人：
//  1. 先把所有现有占用顺延到 [start, now+1)（机器人受阻时就地占住资源）；
//  2. 再按机器人 ID 升序逐个尝试进入下一资源（顺序固定 => 结论确定）。
func (e *Engine) tickMovementLocked(now int64) {
	ids := make([]string, 0, len(e.robots))
	for id, r := range e.robots {
		if r.state.Status == RobotOnline {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)

	// 阶段 1：顺延所有现有占用窗口到本拍结束。
	for _, id := range ids {
		e.rt.ExtendHolds(id, now+1)
	}

	// 阶段 2：逐个尝试移动（ID 升序 => 同拍资源竞争的赢家固定）。
	for _, id := range ids {
		e.moveRobotLocked(id, now)
	}
}

// moveRobotLocked 在一个 tick 内推进一台机器人。
// 节点是零停靠途经点：到达中间节点后若下一条边空闲，同拍立即进入，
// 直到需要等待资源或本拍停在某条边上为止；仅在终点节点停留等待上报。
func (e *Engine) moveRobotLocked(id string, now int64) {
	ex := e.exec[id]
	if ex == nil || ex.arrived || now < ex.waitUntil {
		if ex != nil && now < ex.waitUntil {
			e.logf("robot %q task %q yielding until tick %d", id, ex.taskID, ex.waitUntil)
		}
		return
	}
	// 单拍内最多推进"当前在边 + 途经零停靠节点"，有界（路线长度有限）。
	for steps := 0; steps < len(ex.route)+1; steps++ {
		if ex.arrived {
			return
		}
		if !ex.onEdge {
			// 位于节点：申请下一条边。
			if ex.idx >= len(ex.route) {
				return
			}
			edge := ex.route[ex.idx]
			want := edgeRes(edge)
			if holder := e.rt.HolderAt(want, now, id); holder != "" {
				ex.blockedBy = holder
				e.logf("robot %q task %q blocked at node %q wanting edge %s, held by %q",
					id, ex.taskID, ex.node, want.ID, holder)
				return
			}
			e.rt.ReleaseResource(id, ex.taskID, nodeRes(ex.node))
			e.rt.forcePut(Reservation{RobotID: id, TaskID: ex.taskID, Res: want, Win: Window{now, now + 1}})
			ex.onEdge = true
			ex.enterAt = now
			ex.blockedBy = ""
			e.logf("robot %q task %q enters edge %s at tick %d (length %d)",
				id, ex.taskID, want.ID, now, edge.Length)
			return // 进入边后本拍即在途（边长度 >=1）
		}

		edge := ex.route[ex.idx]
		if now < ex.enterAt+int64(edge.Length) {
			ex.blockedBy = ""
			return // 仍在途
		}
		// 边已走完，申请目的节点。
		dest := edge.To
		want := nodeRes(dest)
		if holder := e.rt.HolderAt(want, now, id); holder != "" {
			if _, executing := e.exec[holder]; executing {
				ex.blockedBy = holder // 对方在执行任务：真实等待，可能成环
				e.logf("robot %q task %q stopped on edge %s, destination node %q held by executing %q",
					id, ex.taskID, edge.From+"->"+dest, dest, holder)
				return
			}
			// 空闲停靠机器人视为可让位（停靠支线），到达者接管节点。
			e.logf("robot %q task %q arriving node %q: idle robot %q yields", id, ex.taskID, dest, holder)
			e.rt.ReleaseResource(holder, "", want)
		}
		e.rt.ReleaseResource(id, ex.taskID, edgeRes(edge))
		e.rt.forcePut(Reservation{RobotID: id, TaskID: ex.taskID, Res: want, Win: Window{now, now + 1}})
		ex.onEdge = false
		ex.enterAt = 0
		ex.node = dest
		ex.idx++
		ex.blockedBy = ""
		e.robots[id].state.Node = dest
		if ex.idx == len(ex.route) {
			ex.arrived = true
			e.logf("robot %q completed route for task %q, arrived %q at tick %d (awaiting result report)",
				id, ex.taskID, dest, now)
			return
		}
		e.logf("robot %q task %q reached node %q at tick %d", id, ex.taskID, dest, now)
		// 途经节点：循环继续，同拍尝试下一条边（零停靠）。
	}
}

// effectivePriorityLocked = 静态优先级 + 等待老化补偿（防饿死）。
func (e *Engine) effectivePriorityLocked(t *TaskState, now int64) int {
	return t.Priority + ageBonus(t.WaitTick, now)
}

// readyTasksLocked 按"有效优先级降序、提交序号升序"返回可分派任务，
// 顺序确定：同优先级严格 FIFO，老任务因老化补偿必然后来居上。
func (e *Engine) readyTasksLocked(now int64) []*TaskState {
	var out []*TaskState
	for _, t := range e.tasks {
		if t.Status != TaskPending {
			continue
		}
		ready := true
		for _, dep := range t.Deps {
			d, ok := e.tasks[dep]
			if !ok || d.Status != TaskCompleted {
				ready = false
				break
			}
		}
		if ready {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		pi, pj := e.effectivePriorityLocked(out[i], now), e.effectivePriorityLocked(out[j], now)
		if pi != pj {
			return pi > pj
		}
		if out[i].WaitTick != out[j].WaitTick {
			return out[i].WaitTick < out[j].WaitTick
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}

// assignNewLocked 尝试把每个就绪任务派给"能最早完成"的空闲在线机器人。
// 老化计时语义：任务"就绪但因资源不足派不出去"的第一个 tick 记为 WaitTick，
// 从那一刻起累计老化补偿；尚未就绪（依赖未完成）不计时；一分派即清零。
func (e *Engine) assignNewLocked(now int64) ([]string, error) {
	var assigned []string
	ready := e.readyTasksLocked(now)
	// 先为"本轮首次就绪"的任务加盖时间戳（排序依赖它，故在排序前完成）。
	for _, t := range ready {
		if t.WaitTick == 0 {
			t.WaitTick = now
		}
	}
	ready = e.readyTasksLocked(now) // 用新时间戳重新排序
	for _, t := range ready {
		if t.Status != TaskPending { // 本轮内可能已被前置连锁影响
			continue
		}
		robotID, route, reason := e.chooseRobotLocked(t, now)
		if robotID == "" {
			e.logf("task %q not dispatchable at tick %d: %s (priority %d+age %d, ready since %d)",
				t.ID, now, reason, t.Priority, ageBonus(t.WaitTick, now), t.WaitTick)
			continue
		}
		t.Status = TaskAssigned
		t.Assignee = robotID
		t.Dispatch++
		t.WaitTick = 0 // 已被服务，停止老化计时
		e.robots[robotID].state.CurrentTask = t.ID
		e.exec[robotID] = &execution{taskID: t.ID, route: route, node: e.robots[robotID].state.Node}
		assigned = append(assigned, t.ID)
		e.logf("assign task %q (priority %d+age %d, deps %v) to robot %q at tick %d, route len %d, dispatch %d",
			t.ID, t.Priority, ageBonus(t.WaitTick, now), t.Deps, robotID, now, len(route), t.Dispatch)
	}
	return assigned, nil
}

// chooseRobotLocked 在空闲在线机器人中选路最佳者：
// 总路程最短者胜，平局取机器人 ID 字典序最小；死锁改道禁边按任务记忆。
func (e *Engine) chooseRobotLocked(t *TaskState, now int64) (string, []*Edge, string) {
	ids := make([]string, 0)
	for id, r := range e.robots {
		if r.state.Status == RobotOnline && r.state.CurrentTask == "" {
			if _, ok := e.exec[id]; !ok {
				ids = append(ids, id)
			}
		}
	}
	sort.Strings(ids)
	var bestID string
	var bestRoute []*Edge
	bestLen := 0
	for _, id := range ids {
		rn := e.robots[id].state.Node
		route, ok := e.buildRouteLocked(rn, t)
		if !ok {
			continue
		}
		l := 0
		for _, ed := range route {
			l += ed.Length
		}
		if bestID == "" || l < bestLen || (l == bestLen && id < bestID) {
			bestID, bestRoute, bestLen = id, route, l
		}
	}
	if bestID != "" {
		return bestID, bestRoute, ""
	}
	if len(ids) == 0 {
		return "", nil, "no idle online robot"
	}
	return "", nil, "no feasible route from any idle robot to task endpoints"
}

// buildRouteLocked 拼接 robotNode -> task.From -> task.To 的确定性最短路径。
// 运行时冲突（车道被占/对头）一律由"等待或死锁退避"处理，规划阶段不改道，
// 从而同一任务每次重派都选择同一条几何路径，结论可解释、可重放。
func (e *Engine) buildRouteLocked(robotNode string, t *TaskState) ([]*Edge, bool) {
	var route []*Edge
	if robotNode != t.From {
		p1, err := e.graph.ShortestPath(robotNode, t.From)
		if err != nil {
			return nil, false
		}
		route = append(route, p1...)
	}
	if t.From != t.To {
		p2, err := e.graph.ShortestPath(t.From, t.To)
		if err != nil {
			return nil, false
		}
		route = append(route, p2...)
	}
	return route, true
}

// maxDeadlockRounds 限制单拍内死锁打破轮数（每轮至少打破一个真实环，
// 环中机器人有限，正常情况远小于此值；达到上界说明模型异常，快速失败）。
const maxDeadlockRounds = 64

// detectAndBreakLocked 构建实时等待图并逐环打破，直到无环。
func (e *Engine) detectAndBreakLocked(now int64) error {
	for round := 0; round < maxDeadlockRounds; round++ {
		nodes := make([]string, 0, len(e.exec))
		for id := range e.exec {
			if e.robots[id].state.Status == RobotOnline {
				nodes = append(nodes, id)
			}
		}
		sort.Strings(nodes)

		var edges []waitsFor
		for _, id := range nodes {
			ex := e.exec[id]
			if ex == nil || ex.arrived || now < ex.waitUntil {
				continue
			}
			want, ok := ex.wantedRes(now)
			if !ok {
				continue
			}
			holder := e.rt.HolderAt(want, now, id)
			if holder == "" {
				continue
			}
			if _, busy := e.exec[holder]; !busy {
				continue // 持有者只是空闲停靠（可让位）：普通争用，非死锁
			}
			edges = append(edges, waitsFor{from: id, to: holder})
		}

		cycle := detectCycle(edges, nodes)
		if cycle == nil {
			return nil
		}
		victim := breakDeadlock(cycle,
			func(robotID string) int {
				return e.effectivePriorityLocked(e.tasks[e.exec[robotID].taskID], now)
			},
			func(robotID string) int64 {
				return e.tasks[e.exec[robotID].taskID].Seq
			},
		)
		if e.preemptLocked(cycle, victim, now) {
			// 抢占仅让受害者退避并冷却一拍（保留任务、机器人与路径），
			// 下一拍运动阶段对方即可进入被让出的资源；唤醒后台循环补做推进。
			e.kick()
		}
	}
	return fail("Engine.detectAndBreak", ErrDeadlock,
		"deadlock resolution exceeded %d rounds; aborting to avoid uncertainty", maxDeadlockRounds)
}

// preemptLocked 让环中的牺牲者退避一拍，把资源让给对方，打破"互相持有并等待"。
// 返回 true 表示执行了退避。两种情形统一处理，均保留任务、执行机器人与几何路径，
// 不做重排队、不更换机器人——保证让路关系单调（老/高优先级任务持续推进），
// 避免同一任务被反复抢占、反复绕行造成的活锁。
//   - 停在边上：撤回当前边的起点；
//   - 停在节点上（正占着对方的目的节点）：沿来路退到上一个节点；
//     若没有来路（任务第一个节点），则就地释放节点并冷却，对方可即时进入。
func (e *Engine) preemptLocked(cycle []string, victim string, now int64) (requeued bool) {
	ex := e.exec[victim]
	task := e.tasks[ex.taskID]
	prios := make([]string, 0, len(cycle))
	for _, id := range cycle {
		t := e.tasks[e.exec[id].taskID]
		prios = append(prios, fmtSprintf("%s(task=%s,effPri=%d,seq=%d)",
			id, t.ID, e.effectivePriorityLocked(t, now), t.Seq))
	}
	e.logf("DEADLOCK cycle %v at tick %d; [%s]; victim=%q (lowest effPri; tie => newest seq, then largest robot id)",
		cycle, now, prios, victim)

	// 计算环中"紧邻受害者的对手"越过所争资源还需要的 tick 数，
	// 冷却至少覆盖该时长 + 2 拍缓冲，并随连续退避次数递增，保证让路单调收敛。
	other := cycleWaitTarget(cycle, victim)
	clearTicks := int64(2)
	if ox := e.exec[other]; ox != nil {
		if ox.onEdge && ox.idx < len(ox.route) {
			// 对手在边上：剩余在途 + 进入下一节点所需的 1 拍。
			clearTicks = ox.enterAt + int64(ox.route[ox.idx].Length) - now + 2
		} else {
			clearTicks = 3 // 对手在节点上，需进入边并越过
		}
	}
	ex.yields++
	backoff := clearTicks + int64(ex.yields) // 连续退避 => 让路更久
	yieldUntil := now + backoff
	if backoff < 2 {
		backoff = 2
	}
	_ = yieldUntil

	// 确定退避目标节点。
	var backNode string
	var occupiedRes Resource
	if ex.onEdge {
		ed := ex.route[ex.idx]
		backNode = ed.From
		occupiedRes = edgeRes(ed)
	} else if ex.idx > 0 {
		prev := ex.route[ex.idx-1]
		backNode = prev.From // 来路的起点（= 上一个所在节点）
		occupiedRes = nodeRes(ex.node)
	} else {
		// 任务首个节点、无路可退：就地释放节点并冷却，对方直接进入。
		n := ex.node
		e.rt.ReleaseResource(victim, task.ID, nodeRes(n))
		ex.waitUntil = now + backoff
		ex.blockedBy = ""
		e.logf("victim robot %q yields start node %s in place and waits until tick %d (backoff %d); task %q retained",
			victim, n, ex.waitUntil, backoff, task.ID)
		return true
	}

	// 释放当前占用，退到 backNode；若 backNode 被占（极少：另一个执行体），
	// 等待后由下一轮正常争用/死锁处理，结论仍确定。
	e.rt.ReleaseResource(victim, task.ID, occupiedRes)
	retreatIdx := ex.idx
	if !ex.onEdge && ex.idx > 0 {
		// 在节点上回退：刚走过的是 route[idx-1]，退回后它重新成为下一条待进入边。
		retreatIdx = ex.idx - 1
	}
	if holder := e.rt.HolderAt(nodeRes(backNode), now, victim); holder != "" {
		ex.waitUntil = now + backoff
		e.logf("victim robot %q wants to retreat to %s but held by %q; waits until tick %d",
			victim, backNode, holder, ex.waitUntil)
		return true
	}
	e.rt.forcePut(Reservation{RobotID: victim, TaskID: task.ID, Res: nodeRes(backNode), Win: Window{now, now + 1}})
	ex.onEdge = false
	ex.enterAt = 0
	ex.node = backNode
	ex.idx = retreatIdx
	ex.waitUntil = now + backoff
	ex.blockedBy = ""
	e.robots[victim].state.Node = backNode
	e.logf("victim robot %q retreats to node %s and yields until tick %d (backoff %d, yield #%d) for %q to clear; task %q retained (edge idx %d)",
		victim, backNode, ex.waitUntil, backoff, ex.yields, other, task.ID, ex.idx)
	return true
}

// cycleWaitTarget 返回等待图中受害者直接等待的那个环成员（即受益者）。
func cycleWaitTarget(cycle []string, victim string) string {
	for i, id := range cycle {
		if id == victim {
			return cycle[(i+1)%len(cycle)]
		}
	}
	if len(cycle) > 1 {
		return cycle[1]
	}
	return victim
}

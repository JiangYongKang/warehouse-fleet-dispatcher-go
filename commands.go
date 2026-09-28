package dispatcher

import "sort"

// RegisterRobot 注册一台位于 node 的在线机器人。
// 重复 ID 返回 already_exists；位置节点不存在返回 not_found。
func (e *Engine) RegisterRobot(id, node string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.registerRobotLocked(id, node)
}

func (e *Engine) registerRobotLocked(id, node string) error {
	if id == "" {
		return fail("Engine.RegisterRobot", ErrInvalidArgument, "robot id is empty")
	}
	if _, ok := e.robots[id]; ok {
		return fail("Engine.RegisterRobot", ErrAlreadyExists, "robot %q already registered", id)
	}
	if !e.graph.HasNode(node) {
		return fail("Engine.RegisterRobot", ErrNotFound, "node %q not found in graph", node)
	}
	e.robots[id] = &Robot{state: RobotState{ID: id, Status: RobotOnline, Node: node}}
	// 空闲机器人从注册时刻起占住其所在节点（逐拍占用模型）。
	e.rt.forcePut(Reservation{RobotID: id, Res: nodeRes(node), Win: Window{e.clock.Now(), e.clock.Now() + 1}})
	e.logf("registered robot %q at node %q", id, node)
	if err := e.persistLocked(); err != nil {
		return err
	}
	e.kick()
	return nil
}

// MarkOffline 将机器人标记掉线：
// 执行中的任务回收为 pending（Attempt 保留以累计重试），预约全部释放，
// 改道禁边记忆清除，任务可被其他机器人重新分派。
// 已完成任务不受影响；对已掉线机器人重复操作返回 invalid_state。
func (e *Engine) MarkOffline(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.robots[id]
	if !ok {
		return fail("Engine.MarkOffline", ErrNotFound, "robot %q not found", id)
	}
	if r.state.Status == RobotOffline {
		return fail("Engine.MarkOffline", ErrInvalidState, "robot %q already offline", id)
	}
	r.state.Status = RobotOffline
	if ex, busy := e.exec[id]; busy {
		t := e.tasks[ex.taskID]
		t.Status = TaskPending
		t.Assignee = ""
		t.WaitTick = 0
		e.logf("robot %q went offline: task %q requeued to pending (attempt %d), reservations released",
			id, t.ID, t.Attempt)
	}
	delete(e.exec, id)
	n := e.rt.ReleaseRobot(id, "")
	r.state.CurrentTask = ""
	e.logf("robot %q marked offline; %d reservation(s) released", id, n)
	if err := e.persistLocked(); err != nil {
		return err
	}
	e.kick()
	return nil
}

// MarkOnline 将机器人重新置为在线（在其最后已知节点恢复）。
func (e *Engine) MarkOnline(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.robots[id]
	if !ok {
		return fail("Engine.MarkOnline", ErrNotFound, "robot %q not found", id)
	}
	if r.state.Status == RobotOnline {
		return fail("Engine.MarkOnline", ErrInvalidState, "robot %q already online", id)
	}
	r.state.Status = RobotOnline
	e.rt.forcePut(Reservation{RobotID: id, Res: nodeRes(r.state.Node), Win: Window{e.clock.Now(), e.clock.Now() + 1}})
	e.logf("robot %q back online at node %q", id, r.state.Node)
	if err := e.persistLocked(); err != nil {
		return err
	}
	e.kick()
	return nil
}

// SubmitTask 提交任务。校验：
//   - ID 非空且不重复（重复提交返回 already_exists，不产生第二次执行）；
//   - From/To 节点存在；
//   - 依赖任务都已存在；
//   - 依赖图无环（依赖环中的任务永远无法就绪，提前拒绝）。
func (e *Engine) SubmitTask(t Task) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.submitTaskLocked(t)
}

func (e *Engine) submitTaskLocked(t Task) error {
	const op = "Engine.SubmitTask"
	if t.ID == "" {
		return fail(op, ErrInvalidArgument, "task id is empty")
	}
	if _, dup := e.tasks[t.ID]; dup {
		return fail(op, ErrAlreadyExists, "task %q already submitted", t.ID)
	}
	if !e.graph.HasNode(t.From) {
		return fail(op, ErrNotFound, "from node %q not found", t.From)
	}
	if !e.graph.HasNode(t.To) {
		return fail(op, ErrNotFound, "to node %q not found", t.To)
	}
	seen := map[string]bool{}
	for _, d := range t.Deps {
		if d == t.ID {
			return fail(op, ErrInvalidArgument, "task %q depends on itself", t.ID)
		}
		if seen[d] {
			return fail(op, ErrInvalidArgument, "duplicate dependency %q on task %q", d, t.ID)
		}
		seen[d] = true
		if _, ok := e.tasks[d]; !ok {
			return fail(op, ErrDepsUnmet, "dependency %q of task %q not submitted", d, t.ID)
		}
	}
	// 依赖环检测：DFS 新任务沿依赖边追溯。
	if err := e.checkDepCycleLocked(t); err != nil {
		return err
	}

	e.seq++
	deps := append([]string(nil), t.Deps...)
	sort.Strings(deps) // 规范化，保证快照/重放一致
	st := &TaskState{
		Task:     t,
		Status:   TaskPending,
		Seq:      e.seq,
		WaitTick: 0, // 尚未就绪，老化计时从"就绪却派不出去"开始
	}
	st.Deps = deps
	e.tasks[t.ID] = st
	e.logf("submitted task %q: from=%s to=%s priority=%d deps=%v seq=%d",
		t.ID, t.From, t.To, t.Priority, deps, e.seq)
	if err := e.persistLocked(); err != nil {
		return err
	}
	e.kick()
	return nil
}

// checkDepCycleLocked 从"假设加入 newTask"出发沿依赖边 DFS，
// 若回到 newTask 则存在环。
func (e *Engine) checkDepCycleLocked(newTask Task) error {
	const op = "Engine.SubmitTask"
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[string]int{newTask.ID: gray}
	var dfs func(id string) bool
	dfs = func(id string) bool {
		var deps []string
		if id == newTask.ID {
			deps = newTask.Deps
		} else if t, ok := e.tasks[id]; ok {
			deps = t.Deps
		}
		sort.Strings(deps)
		for _, d := range deps {
			if d == newTask.ID {
				return true // 回到新任务：成环
			}
			if color[d] == gray {
				return true
			}
			if color[d] == black {
				continue
			}
			color[d] = gray
			if dfs(d) {
				return true
			}
			color[d] = black
		}
		return false
	}
	if dfs(newTask.ID) {
		return fail(op, ErrInvalidArgument,
			"adding task %q would create a dependency cycle among %v", newTask.ID, newTask.Deps)
	}
	return nil
}

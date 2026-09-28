package dispatcher

// MaxAttempts 是单个任务允许的执行次数上限；超过后任务进入 failed 终态。
const MaxAttempts = 3

// TickReport 是机器人上报的执行结果。
type TickReport struct {
	TaskID  string
	RobotID string
	Success bool
	Reason  string // 失败原因（Success=false 时）
}

// ReportResult 处理机器人的执行结果上报。判定规则（均给出可区分原因）：
//   - 任务不存在            -> not_found
//   - 上报机器人不是执行者   -> stale_result（防串台/过期上报）
//   - 任务尚未到达(未完成路由) -> invalid_state
//   - 已 completed 的任务重复上报：
//   - 成功重复 -> already_done（幂等，绝不二次执行）
//   - 失败重复 -> stale_result（结果已裁定）
//   - failed 终态的任何上报   -> already_done
//   - pending 状态的上报      -> invalid_state（已被掉线/抢占回收）
func (e *Engine) ReportResult(rep TickReport) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	const op = "Engine.ReportResult"
	t, ok := e.tasks[rep.TaskID]
	if !ok {
		return fail(op, ErrNotFound, "task %q not found", rep.TaskID)
	}
	e.logf("report received: task=%q robot=%q success=%v reason=%q | current status=%s assignee=%q attempt=%d",
		rep.TaskID, rep.RobotID, rep.Success, rep.Reason, t.Status, t.Assignee, t.Attempt)

	switch t.Status {
	case TaskCompleted:
		// 已完成任务绝不再次执行；重复成功上报幂等确认。
		if rep.Success {
			e.logf("task %q already completed: duplicate success report treated as idempotent no-op", rep.TaskID)
			return fail(op, ErrAlreadyDone, "task %q already completed", rep.TaskID)
		}
		e.logf("task %q already completed: late failure report rejected", rep.TaskID)
		return fail(op, ErrStaleResult, "task %q already completed; failure report is stale", rep.TaskID)
	case TaskFailed:
		return fail(op, ErrAlreadyDone, "task %q already failed terminally", rep.TaskID)
	case TaskPending:
		// 执行者已掉线或被抢占，任务已回收重派：该上报属于旧执行，作废。
		return fail(op, ErrStaleResult,
			"task %q was requeued before report; report from %q discarded", rep.TaskID, rep.RobotID)
	case TaskAssigned:
		// 继续严格校验
	}

	if t.Assignee != rep.RobotID {
		e.logf("report rejected: task %q assignee is %q, report came from %q",
			rep.TaskID, t.Assignee, rep.RobotID)
		return fail(op, ErrStaleResult,
			"task %q is assigned to %q, not %q", rep.TaskID, t.Assignee, rep.RobotID)
	}
	ex := e.exec[rep.RobotID]
	if ex == nil || !ex.arrived {
		return fail(op, ErrInvalidState,
			"task %q has not arrived yet; result report premature", rep.TaskID)
	}

	if rep.Success {
		e.completeLocked(t, rep.RobotID)
		e.logf("task %q COMPLETED by robot %q at tick %d after attempt %d",
			t.ID, rep.RobotID, e.clock.Now(), t.Attempt)
	} else {
		e.failOnceLocked(t, rep.RobotID, rep.Reason)
	}
	if err := e.persistLocked(); err != nil {
		return err
	}
	e.kick() // 成功完成可能解锁依赖任务；失败重排也需要再分派
	return nil
}

// completeLocked 把任务置为终态 completed：释放占用、机器人空闲、清改道记忆。
func (e *Engine) completeLocked(t *TaskState, robotID string) {
	t.Status = TaskCompleted
	t.Reason = ""
	e.rt.ReleaseRobot(robotID, "")
	delete(e.exec, robotID)
	r := e.robots[robotID]
	if r != nil {
		r.state.CurrentTask = ""
		// 完成后机器人停在终点并占住该节点。
		e.rt.forcePut(Reservation{RobotID: robotID, Res: nodeRes(t.To), Win: Window{e.clock.Now(), e.clock.Now() + 1}})
		r.state.Node = t.To
	}
	e.resultLog[t.ID] = resultRecord{
		taskID: t.ID, robotID: robotID, ok: true, attempt: t.Attempt, seq: e.seq,
	}
}

// failOnceLocked 处理一次"机器人上报的执行失败"：这是唯一累加 Attempt 的地方；
// 未超次数则回收重派，超过则终态 failed。调度抢占/掉线回收不累加 Attempt。
func (e *Engine) failOnceLocked(t *TaskState, robotID, reason string) {
	t.Attempt++
	e.logf("task %q execution failed on robot %q (failure #%d): %s", t.ID, robotID, t.Attempt, reason)
	e.rt.ReleaseRobot(robotID, "")
	delete(e.exec, robotID)
	if r := e.robots[robotID]; r != nil {
		r.state.CurrentTask = ""
		node := t.To // 到达终点后才可能收到上报，故位置即终点
		r.state.Node = node
		e.rt.forcePut(Reservation{RobotID: robotID, Res: nodeRes(node), Win: Window{e.clock.Now(), e.clock.Now() + 1}})
	}
	if t.Attempt >= MaxAttempts {
		t.Status = TaskFailed
		t.Assignee = ""
		t.Reason = reason
		e.resultLog[t.ID] = resultRecord{
			taskID: t.ID, robotID: robotID, ok: false, attempt: t.Attempt, seq: e.seq,
		}
		e.logf("task %q FAILED terminally after %d attempts: %s", t.ID, t.Attempt, reason)
		return
	}
	// 回收重派：老化计时清零，重新就绪后重新累计。
	t.Status = TaskPending
	t.Assignee = ""
	t.WaitTick = 0
	e.logf("task %q requeued to pending for retry (failures %d/%d)", t.ID, t.Attempt, MaxAttempts)
}

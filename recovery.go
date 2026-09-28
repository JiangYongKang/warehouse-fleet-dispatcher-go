package dispatcher

import "sort"

// Snapshot 导出当前内核状态（用于备份/测试比对）。
func (e *Engine) Snapshot() (Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.buildSnapshotLocked()
}

// buildSnapshotLocked 在锁内组装 v2 快照。
func (e *Engine) buildSnapshotLocked() (Snapshot, error) {
	snap := Snapshot{
		Format: SnapshotFormat,
		Clock:  mapClock(e.clock),
		Seq:    e.seq,
	}
	robotIDs := sortedKeys(e.robots)
	for _, id := range robotIDs {
		st := e.robots[id].Snapshot()
		snap.Robots = append(snap.Robots, robotSnapshot{
			ID: st.ID, Status: string(st.Status), Node: st.Node, CurrentTask: st.CurrentTask,
		})
	}
	for _, t := range e.tasks {
		cp := t.TaskSnapshot()
		snap.Tasks = append(snap.Tasks, taskSnapshot{
			ID: cp.ID, Priority: cp.Priority, From: cp.From, To: cp.To,
			Deps:     cp.Deps,
			Status:   string(cp.Status),
			Assignee: cp.Assignee,
			Attempt:  cp.Attempt,
			Dispatch: cp.Dispatch,
			Seq:      cp.Seq,
			WaitTick: cp.WaitTick,
			Reason:   cp.Reason,
		})
	}
	for _, r := range e.rt.AllReservations() {
		snap.Resv = append(snap.Resv, resvSnapshot{
			RobotID: r.RobotID, TaskID: r.TaskID,
			Kind: string(r.Res.Kind), ResID: r.Res.ID,
			Start: r.Win.Start, End: r.Win.End,
		})
	}
	return snap, nil
}

// Recover 从配置的存储加载快照并覆盖运行时状态。
// 必须在注册任何机器人/提交任何任务之前调用（路网可提前构建）。
func (e *Engine) Recover() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.store == nil {
		return fail("Engine.Recover", ErrInvalidArgument, "no store configured")
	}
	if len(e.robots) != 0 || len(e.tasks) != 0 {
		return fail("Engine.Recover", ErrInvalidState, "engine already contains live data; recover before use")
	}
	snap, err := e.store.Load()
	if err != nil {
		return err // ErrNotFound 由调用方决定是否属于"首次启动"
	}
	return e.restoreLocked(snap)
}

// restoreLocked 用快照重建运行时状态。
// 关键恢复语义：
//   - completed/failed 终态任务原样恢复，永远不会被再次执行；
//   - 重启前 assigned 的任务，其执行进度/在途占用无法保证一致，
//     一律回收为 pending（Attempt/等待计时保留），预约作废后按当前路网重新规划；
//   - 机器人恢复为其最后节点的占用；掉线机器人保持掉线、不带预约；
//   - 时钟与提交序号从快照恢复，保证重放同一批操作结论一致；
//   - 快照中的节点必须仍存在于当前路网，否则拒绝恢复（fast-fail，不猜状态）。
func (e *Engine) restoreLocked(snap Snapshot) error {
	const op = "Engine.restore"
	if snap.Format != SnapshotFormat {
		return fail(op, ErrInvalidArgument, "unsupported snapshot format %q", snap.Format)
	}
	// 1. 校验全部引用节点仍在当前路网。
	for _, r := range snap.Robots {
		if !e.graph.HasNode(r.Node) {
			return fail(op, ErrNotFound, "robot %q snapshot node %q missing from current graph", r.ID, r.Node)
		}
	}
	for _, t := range snap.Tasks {
		if !e.graph.HasNode(t.From) || !e.graph.HasNode(t.To) {
			return fail(op, ErrNotFound, "task %q references node missing from current graph", t.ID)
		}
	}

	// 2. 重建机器人。
	for _, rs := range snap.Robots {
		status := RobotStatus(rs.Status)
		if status == "" {
			status = RobotOnline
		}
		e.robots[rs.ID] = &Robot{state: RobotState{
			ID: rs.ID, Status: status, Node: rs.Node, CurrentTask: rs.CurrentTask,
		}}
	}

	// 3. 重建任务；assigned 回收为 pending（核心安全回收）。
	var recovered int
	for _, ts := range snap.Tasks {
		status := TaskStatus(ts.Status)
		assignee := ts.Assignee
		switch status {
		case TaskAssigned:
			e.logf("recover: task %q was assigned to %q at shutdown; requeue to pending (attempt %d)",
				ts.ID, ts.Assignee, ts.Attempt)
			status = TaskPending
			assignee = ""
			recovered++
		case TaskCompleted, TaskFailed, TaskPending, "":
			if status == "" {
				status = TaskPending
			}
		default:
			return fail(op, ErrInvalidArgument, "task %q has unknown status %q", ts.ID, ts.Status)
		}
		st := &TaskState{
			Task: Task{
				ID: ts.ID, Priority: ts.Priority, From: ts.From, To: ts.To,
				Deps: append([]string(nil), ts.Deps...),
			},
			Status:   status,
			Assignee: assignee,
			Attempt:  ts.Attempt,
			Dispatch: ts.Dispatch,
			Seq:      ts.Seq,
			WaitTick: ts.WaitTick,
			Reason:   ts.Reason,
		}
		e.tasks[ts.ID] = st
	}
	// 4. 修正机器人 CurrentTask 指针：只有仍 assigned 的任务保留。
	for _, r := range e.robots {
		r.state.CurrentTask = ""
	}
	for _, t := range e.tasks {
		if t.Status == TaskAssigned && t.Assignee != "" {
			if r, ok := e.robots[t.Assignee]; ok {
				r.state.CurrentTask = t.ID
			}
		}
	}

	// 5. 时钟、序号与预约：assigned 的在途预约全部丢弃，
	//    只保留空闲机器人对所在节点的占用（逐拍模型下随时可重建）。
	e.clock.load(max64(snap.Clock, 0))
	if snap.Seq > e.seq {
		e.seq = snap.Seq
	}
	e.rt = NewReservationTable()
	now := e.clock.Now()
	for _, r := range e.robots {
		if r.state.Status == RobotOnline {
			e.rt.forcePut(Reservation{RobotID: r.state.ID, Res: nodeRes(r.state.Node),
				Win: Window{now, now + 1}})
		}
	}
	e.exec = map[string]*execution{}
	e.resultLog = map[string]resultRecord{}
	for _, t := range e.tasks {
		if t.Status == TaskCompleted || t.Status == TaskFailed {
			e.resultLog[t.ID] = resultRecord{
				taskID: t.ID, ok: t.Status == TaskCompleted, attempt: t.Attempt, seq: t.Seq,
			}
		}
	}
	e.lastTick = now
	e.logf("recover complete: clock=%d seq=%d robots=%d tasks=%d (%d assigned requeued), reservations rebuilt from clean",
		now, e.seq, len(e.robots), len(e.tasks), recovered)
	return nil
}

func sortedKeys(m map[string]*Robot) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func mapClock(c *Clock) int64 { return c.Now() }
func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

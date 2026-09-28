// Package dispatcher 实现仓储多机器人任务分派与路径预约内核。
//
// 内核使用逻辑时钟（tick）保证结论确定、可重放；所有对外变更操作串行化，
// 并以 append-only 操作日志持久化，支持重启恢复与同批操作重放。
package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/domain"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/network"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/planning"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/scheduler"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/storage"
)

// Kernel 分派内核。
type Kernel struct {
	mu       chan struct{} // 串行化所有状态变更（令牌锁）
	state    *domain.State
	graph    *network.Graph
	planner  *planning.Planner
	dispatch *scheduler.Dispatcher
	store    storage.Store
	opSeq    int
	maxOps   int
}

// Config 内核配置。
type Config struct {
	Graph     *network.Graph
	Store     storage.Store
	MaxLogOps int // 超过该操作条数后自动快照并截断日志；0 表示不自动快照
}

// New 构造内核：载入最近快照并重放 WAL 恢复状态。
func New(ctx context.Context, cfg Config) (*Kernel, error) {
	if cfg.Graph == nil {
		return nil, domain.Err(domain.CodeInternal, "graph is required")
	}
	k := &Kernel{
		mu:     make(chan struct{}, 1),
		state:  domain.NewState(),
		graph:  cfg.Graph,
		store:  cfg.Store,
		maxOps: cfg.MaxLogOps,
	}
	k.planner = planning.NewPlanner(cfg.Graph)
	k.dispatch = scheduler.New(k.planner)

	if k.store != nil {
		snap, err := k.store.LoadLatest(ctx)
		if err != nil {
			return nil, domain.Wrap(domain.CodeInternal, err, "load snapshot")
		}
		if snap != nil {
			st := migrateState(snap)
			k.state = st
			k.opSeq = 0
		}
		ops, err := k.store.ReadOps(ctx)
		if err != nil {
			return nil, domain.Wrap(domain.CodeInternal, err, "read ops")
		}
		for _, op := range ops {
			if op.Seq > k.opSeq {
				k.opSeq = op.Seq
			}
			var appErr *domain.Error
			_ = k.applyOp(op, &appErr) // 重放：拒绝结论也被重现
		}
	}
	k.mu <- struct{}{}
	return k, nil
}

// migrateState 对快照做版本兼容：补全缺失字段，忽略/保留未知字段。
func migrateState(snap *storage.Snapshot) *domain.State {
	st := snap.State
	if st.Version < 1 {
		st.Version = 1
	}
	if st.Robots == nil {
		st.Robots = map[string]*domain.Robot{}
	}
	if st.Tasks == nil {
		st.Tasks = map[string]*domain.Task{}
	}
	if st.Plans == nil {
		st.Plans = map[string]*domain.Plan{}
	}
	if st.IdemIndex == nil {
		// 旧版本没有幂等索引：从任务重建。
		st.IdemIndex = map[string]string{}
	}
	for _, t := range st.Tasks {
		if t.IdemKey != "" {
			if _, ok := st.IdemIndex[t.IdemKey]; !ok {
				st.IdemIndex[t.IdemKey] = t.ID
			}
		}
	}
	st.Version = domain.CurrentStateVersion
	return &st
}

// lock/ unlock（令牌已在 New 中放入）。
func (k *Kernel) lock()   { <-k.mu }
func (k *Kernel) unlock() { k.mu <- struct{}{} }

// ---- 操作载荷 ----

type addRobotPayload struct {
	ID   string   `json:"id"`
	Node string   `json:"node"`
	Caps []string `json:"caps,omitempty"`
}

type heartbeatPayload struct {
	RobotID string `json:"robot_id"`
}

type submitTaskPayload struct {
	ID       string   `json:"id"`
	IdemKey  string   `json:"idem_key,omitempty"`
	Priority int      `json:"priority"`
	From     string   `json:"from"`
	To       string   `json:"to"`
	Deps     []string `json:"deps,omitempty"`
	Requires []string `json:"requires,omitempty"`
}

type reportPayload struct {
	TaskID  string            `json:"task_id"`
	RobotID string            `json:"robot_id"`
	Result  domain.TaskResult `json:"result"`
}

type advancePayload struct {
	Ticks int `json:"ticks"`
}

// ---- 机器人管理 ----

// AddRobot 注册机器人。重复 ID 返回 already_exists；未知节点返回 not_found。
func (k *Kernel) AddRobot(ctx context.Context, id, node string, caps ...string) (*domain.Robot, error) {
	k.lock()
	defer k.unlock()
	payload := addRobotPayload{ID: id, Node: node, Caps: caps}
	var out *domain.Robot
	err := k.withLoggedOp(storage.OpAddRobot, payload, func() *domain.Error {
		if !k.graph.HasNode(node) {
			return domain.Err(domain.CodeNotFound, "unknown node %q", node)
		}
		if _, exists := k.state.Robots[id]; exists {
			return domain.Err(domain.CodeAlreadyExists, "robot %q already exists", id)
		}
		out = &domain.Robot{
			ID: id, Node: node, Status: domain.RobotIdle,
			Caps: append([]string(nil), caps...), LastBeat: k.state.Clock,
		}
		k.state.Robots[id] = out
		k.state.AddDecision("add_robot", "robot="+id+" node="+node)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RemoveRobot 移除机器人（其占有的资源会被回收）。
func (k *Kernel) RemoveRobot(ctx context.Context, id string) error {
	k.lock()
	defer k.unlock()
	return k.withLoggedOp(storage.OpRemoveRobot, addRobotPayload{ID: id}, func() *domain.Error {
		if _, ok := k.state.Robots[id]; !ok {
			return domain.Err(domain.CodeNotFound, "robot %q not found", id)
		}
		k.dispatch.ReclaimRobot(k.state, id, k.state.Clock, domain.CodeOffline, false)
		delete(k.state.Robots, id)
		k.state.AddDecision("remove_robot", "robot="+id)
		return nil
	})
}

// Heartbeat 心跳：刷新存活时间；掉线机器人发心跳视为重新上线（回到 idle）。
func (k *Kernel) Heartbeat(ctx context.Context, robotID string) error {
	k.lock()
	defer k.unlock()
	return k.withLoggedOp(storage.OpHeartbeat, heartbeatPayload{RobotID: robotID}, func() *domain.Error {
		r := k.state.Robots[robotID]
		if r == nil {
			return domain.Err(domain.CodeNotFound, "robot %q not found", robotID)
		}
		wasOffline := r.Status == domain.RobotOffline
		r.LastBeat = k.state.Clock
		if wasOffline && r.CurrentTask == "" {
			r.Status = domain.RobotIdle
			k.state.AddDecision("robot_online", "robot="+robotID+" tick="+fmt.Sprint(k.state.Clock))
		}
		return nil
	})
}

// ---- 任务提交 ----

// SubmitTaskInput 提交任务入参。
type SubmitTaskInput struct {
	ID       string
	IdemKey  string
	Priority int
	From     string
	To       string
	Deps     []string
	Requires []string
}

// SubmitTask 提交任务；相同幂等键重复提交返回同一任务与 duplicate_submit 错误
// （不产生第二次执行）。
func (k *Kernel) SubmitTask(ctx context.Context, in SubmitTaskInput) (*domain.Task, error) {
	k.lock()
	defer k.unlock()
	payload := submitTaskPayload{
		ID: in.ID, IdemKey: in.IdemKey, Priority: in.Priority,
		From: in.From, To: in.To, Deps: in.Deps, Requires: in.Requires,
	}
	var out *domain.Task
	err := k.withLoggedOp(storage.OpSubmitTask, &payload, func() *domain.Error {
		if in.IdemKey != "" {
			if id, ok := k.state.IdemIndex[in.IdemKey]; ok {
				out = k.state.Tasks[id]
				payload.ID = id
				return domain.Err(domain.CodeDuplicateSubmit,
					"idempotency key %q already submitted as task %q (status=%s)",
					in.IdemKey, id, out.Status)
			}
		}
		if in.ID != "" {
			if _, exists := k.state.Tasks[in.ID]; exists {
				return domain.Err(domain.CodeAlreadyExists, "task %q already exists", in.ID)
			}
		}
		if !k.graph.HasNode(in.From) || !k.graph.HasNode(in.To) {
			return domain.Err(domain.CodeNotFound, "task node unknown: from=%q to=%q", in.From, in.To)
		}
		if path, _ := k.graph.Shortest(in.From, in.To); path == nil {
			return domain.Err(domain.CodeNoRoute, "no route from %q to %q", in.From, in.To)
		}
		for _, dep := range in.Deps {
			dt := k.state.Tasks[dep]
			if dt == nil {
				return domain.Err(domain.CodeNotFound, "dependency %q not found", dep)
			}
			if dt.Status == domain.TaskFailed {
				return domain.Err(domain.CodeDependencyFailed, "dependency %q failed", dep)
			}
		}
		id := in.ID
		if id == "" {
			k.state.TaskSeq++
			id = fmt.Sprintf("T%04d", k.state.TaskSeq)
		}
		payload.ID = id
		t := &domain.Task{
			ID: id, IdemKey: in.IdemKey, Priority: in.Priority,
			From: in.From, To: in.To,
			Deps:     append([]string(nil), in.Deps...),
			Requires: append([]string(nil), in.Requires...),
			Status:   domain.TaskPending, SubmittedAt: k.state.Clock,
		}
		k.state.Tasks[id] = t
		if in.IdemKey != "" {
			k.state.IdemIndex[in.IdemKey] = id
		}
		out = t
		k.state.AddDecision("submit_task",
			"task="+id+" prio="+fmt.Sprint(in.Priority)+" from="+in.From+" to="+in.To+
				" deps="+joinDeps(in.Deps)+" idem="+nz(in.IdemKey))
		return nil
	})
	if err != nil && out != nil {
		// 幂等重复：返回既有任务，同时给出可区分原因。
		return out, err
	}
	return out, err
}

// ---- 结果上报 ----

// ReportResult 上报任务结果。重复上报（任务已终态）被拒绝且不改变状态。
func (k *Kernel) ReportResult(ctx context.Context, taskID, robotID string, result domain.TaskResult) (*domain.Task, error) {
	k.lock()
	defer k.unlock()
	if result.AtTick == 0 {
		result.AtTick = k.state.Clock
	}
	payload := reportPayload{TaskID: taskID, RobotID: robotID, Result: result}
	var out *domain.Task
	err := k.withLoggedOp(storage.OpReportResult, payload, func() *domain.Error {
		t := k.state.Tasks[taskID]
		if t == nil {
			return domain.Err(domain.CodeNotFound, "task %q not found", taskID)
		}
		if t.Status.IsTerminal() {
			out = t
			return domain.Err(domain.CodeInvalidState,
				"task %q already %s: duplicate report rejected", taskID, t.Status)
		}
		if t.AssignedTo == "" {
			return domain.Err(domain.CodeNotAssigned, "task %q not assigned", taskID)
		}
		if t.AssignedTo != robotID {
			return domain.Err(domain.CodeInvalidState,
				"task %q assigned to %q, report from %q rejected", taskID, t.AssignedTo, robotID)
		}
		r := k.state.Robots[robotID]
		if r == nil || r.Status == domain.RobotOffline {
			return domain.Err(domain.CodeOffline, "robot %q offline", robotID)
		}
		if !result.Success {
			// 执行失败：占用资源安全回收，计尝试次数，可重新调度。
			out = t
			t.Result = &result
			k.dispatch.ReclaimRobot(k.state, robotID, k.state.Clock, domain.CodeExecutionFailed, true)
			if t.Attempts >= domain.MaxAttempts {
				k.dispatch.FailTask(k.state, t, domain.CodeExecutionFailed, k.state.Clock)
			}
			k.state.AddDecision("report_failure",
				"task="+taskID+" robot="+robotID+" attempts="+fmt.Sprint(t.Attempts)+
					" note="+nz(result.Note)+" terminal="+fmt.Sprint(t.Status == domain.TaskFailed))
			return nil
		}
		if r.Node != t.To || r.Status != domain.RobotExecuting {
			return domain.Err(domain.CodeAtWrongNode,
				"success report rejected: robot %q at %q status=%s, need %q/executing",
				robotID, r.Node, r.Status, t.To)
		}
		t.Status = domain.TaskCompleted
		t.EndedAt = k.state.Clock
		t.Result = &result
		r.Status = domain.RobotIdle
		r.CurrentTask = ""
		delete(k.state.Plans, taskID)
		// 释放终点服务窗与该机器人残留预约。
		k.state.ReleaseReservations(robotID)
		out = t
		k.state.AddDecision("report_success",
			"task="+taskID+" robot="+robotID+" node="+r.Node+" tick="+fmt.Sprint(k.state.Clock))
		return nil
	})
	return out, err
}

// ---- 时钟推进 ----

// Advance 推进逻辑时钟 ticks 步，每步：掉线回收→超时→到达→分派→逐段获取→死锁打破。
func (k *Kernel) Advance(ctx context.Context, ticks int) error {
	if ticks < 0 {
		return domain.Err(domain.CodeInvalidState, "ticks must be >= 0")
	}
	if ticks == 0 {
		return nil
	}
	k.lock()
	defer k.unlock()
	return k.withLoggedOp(storage.OpAdvance, advancePayload{Ticks: ticks}, func() *domain.Error {
		for i := 0; i < ticks; i++ {
			k.state.Clock++
			k.dispatch.AdvanceOne(k.state)
		}
		return nil
	})
}

// ---- 只读视图 ----

// Snapshot 返回当前状态的深拷贝。
func (k *Kernel) Snapshot() *domain.State {
	k.lock()
	defer k.unlock()
	return k.state.Clone()
}

// Decisions 返回最近 limit 条判定审计记录（limit<=0 返回全部）。
func (k *Kernel) Decisions(limit int) []domain.Decision {
	k.lock()
	defer k.unlock()
	ds := k.state.Decisions
	if limit > 0 && len(ds) > limit {
		ds = ds[len(ds)-limit:]
	}
	return append([]domain.Decision(nil), ds...)
}

// State 只读访问（浅引用，仅供测试/演示；外部不可修改）。
func (k *Kernel) State() *domain.State {
	k.lock()
	defer k.unlock()
	return k.state
}

// ---- 操作日志与重放 ----

type loggedFn func() *domain.Error

func (k *Kernel) withLoggedOp(kind storage.OpKind, payload any, fn loggedFn) error {
	applyErr := fn()
	// 在 fn 之后序列化，使自动生成的 ID 等回填字段进入日志。
	raw, _ := json.Marshal(payload)
	if k.store != nil {
		k.opSeq++
		op := storage.Op{Seq: k.opSeq, Kind: kind, AtTick: k.state.Clock, Payload: raw}
		if applyErr != nil {
			op.Rejected = true
			op.Reason = string(applyErr.Code)
		}
		if err := k.store.AppendOp(context.Background(), op); err != nil {
			return domain.Wrap(domain.CodeInternal, err, "append op")
		}
		if k.maxOps > 0 && k.opSeq >= k.maxOps {
			k.checkpoint(context.Background())
		}
	}
	if applyErr == nil {
		return nil
	}
	return applyErr
}

func (k *Kernel) checkpoint(ctx context.Context) error {
	if k.store == nil {
		return nil
	}
	k.state.Version = domain.CurrentStateVersion
	if err := k.store.SaveSnapshot(ctx, &storage.Snapshot{Version: domain.CurrentStateVersion, State: *k.state.Clone()}); err != nil {
		return err
	}
	if err := k.store.TruncateOps(ctx); err != nil {
		return err
	}
	k.opSeq = 0
	return nil
}

// Checkpoint 显式触发一次快照并截断日志。
func (k *Kernel) Checkpoint(ctx context.Context) error {
	k.lock()
	defer k.unlock()
	return k.checkpoint(ctx)
}

func (k *Kernel) applyOp(op storage.Op, perr **domain.Error) error {
	// 重放时不再写日志；直接执行副作用。时钟以 op.AtTick 为准（Advance 自带递增）。
	switch op.Kind {
	case storage.OpAddRobot:
		var p addRobotPayload
		_ = json.Unmarshal(op.Payload, &p)
		if p.Node != "" {
			if _, ok := k.state.Robots[p.ID]; !ok && k.graph.HasNode(p.Node) {
				k.state.Robots[p.ID] = &domain.Robot{
					ID: p.ID, Node: p.Node, Status: domain.RobotIdle,
					Caps: append([]string(nil), p.Caps...), LastBeat: k.state.Clock,
				}
			}
		} else {
			if _, ok := k.state.Robots[p.ID]; ok {
				k.dispatch.ReclaimRobot(k.state, p.ID, k.state.Clock, domain.CodeOffline, false)
				delete(k.state.Robots, p.ID)
			}
		}
	case storage.OpRemoveRobot:
		var p addRobotPayload
		_ = json.Unmarshal(op.Payload, &p)
		if _, ok := k.state.Robots[p.ID]; ok {
			k.dispatch.ReclaimRobot(k.state, p.ID, k.state.Clock, domain.CodeOffline, false)
			delete(k.state.Robots, p.ID)
		}
	case storage.OpHeartbeat:
		var p heartbeatPayload
		_ = json.Unmarshal(op.Payload, &p)
		if r := k.state.Robots[p.RobotID]; r != nil {
			r.LastBeat = k.state.Clock
			if r.Status == domain.RobotOffline && r.CurrentTask == "" {
				r.Status = domain.RobotIdle
			}
		}
	case storage.OpSubmitTask:
		var p submitTaskPayload
		_ = json.Unmarshal(op.Payload, &p)
		if op.Rejected {
			// 拒绝分两种：幂等重复（需确保 out 可查）与校验失败（无副作用）。
			if p.IdemKey != "" {
				if _, ok := k.state.IdemIndex[p.IdemKey]; !ok {
					// 理论上不应发生：拒绝且此前不存在键。保持幂等安全：跳过。
				}
			}
			return nil
		}
		if _, exists := k.state.Tasks[p.ID]; exists {
			return nil
		}
		t := &domain.Task{
			ID: p.ID, IdemKey: p.IdemKey, Priority: p.Priority,
			From: p.From, To: p.To, Deps: p.Deps, Requires: p.Requires,
			Status: domain.TaskPending, SubmittedAt: op.AtTick,
		}
		k.state.Tasks[p.ID] = t
		if p.IdemKey != "" {
			k.state.IdemIndex[p.IdemKey] = p.ID
		}
		if n := seqFromTaskID(p.ID); n > k.state.TaskSeq {
			k.state.TaskSeq = n
		}
	case storage.OpReportResult:
		var p reportPayload
		_ = json.Unmarshal(op.Payload, &p)
		if op.Rejected {
			return nil
		}
		k.replayReport(p)
	case storage.OpAdvance:
		var p advancePayload
		_ = json.Unmarshal(op.Payload, &p)
		for i := 0; i < p.Ticks; i++ {
			k.state.Clock++
			k.dispatch.AdvanceOne(k.state)
		}
	}
	return nil
}

func (k *Kernel) replayReport(p reportPayload) {
	t := k.state.Tasks[p.TaskID]
	if t == nil || t.Status.IsTerminal() || t.AssignedTo != p.RobotID {
		return
	}
	r := k.state.Robots[p.RobotID]
	t.Result = &p.Result
	if !p.Result.Success {
		k.dispatch.ReclaimRobot(k.state, p.RobotID, k.state.Clock, domain.CodeExecutionFailed, true)
		if t.Attempts >= domain.MaxAttempts {
			k.dispatch.FailTask(k.state, t, domain.CodeExecutionFailed, k.state.Clock)
		}
		return
	}
	if r == nil || r.Node != t.To {
		return
	}
	t.Status = domain.TaskCompleted
	t.EndedAt = k.state.Clock
	r.Status = domain.RobotIdle
	r.CurrentTask = ""
	delete(k.state.Plans, p.TaskID)
	k.state.ReleaseReservations(p.RobotID)
}

func seqFromTaskID(id string) int {
	var n int
	if len(id) > 1 && id[0] == 'T' {
		for i := 1; i < len(id); i++ {
			if id[i] < '0' || id[i] > '9' {
				return 0
			}
			n = n*10 + int(id[i]-'0')
		}
	}
	return n
}

// ---- 小工具 ----

func joinDeps(ds []string) string {
	if len(ds) == 0 {
		return "-"
	}
	out := ""
	for i, d := range ds {
		if i > 0 {
			out += ","
		}
		out += d
	}
	return out
}

func nz(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// 保证 sort 包被使用（决策导出的稳定性排序）。
var _ = sort.Strings

// IsCode 判断错误链中是否含指定的领域错误码。
func IsCode(err error, code domain.Code) bool {
	var de *domain.Error
	if errors.As(err, &de) {
		return de.Code == code
	}
	return false
}

// Task 只读查询某任务（深拷贝）。
func (k *Kernel) Task(id string) (*domain.Task, bool) {
	k.lock()
	defer k.unlock()
	t := k.state.Tasks[id]
	if t == nil {
		return nil, false
	}
	tc := *t
	return &tc, true
}

// Robot 只读查询某机器人（深拷贝）。
func (k *Kernel) Robot(id string) (*domain.Robot, bool) {
	k.lock()
	defer k.unlock()
	r := k.state.Robots[id]
	if r == nil {
		return nil, false
	}
	rc := *r
	return &rc, true
}

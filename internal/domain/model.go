package domain

// RobotStatus 机器人状态。
type RobotStatus string

const (
	RobotOffline   RobotStatus = "offline"
	RobotIdle      RobotStatus = "idle"
	RobotBusy      RobotStatus = "busy"      // 已派单，正在移动或等待
	RobotExecuting RobotStatus = "executing" // 已到达任务终点，等待上报结果
)

// IsBusy 是否处于占用任务的状态。
func (s RobotStatus) IsBusy() bool {
	return s == RobotBusy || s == RobotExecuting
}

// TaskStatus 任务状态。
type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"
	TaskAssigned  TaskStatus = "assigned"
	TaskRunning   TaskStatus = "running" // 机器人正在移动/等待（已离开起点）
	TaskCompleted TaskStatus = "completed"
	TaskFailed    TaskStatus = "failed"
	TaskPreempted TaskStatus = "preempted" // 因死锁被抢占，回到 pending 重新调度
)

// IsTerminal 是否为不可再变更的终态。
func (s TaskStatus) IsTerminal() bool {
	return s == TaskCompleted || s == TaskFailed
}

// PlanMode 最近一次冲突消解方式（可解释性审计）。
type PlanMode string

const (
	PlanDirect  PlanMode = "direct"
	PlanWait    PlanMode = "wait"
	PlanReroute PlanMode = "reroute"
	PlanPreempt PlanMode = "preempt"
)

// PlanState 计划在单个 tick 上的微观状态。
type PlanState string

const (
	PlanAtNode PlanState = "at_node" // 位于节点，下一 tick 可申请下一段
	PlanMoving PlanState = "moving"  // 正在通过边
)

// Robot 机器人聚合。
type Robot struct {
	ID          string      `json:"id"`
	Node        string      `json:"node"`
	Status      RobotStatus `json:"status"`
	CurrentTask string      `json:"current_task,omitempty"`
	Caps        []string    `json:"caps,omitempty"`
	LastBeat    int64       `json:"last_beat"`
}

// Task 任务聚合。
type Task struct {
	ID           string     `json:"id"`
	IdemKey      string     `json:"idem_key,omitempty"`
	Priority     int        `json:"priority"` // 数值越大优先级越高
	From         string     `json:"from"`
	To           string     `json:"to"`
	Status       TaskStatus `json:"status"`
	AssignedTo   string     `json:"assigned_to,omitempty"`
	Deps         []string   `json:"deps,omitempty"`
	Requires     []string   `json:"requires,omitempty"`
	SubmittedAt  int64      `json:"submitted_at"`
	StartedAt    int64      `json:"started_at,omitempty"`
	EndedAt      int64      `json:"ended_at,omitempty"`
	Attempts     int        `json:"attempts"`
	BackoffUntil int64      `json:"backoff_until,omitempty"`
	// PickedUp 机器人是否已到达取货点 From（恢复路径时决定是否还需空驶段）。
	PickedUp bool        `json:"picked_up,omitempty"`
	Result   *TaskResult `json:"result,omitempty"`
	// FailedReasonCode 终态失败原因码。
	FailedReasonCode Code `json:"failed_reason_code,omitempty"`
}

// TaskResult 任务执行结果上报。
type TaskResult struct {
	Success bool   `json:"success"`
	Note    string `json:"note,omitempty"`
	AtTick  int64  `json:"at_tick"`
}

// Reservation 对节点或有向边在半开时间窗 [Start,End) 上的独占占用。
// End 为 SentinelForever 表示持续到显式释放（终点服务窗/停车窗）。
type Reservation struct {
	RobotID  string `json:"robot_id"`
	TaskID   string `json:"task_id,omitempty"` // 空表示停车占用（不属于任何任务）
	Resource string `json:"resource"`
	Start    int64  `json:"start"`
	End      int64  `json:"end"`
}

// SentinelForever 无限长占用窗的结束时刻。
const SentinelForever int64 = 1 << 62

// PlanStep 计划路径上的一步（边 u->v）。
type PlanStep struct {
	From string `json:"from"`
	To   string `json:"to"`
	Cost int    `json:"cost"`
}

// Plan 机器人逐段推进的路径计划与运行时游标。
type Plan struct {
	TaskID  string    `json:"task_id"`
	RobotID string    `json:"robot_id"`
	Path    []string  `json:"path"` // 完整节点序列（含改道后的更新）
	Idx     int       `json:"idx"`  // 当前所在节点在 Path 中的下标
	State   PlanState `json:"state"`
	Mode    PlanMode  `json:"mode"` // 最近一次冲突消解方式
	// EnteredAt 进入当前节点的时刻；Moving 时为边的起始时刻。
	EnteredAt int64 `json:"entered_at"`
	// EdgeUntil 移动状态下到达下一节点的时刻。
	EdgeUntil int64 `json:"edge_until,omitempty"`
	// WaitingSince 因冲突开始等待的时刻（用于等待审计/老化）。
	WaitingSince int64 `json:"waiting_since,omitempty"`
	// ReleaseAt 到达终点的时刻。
	ReleaseAt int64 `json:"release_at,omitempty"`
	// Victims 最近一次抢占回收的机器人。
	Victims []string `json:"victims,omitempty"`
}

// Decision 审计日志：记录关键判定依据。
type Decision struct {
	At     int64  `json:"at"`
	Kind   string `json:"kind"`
	Detail string `json:"detail"`
}

// State 是内核的全部可持久化状态（快照 v1）。
type State struct {
	Version      int               `json:"version"`
	Clock        int64             `json:"clock"`
	TaskSeq      int               `json:"task_seq"`
	Robots       map[string]*Robot `json:"robots"`
	Tasks        map[string]*Task  `json:"tasks"`
	Plans        map[string]*Plan  `json:"plans"` // taskID -> 有效计划
	Reservations []Reservation     `json:"reservations"`
	Decisions    []Decision        `json:"decisions"`
	// IdemIndex 幂等键 -> 任务 ID。
	IdemIndex map[string]string `json:"idem_index,omitempty"`
	// Extra 用于前后版本兼容：未知字段统一落在这里。
	Extra map[string]interface{} `json:"extra,omitempty"`
}

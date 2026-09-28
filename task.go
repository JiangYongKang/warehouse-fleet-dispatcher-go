package dispatcher

// TaskStatus 任务生命周期状态。
type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"   // 已提交，未分派
	TaskAssigned  TaskStatus = "assigned"  // 已分派，执行中
	TaskCompleted TaskStatus = "completed" // 成功完成
	TaskFailed    TaskStatus = "failed"    // 执行失败（终态）
)

// Task 是一个搬运任务：从 From 到 To，带优先级与前置依赖。
type Task struct {
	ID       string
	Priority int      // 越大优先级越高；同优先级按提交顺序（ID 字典序保证确定性）
	From     string   // 起点节点
	To       string   // 终点节点
	Deps     []string // 前置任务 ID，全部完成后方可分派
}

// TaskState 是任务的完整运行时状态。
type TaskState struct {
	Task
	Status   TaskStatus
	Assignee string // 被分派给的机器人 ID
	Attempt  int    // 机器人上报"执行失败"的次数（调度抢占/掉线回收不计入）
	Dispatch int    // 被分派（含抢占/掉线/重启后重派）的次数，纯观测用
	Seq      int64  // 单调提交序号，保证同优先级下 FIFO 与防饿死
	WaitTick int64  // 进入等待的最早 tick（提交后即记录，供老化补偿）
	Reason   string // 终态（failed）原因说明
}

// TaskSnapshot 返回任务状态拷贝（含 Deps 的拷贝）。
func (t *TaskState) TaskSnapshot() TaskState {
	cp := *t
	cp.Deps = append([]string(nil), t.Deps...)
	return cp
}

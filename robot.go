package dispatcher

// RobotStatus 机器人在线状态。
type RobotStatus string

const (
	RobotOnline  RobotStatus = "online"
	RobotOffline RobotStatus = "offline"
)

// RobotState 是机器人在调度内核中的状态（不含执行中的任务引用细节）。
type RobotState struct {
	ID          string
	Status      RobotStatus
	Node        string // 当前所在节点（空闲或掉线时的位置）
	CurrentTask string // 正在执行的任务 ID，空表示空闲
}

// Robot 是一台机器人的运行时记录。
type Robot struct {
	state RobotState
}

// Snapshot 返回机器人状态的拷贝。
func (r *Robot) Snapshot() RobotState { return r.state }

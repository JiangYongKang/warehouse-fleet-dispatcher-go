package storage

import "encoding/json"

// OpKind 操作类型（重放时按类型分发）。
type OpKind string

const (
	OpAddRobot     OpKind = "add_robot"
	OpRemoveRobot  OpKind = "remove_robot"
	OpSubmitTask   OpKind = "submit_task"
	OpHeartbeat    OpKind = "heartbeat"
	OpReportResult OpKind = "report_result"
	OpAdvance      OpKind = "advance"
)

// Op 是一条确定性的状态变更操作（append-only JSONL）。
type Op struct {
	Seq     int             `json:"seq"`
	Kind    OpKind          `json:"kind"`
	AtTick  int64           `json:"at_tick"`
	Payload json.RawMessage `json:"payload"`
	// Rejected 标记该操作应用时是否被拒绝（重放需重现拒绝结论）。
	Rejected bool   `json:"rejected,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

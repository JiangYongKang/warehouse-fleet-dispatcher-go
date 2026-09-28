package domain

import "fmt"

// Code 是可区分的拒绝原因码。
type Code string

const (
	CodeInternal          Code = "internal"
	CodeNotFound          Code = "not_found"
	CodeAlreadyExists     Code = "already_exists"
	CodeInvalidState      Code = "invalid_state"
	CodeDuplicateSubmit   Code = "duplicate_submit"
	CodeDependencyPending Code = "dependency_pending"
	CodeDependencyFailed  Code = "dependency_failed"
	CodeNoEligibleRobot   Code = "no_eligible_robot"
	CodeNoRoute           Code = "no_route"
	CodeConflict          Code = "conflict"
	CodeDeadlockVictim    Code = "deadlock_victim"
	CodeTaskTimeout       Code = "task_timeout"
	CodeOffline           Code = "robot_offline"
	CodeAtWrongNode       Code = "at_wrong_node"
	CodeExecutionFailed   Code = "execution_failed"
	CodeNotAssigned       Code = "not_assigned"
)

// Error 携带稳定的错误码，便于调用方按原因区分。
type Error struct {
	Code    Code
	Message string
	Cause   error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.Cause }

func Err(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

func Wrap(code Code, cause error, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), Cause: cause}
}

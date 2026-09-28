package dispatcher

import "fmt"

// ErrorCode 用稳定的字符串标识可区分的拒绝原因，便于调用方判定与日志检索。
type ErrorCode string

const (
	ErrInvalidArgument ErrorCode = "invalid_argument"
	ErrNotFound        ErrorCode = "not_found"
	ErrAlreadyExists   ErrorCode = "already_exists"
	ErrInvalidState    ErrorCode = "invalid_state"
	ErrDepsUnmet       ErrorCode = "deps_unmet"
	ErrAlreadyDone     ErrorCode = "already_done"
	ErrConflict        ErrorCode = "conflict"
	ErrDeadlock        ErrorCode = "deadlock"
	ErrNoPath          ErrorCode = "no_path"
	ErrStaleResult     ErrorCode = "stale_result"
)

// Error 携带操作名、错误码与解释信息，保证非法流转的原因可区分。
type Error struct {
	Code ErrorCode
	Op   string
	Msg  string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s: %s", e.Op, e.Code, e.Msg)
}

func fail(op string, code ErrorCode, format string, args ...any) error {
	return &Error{Code: code, Op: op, Msg: fmt.Sprintf(format, args...)}
}

// ErrorCodeOf 提取错误码，非本包错误返回空字符串。
func ErrorCodeOf(err error) ErrorCode {
	if err == nil {
		return ""
	}
	if e, ok := err.(*Error); ok {
		return e.Code
	}
	return ""
}

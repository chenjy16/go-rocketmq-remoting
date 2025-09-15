package errors

import (
	"fmt"
)

// RemotingError 远程调用错误
type RemotingError struct {
	Code    int
	Message string
	Err     error
}

// Error 实现error接口
func (e *RemotingError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("RemotingError[code=%d, message=%s]: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("RemotingError[code=%d, message=%s]", e.Code, e.Message)
}

// Unwrap 返回底层错误
func (e *RemotingError) Unwrap() error {
	return e.Err
}

// Error codes
const (
	// Common error codes
	ErrCodeInvalidAddress    = 1001
	ErrCodeConnectionFailed  = 1002
	ErrCodeConnectionTimeout = 1003
	ErrCodePoolClosed        = 1004
	ErrCodeEncodeFailed      = 1005
	ErrCodeDecodeFailed      = 1006
	ErrCodeInvalidResponse   = 1007
	ErrCodeInvalidRequest    = 1008
	ErrCodeNetworkError      = 1009

	// Server error codes
	ErrCodeServerStartFailed    = 2001
	ErrCodeServerStopFailed     = 2002
	ErrCodeServerListenFailed   = 2003
	ErrCodeServerProcessFailed  = 2004
	ErrCodeServerEncodeFailed   = 2005
	ErrCodeServerDecodeFailed   = 2006
	ErrCodeServerInvalidRequest = 2007
	ErrCodeServerNetworkError   = 2008
)

// NewRemotingError 创建远程调用错误
func NewRemotingError(code int, message string, err error) *RemotingError {
	return &RemotingError{
		Code:    code,
		Message: message,
		Err:     err,
	}
}

// IsConnectionError 检查是否为连接错误
func (e *RemotingError) IsConnectionError() bool {
	switch e.Code {
	case ErrCodeInvalidAddress, ErrCodeConnectionFailed, ErrCodeConnectionTimeout:
		return true
	default:
		return false
	}
}

// IsTimeoutError 检查是否为超时错误
func (e *RemotingError) IsTimeoutError() bool {
	return e.Code == ErrCodeConnectionTimeout
}

// IsServerError 检查是否为服务端错误
func (e *RemotingError) IsServerError() bool {
	return e.Code >= ErrCodeServerStartFailed && e.Code <= ErrCodeServerNetworkError
}

// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
package mooncakeconductor

import "fmt"

type ErrorCode int32

const (
	InternalError        ErrorCode = -1
	InvalidParams        ErrorCode = -600
	RPCFail              ErrorCode = -900
	RPCTimeout           ErrorCode = -901
	ConductorUnavailable ErrorCode = -2000
	ServiceNotFound      ErrorCode = -2001
)

// Error retains the numeric native/RPC code. Use errors.As to inspect it.
type Error struct {
	Op   string
	Code ErrorCode
}

func (e *Error) Error() string { return fmt.Sprintf("mooncakeconductor: %s: error %d", e.Op, e.Code) }
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t != nil && e.Code == t.Code
}

func nativeError(op string, code int32) error {
	if code == 0 {
		return nil
	}
	return &Error{Op: op, Code: ErrorCode(code)}
}

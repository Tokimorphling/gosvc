package jsonrpc

import (
	"example.com/gosvc/apierror"
)

// Standard JSON-RPC 2.0 error codes.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternal       = -32603
)

// Application error codes, from the implementation-defined range -32000..-32099.
const (
	CodeNotFound         = -32001
	CodeConflict         = -32002
	CodePermissionDenied = -32003
	CodeUnavailable      = -32004
	CodeRateLimited      = -32005
	CodeUnauthenticated  = -32006
)

// codeOf maps an application error onto a JSON-RPC error code.
func codeOf(err error) (int, string) {
	switch apierror.KindOf(err) {
	case apierror.KindInvalidArgument:
		return CodeInvalidParams, apierror.ClientMessage(err)
	case apierror.KindUnauthenticated:
		return CodeUnauthenticated, apierror.ClientMessage(err)
	case apierror.KindNotFound:
		return CodeNotFound, apierror.ClientMessage(err)
	case apierror.KindConflict:
		return CodeConflict, apierror.ClientMessage(err)
	case apierror.KindPermissionDenied:
		return CodePermissionDenied, apierror.ClientMessage(err)
	case apierror.KindRateLimited:
		return CodeRateLimited, apierror.ClientMessage(err)
	case apierror.KindUnavailable:
		return CodeUnavailable, apierror.ClientMessage(err)
	default:
		return CodeInternal, apierror.ClientMessage(err)
	}
}

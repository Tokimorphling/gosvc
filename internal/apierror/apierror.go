// Package apierror defines transport-agnostic application errors.
//
// Handlers return *apierror.Error values; each transport maps them onto its own
// error representation (HTTP status codes, JSON-RPC error codes, gRPC status
// codes) in a single place.
package apierror

import (
	"errors"
	"fmt"
)

// Kind classifies an error independently of any transport.
type Kind string

const (
	KindUnknown          Kind = "unknown"
	KindInvalidArgument  Kind = "invalid_argument"
	KindNotFound         Kind = "not_found"
	KindConflict         Kind = "conflict"
	KindPermissionDenied Kind = "permission_denied"
	KindRateLimited      Kind = "rate_limited"
	KindUnavailable      Kind = "unavailable"
	KindInternal         Kind = "internal"
)

// Error is an application error carrying a transport-agnostic Kind.
type Error struct {
	Kind    Kind
	Message string
	Op      string
	Err     error
}

// New creates an Error with the given kind and message.
func New(kind Kind, message string) *Error {
	return &Error{Kind: kind, Message: message}
}

// Newf creates an Error with a formatted message.
func Newf(kind Kind, format string, args ...any) *Error {
	return &Error{Kind: kind, Message: fmt.Sprintf(format, args...)}
}

// Wrap attaches a kind and message to err. When err already is an *Error its
// kind is preserved and only the message is extended.
func Wrap(err error, kind Kind, message string) *Error {
	if err == nil {
		return nil
	}
	var ae *Error
	if errors.As(err, &ae) {
		return &Error{Kind: ae.Kind, Message: message + ": " + ae.Message, Op: ae.Op, Err: err}
	}
	return &Error{Kind: kind, Message: message, Err: err}
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	switch {
	case e.Op != "" && e.Err != nil:
		return fmt.Sprintf("%s: %s: %v", e.Op, e.Message, e.Err)
	case e.Op != "" && e.Message != "":
		return e.Op + ": " + e.Message
	case e.Err != nil && e.Message != "":
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	case e.Err != nil:
		return e.Err.Error()
	case e.Message != "":
		return e.Message
	default:
		return string(e.Kind)
	}
}

// Unwrap supports errors.Is / errors.As.
func (e *Error) Unwrap() error { return e.Err }

// KindOf extracts the Kind from an error chain.
func KindOf(err error) Kind {
	var ae *Error
	if errors.As(err, &ae) {
		return ae.Kind
	}
	return KindUnknown
}

// ClientMessage returns a message that is safe to expose to clients. Internal
// and unknown errors are replaced by a generic message.
func ClientMessage(err error) string {
	var ae *Error
	if errors.As(err, &ae) {
		switch ae.Kind {
		case KindInternal, KindUnknown:
			return "internal server error"
		}
		if ae.Message != "" {
			return ae.Message
		}
	}
	return "internal server error"
}

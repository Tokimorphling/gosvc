// Package http contains the REST and JSON-RPC transport built on Hertz.
package http

import (
	"context"
	"log/slog"

	"github.com/bytedance/sonic"
	"github.com/cloudwego/hertz/pkg/app"

	"github.com/Tokimorphling/gosvc/apierror"
	"github.com/Tokimorphling/gosvc/logging"
)

type errorBody struct {
	Error errorInfo `json:"error"`
}

type errorInfo struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"requestId,omitempty"`
}

func writeJSON(c *app.RequestContext, status int, v any) {
	raw, err := sonic.Marshal(v)
	if err != nil {
		writeRawJSON(c, 500, []byte(`{"error":{"code":"internal","message":"failed to encode response"}}`))
		return
	}
	writeRawJSON(c, status, raw)
}

func writeRawJSON(c *app.RequestContext, status int, raw []byte) {
	c.SetStatusCode(status)
	c.SetContentType("application/json; charset=utf-8")
	_, _ = c.Write(raw)
}

// WriteError maps a domain error onto an HTTP status and JSON body.
func WriteError(ctx context.Context, c *app.RequestContext, err error) {
	kind := apierror.KindOf(err)
	writeJSON(c, statusFromKind(kind), errorBody{Error: errorInfo{
		Code:      string(kind),
		Message:   apierror.ClientMessage(err),
		RequestID: logging.RequestID(ctx),
	}})
}

func statusFromKind(kind apierror.Kind) int {
	switch kind {
	case apierror.KindInvalidArgument:
		return 400
	case apierror.KindUnauthenticated:
		return 401
	case apierror.KindNotFound:
		return 404
	case apierror.KindConflict:
		return 409
	case apierror.KindPermissionDenied:
		return 403
	case apierror.KindRateLimited:
		return 429
	case apierror.KindUnavailable:
		return 503
	default:
		return 500
	}
}

// writeInternalError is used by middleware that has no domain error.
func writeInternalError(ctx context.Context, c *app.RequestContext, message string) {
	writeJSON(c, 500, errorBody{Error: errorInfo{
		Code:      string(apierror.KindInternal),
		Message:   message,
		RequestID: logging.RequestID(ctx),
	}})
}

// logError preserves the request context for context-aware logging handlers.
func logError(ctx context.Context, logger *slog.Logger, msg string, args ...any) {
	if logger == nil {
		return
	}
	logger.ErrorContext(ctx, msg, args...)
}

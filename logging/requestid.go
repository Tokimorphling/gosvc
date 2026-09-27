package logging

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	"go.opentelemetry.io/otel/trace"
)

const (
	// RequestIDHeader is the HTTP header carrying the request id.
	RequestIDHeader = "X-Request-ID"
	// RequestIDMetadataKey is the gRPC metadata key carrying the request id.
	RequestIDMetadataKey = "x-request-id"
)

type requestIDKey struct{}

// NewRequestID returns a random 128-bit request id as 32 hex characters.
func NewRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "00000000000000000000000000000000"
	}
	return hex.EncodeToString(b[:])
}

// WithRequestID stores the request id in the context.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestID returns the request id stored in the context, or an empty string.
func RequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if id, ok := ctx.Value(requestIDKey{}).(string); ok {
		return id
	}
	return ""
}

// RequestAttrs returns the request-scoped attributes available in ctx:
// request_id and, when tracing is active, trace_id and span_id. It is used by
// transports that write access logs to a sink outside the context logger.
func RequestAttrs(ctx context.Context) []any {
	attrs := make([]any, 0, 6)
	if id := RequestID(ctx); id != "" {
		attrs = append(attrs, "request_id", id)
	}
	if spanContext := trace.SpanContextFromContext(ctx); spanContext.IsValid() {
		attrs = append(attrs,
			"trace_id", spanContext.TraceID().String(),
			"span_id", spanContext.SpanID().String(),
		)
	}
	return attrs
}

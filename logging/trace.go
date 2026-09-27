package logging

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// WithTrace enriches the context logger with the active span identifiers so
// logs and traces correlate. It is a no-op when the context carries no valid
// span (for example when tracing is disabled).
func WithTrace(ctx context.Context) context.Context {
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		return ctx
	}
	return WithLogger(ctx, FromContext(ctx).With(
		slog.String("trace_id", spanContext.TraceID().String()),
		slog.String("span_id", spanContext.SpanID().String()),
	))
}

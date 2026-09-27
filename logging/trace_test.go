package logging

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestWithTraceAddsSpanIdentifiers(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	defer func() { _ = provider.Shutdown(context.Background()) }()

	ctx, span := provider.Tracer("test").Start(context.Background(), "operation")
	defer span.End()

	var buf bytes.Buffer
	ctx = WithLogger(ctx, slog.New(slog.NewJSONHandler(&buf, nil)))
	ctx = WithTrace(ctx)

	FromContext(ctx).Info("inside span")

	out := buf.String()
	if !strings.Contains(out, `"trace_id":"`+span.SpanContext().TraceID().String()+`"`) {
		t.Fatalf("trace_id missing from %q", out)
	}
	if !strings.Contains(out, `"span_id":"`+span.SpanContext().SpanID().String()+`"`) {
		t.Fatalf("span_id missing from %q", out)
	}
}

func TestWithTraceWithoutSpanIsNoop(t *testing.T) {
	var buf bytes.Buffer
	ctx := WithLogger(context.Background(), slog.New(slog.NewJSONHandler(&buf, nil)))

	enriched := WithTrace(ctx)
	if enriched != ctx {
		t.Fatal("WithTrace must return the same context when there is no span")
	}

	FromContext(enriched).Info("no span")
	if strings.Contains(buf.String(), "trace_id") {
		t.Fatalf("unexpected trace_id in %q", buf.String())
	}
}

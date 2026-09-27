// Package telemetry sets up OpenTelemetry tracing with an OTLP/HTTP exporter.
package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"example.com/gosvc/config"
)

// Provider owns the tracer provider and its shutdown hook. The zero value is a
// valid "tracing disabled" provider.
type Provider struct {
	Tracer   trace.Tracer
	shutdown func(context.Context) error
}

// New builds the provider. When cfg.Enabled is false it returns a disabled
// provider without touching the global OpenTelemetry state.
func New(ctx context.Context, cfg config.TelemetryConfig, service, env, serviceVersion string) (*Provider, error) {
	if !cfg.Enabled {
		return &Provider{}, nil
	}

	options := []otlptracehttp.Option{otlptracehttp.WithEndpoint(cfg.OTLPEndpoint)}
	if cfg.Insecure {
		options = append(options, otlptracehttp.WithInsecure())
	}
	exporter, err := otlptracehttp.New(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("create OTLP exporter: %w", err)
	}

	res := resource.NewSchemaless(
		attribute.String("service.name", service),
		attribute.String("service.version", serviceVersion),
		attribute.String("deployment.environment", env),
	)

	ratio := cfg.SampleRatio
	if ratio <= 0 {
		ratio = 1
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)

	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &Provider{Tracer: provider.Tracer(service), shutdown: provider.Shutdown}, nil
}

// Enabled reports whether tracing is active.
func (p *Provider) Enabled() bool { return p != nil && p.Tracer != nil }

// Shutdown flushes pending spans. It is safe to call on a disabled provider.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p == nil || p.shutdown == nil {
		return nil
	}
	return p.shutdown(ctx)
}

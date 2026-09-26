package http

import (
	"context"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"example.com/gosvc/internal/apierror"
	"example.com/gosvc/internal/auth"
	"example.com/gosvc/internal/logging"
	"example.com/gosvc/internal/observability"
	"example.com/gosvc/internal/ratelimit"
	"example.com/gosvc/internal/reqid"
	"example.com/gosvc/internal/store"
)

// RequestID propagates or generates X-Request-ID and stores a request-scoped
// logger in the context.
func RequestID() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		id := string(c.GetHeader("X-Request-ID"))
		if id == "" {
			id = reqid.New()
		}
		c.Header("X-Request-ID", id)
		ctx = reqid.With(ctx, id)
		ctx = logging.WithLogger(ctx, logging.FromContext(ctx).With("request_id", id))
		c.Next(ctx)
	}
}

// Tracing starts a server span and extracts the W3C trace context.
func Tracing(tracer trace.Tracer) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if tracer == nil {
			c.Next(ctx)
			return
		}

		parent := otel.GetTextMapPropagator().Extract(ctx, &hertzCarrier{c: c})
		route := c.FullPath()
		if route == "" {
			route = string(c.Path())
		}

		ctx, span := tracer.Start(parent, string(c.Method())+" "+route,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.request.method", string(c.Method())),
				attribute.String("url.path", string(c.Path())),
				attribute.String("http.route", route),
			))
		defer span.End()

		c.Next(ctx)

		status := c.Response.StatusCode()
		span.SetAttributes(attribute.Int("http.response.status_code", status))
		if status >= 500 {
			span.SetStatus(codes.Error, http.StatusText(status))
		}
	}
}

type hertzCarrier struct {
	c *app.RequestContext
}

func (h *hertzCarrier) Get(key string) string { return string(h.c.GetHeader(key)) }

func (h *hertzCarrier) Set(key, value string) { h.c.Header(key, value) }

func (h *hertzCarrier) Keys() []string {
	keys := make([]string, 0, 16)
	h.c.Request.Header.VisitAll(func(key, _ []byte) {
		keys = append(keys, string(key))
	})
	return keys
}

// AccessLog logs one line per request and records metrics and time-series
// samples.
func AccessLog(metrics *observability.Metrics, recorder store.Recorder) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		start := time.Now()
		c.Next(ctx)

		latency := time.Since(start)
		method := string(c.Method())
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		status := c.Response.StatusCode()

		logging.FromContext(ctx).Info("http request",
			"method", method,
			"path", string(c.Path()),
			"route", route,
			"status", status,
			"latency_ms", float64(latency.Microseconds())/1000.0,
			"client_ip", c.ClientIP(),
		)
		metrics.ObserveHTTP(method, route, status, latency)
		if recorder != nil {
			_ = recorder.Incr(ctx, "http.requests:"+route, 1)
		}
	}
}

// Recovery converts panics into 500 responses instead of killing the process.
func Recovery(logger *slog.Logger) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		defer func() {
			if r := recover(); r != nil {
				logError(ctx, logger, "panic recovered",
					"panic", r,
					"stack", string(debug.Stack()),
					"path", string(c.Path()),
				)
				c.Abort()
				writeInternalError(ctx, c, "internal server error")
			}
		}()
		c.Next(ctx)
	}
}

// CORS allows cross-origin requests and answers preflight requests.
func CORS() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key, X-Request-ID")
		if string(c.Method()) == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next(ctx)
	}
}

// RateLimit rejects requests that exceed the per-client budget.
func RateLimit(limiter *ratelimit.Limiter) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if limiter != nil && !limiter.Allow(c.ClientIP()) {
			c.Abort()
			WriteError(ctx, c, apierror.New(apierror.KindRateLimited, "rate limit exceeded"))
			return
		}
		c.Next(ctx)
	}
}

// Auth enforces API key / JWT authentication. It is a no-op when the
// authenticator is disabled.
func Auth(authenticator *auth.Authenticator) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if !authenticator.Enabled() {
			c.Next(ctx)
			return
		}

		identity, err := authenticator.Authenticate(
			auth.BearerToken(string(c.GetHeader("Authorization"))),
			string(c.GetHeader("X-API-Key")),
		)
		if err != nil {
			c.Abort()
			WriteError(ctx, c, err)
			return
		}

		ctx = auth.WithIdentity(ctx, identity)
		ctx = logging.WithLogger(ctx, logging.FromContext(ctx).With(
			"subject", identity.Subject,
			"auth_method", string(identity.Method),
		))
		c.Next(ctx)
	}
}

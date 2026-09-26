package http

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/cloudwego/hertz/pkg/app"

	"example.com/gosvc/internal/apierror"
	"example.com/gosvc/internal/logging"
	"example.com/gosvc/internal/observability"
	"example.com/gosvc/internal/ratelimit"
	"example.com/gosvc/internal/reqid"
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

// AccessLog logs one line per request and records metrics.
func AccessLog(metrics *observability.Metrics) app.HandlerFunc {
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
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Request-ID")
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

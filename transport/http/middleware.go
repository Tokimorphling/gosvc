package http

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Tokimorphling/gosvc/apierror"
	"github.com/Tokimorphling/gosvc/auth"
	"github.com/Tokimorphling/gosvc/config"
	"github.com/Tokimorphling/gosvc/logging"
	"github.com/Tokimorphling/gosvc/observability"
	"github.com/Tokimorphling/gosvc/ratelimit"
	"github.com/Tokimorphling/gosvc/store"
)

// RequestID propagates or generates X-Request-ID and stores a request-scoped
// logger in the context.
func RequestID() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		id := string(c.GetHeader("X-Request-ID"))
		if id == "" {
			id = logging.NewRequestID()
		}
		c.Header("X-Request-ID", id)
		ctx = logging.WithRequestID(ctx, id)
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

		// Correlate application logs with the trace.
		ctx = logging.WithTrace(ctx)

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

// AccessLog writes one line per request: to the dedicated access logger when
// one is configured, otherwise to the request-scoped application logger.
func AccessLog(metrics *observability.Metrics, recorder store.Recorder, accessLogger *slog.Logger) app.HandlerFunc {
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

		fields := []any{
			"method", method,
			"path", string(c.Path()),
			"route", route,
			"status", status,
			"latency_ms", float64(latency.Microseconds()) / 1000.0,
			"client_ip", c.ClientIP(),
		}
		if accessLogger != nil {
			accessLogger.Info("http request", append(logging.RequestAttrs(ctx), fields...)...)
		} else {
			logging.FromContext(ctx).Info("http request", fields...)
		}

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

// CORS answers cross-origin preflight requests. The middleware is configured
// through [http.cors]: disabled entirely, or restricted to an origin list.
// When the list is ["*"] any origin is allowed (mirroring a public API); other
// entries match the request's Origin header exactly and only matched origins
// receive CORS headers.
func CORS(cfg config.CORSConfig) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if !cfg.Enabled {
			c.Next(ctx)
			return
		}
		allowOrigin := matchOrigin(cfg.AllowOrigins, string(c.GetHeader("Origin")))
		if allowOrigin != "" {
			c.Header("Access-Control-Allow-Origin", allowOrigin)
			c.Header("Vary", "Origin")
			c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key, X-Request-ID")
		}
		if string(c.Method()) == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next(ctx)
	}
}

// matchOrigin returns the Access-Control-Allow-Origin value for the request
// origin: "*" when the wildcard is configured, the origin itself when it is
// listed, and "" (no CORS headers) otherwise.
func matchOrigin(allow []string, origin string) string {
	for _, candidate := range allow {
		if candidate == "*" {
			return "*"
		}
		if origin != "" && candidate == origin {
			return origin
		}
	}
	return ""
}

// RateLimit rejects requests that exceed the per-client budget. The key is
// the direct peer address, not c.ClientIP(): Hertz trusts X-Forwarded-For /
// X-Real-IP from every peer by default, so keying on ClientIP would let
// clients rotate the key per request and bypass the limit. Deployments behind
// a trusted proxy that terminates TCP therefore share one bucket (the proxy),
// which fails closed; front such services with a proxy-level rate limiter or
// re-enable header trust consciously.
func RateLimit(limiter *ratelimit.Limiter) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if limiter != nil && !limiter.Allow(peerIP(c)) {
			c.Abort()
			WriteError(ctx, c, apierror.New(apierror.KindRateLimited, "rate limit exceeded"))
			return
		}
		c.Next(ctx)
	}
}

// peerIP returns the transport-level peer address of the request.
func peerIP(c *app.RequestContext) string {
	addr := c.RemoteAddr()
	if host, _, err := net.SplitHostPort(addr.String()); err == nil {
		return host
	}
	return addr.String()
}

// Auth enforces API key / JWT authentication. It is a no-op when the
// authenticator is disabled, and public paths bypass it. A public path matches
// either the literal request path or the route pattern, so parameterised
// routes can be whitelisted as a whole ("/api/v1/orders/:id").
func Auth(authenticator *auth.Authenticator, publicPaths ...string) app.HandlerFunc {
	public := make(map[string]struct{}, len(publicPaths))
	for _, path := range publicPaths {
		public[path] = struct{}{}
	}

	return func(ctx context.Context, c *app.RequestContext) {
		if _, ok := public[string(c.Path())]; ok {
			c.Next(ctx)
			return
		}
		if route := c.FullPath(); route != "" {
			if _, ok := public[route]; ok {
				c.Next(ctx)
				return
			}
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
		if identity.Method == auth.MethodAnonymous {
			c.Next(ctx)
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

package grpc

import (
	"context"
	"log/slog"
	"net"
	"runtime/debug"
	"strings"
	"time"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/Tokimorphling/gosvc/auth"
	"github.com/Tokimorphling/gosvc/logging"
	"github.com/Tokimorphling/gosvc/observability"
	"github.com/Tokimorphling/gosvc/ratelimit"
	"github.com/Tokimorphling/gosvc/store"
)

const requestIDKey = "x-request-id"

func recoveryInterceptor(logger *slog.Logger) ggrpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("panic recovered",
					"panic", r,
					"stack", string(debug.Stack()),
					"method", info.FullMethod,
				)
				err = status.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(ctx, req)
	}
}

func requestIDInterceptor() ggrpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (any, error) {
		id := ""
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if values := md.Get(requestIDKey); len(values) > 0 {
				id = values[0]
			}
		}
		if id == "" {
			id = logging.NewRequestID()
		}
		ctx = logging.WithRequestID(ctx, id)
		ctx = logging.WithLogger(ctx, logging.FromContext(ctx).With("request_id", id))
		return handler(ctx, req)
	}
}

// traceInterceptor adds the active span identifiers (started by the otelgrpc
// stats handler) to the context logger.
func traceInterceptor() ggrpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (any, error) {
		return handler(logging.WithTrace(ctx), req)
	}
}

func authInterceptor(authenticator *auth.Authenticator) ggrpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (any, error) {
		if !authenticator.Enabled() || isPublicMethod(info.FullMethod) {
			return handler(ctx, req)
		}

		identity, err := authenticator.Authenticate(credentialsFromMetadata(ctx))
		if err != nil {
			return nil, ToStatus(err)
		}

		ctx = auth.WithIdentity(ctx, identity)
		ctx = logging.WithLogger(ctx, logging.FromContext(ctx).With(
			"subject", identity.Subject,
			"auth_method", string(identity.Method),
		))
		return handler(ctx, req)
	}
}

func loggingInterceptor(recorder store.Recorder, accessLogger *slog.Logger) ggrpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)

		fields := []any{
			"method", info.FullMethod,
			"code", status.Code(err).String(),
			"latency_ms", float64(time.Since(start).Microseconds()) / 1000.0,
			"peer", peerAddr(ctx),
		}
		if accessLogger != nil {
			accessLogger.Info("grpc request", append(logging.RequestAttrs(ctx), fields...)...)
		} else {
			logging.FromContext(ctx).Info("grpc request", fields...)
		}

		if recorder != nil {
			_ = recorder.Incr(ctx, "grpc.requests:"+info.FullMethod, 1)
		}
		return resp, err
	}
}

func metricsInterceptor(metrics *observability.Metrics) ggrpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		if metrics != nil {
			metrics.ObserveGRPC(info.FullMethod, status.Code(err).String(), time.Since(start))
		}
		return resp, err
	}
}

func rateLimitInterceptor(limiter *ratelimit.Limiter) ggrpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (any, error) {
		if limiter != nil && !limiter.Allow(peerKey(ctx)) {
			return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}
		return handler(ctx, req)
	}
}

func credentialsFromMetadata(ctx context.Context) (bearer, apiKey string) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", ""
	}
	if values := md.Get("authorization"); len(values) > 0 {
		bearer = auth.BearerToken(values[0])
	}
	if values := md.Get("x-api-key"); len(values) > 0 {
		apiKey = values[0]
	}
	return bearer, apiKey
}

// isPublicMethod keeps health checks and reflection usable without credentials.
func isPublicMethod(method string) bool {
	return strings.HasPrefix(method, "/grpc.health.v1.Health/") ||
		strings.HasPrefix(method, "/grpc.reflection.")
}

func peerAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return "unknown"
}

// peerKey returns the client address without the ephemeral port, so the rate
// limiter buckets per client, not per connection.
func peerKey(ctx context.Context) string {
	addr := peerAddr(ctx)
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

// serverStream carries a derived context (auth identity, request id) through
// a streaming RPC. Panics raised inside handlers propagate up to
// recoveryStreamInterceptor, which logs them with a stack trace.
type serverStream struct {
	ggrpc.ServerStream
	ctx context.Context
}

func (s *serverStream) Context() context.Context { return s.ctx }

// Stream interceptors mirror the unary chain so streaming RPCs get the same
// recovery, request ids, tracing context, authentication, logging, metrics
// and rate limiting. Applications registering streaming services are
// therefore protected the same way as unary ones.

func recoveryStreamInterceptor(logger *slog.Logger) ggrpc.StreamServerInterceptor {
	return func(srv any, ss ggrpc.ServerStream, info *ggrpc.StreamServerInfo, handler ggrpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("panic recovered",
					"panic", r,
					"stack", string(debug.Stack()),
					"method", info.FullMethod,
				)
				err = status.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(srv, ss)
	}
}

func requestIDStreamInterceptor() ggrpc.StreamServerInterceptor {
	return func(srv any, ss ggrpc.ServerStream, info *ggrpc.StreamServerInfo, handler ggrpc.StreamHandler) error {
		id := ""
		if md, ok := metadata.FromIncomingContext(ss.Context()); ok {
			if values := md.Get(requestIDKey); len(values) > 0 {
				id = values[0]
			}
		}
		if id == "" {
			id = logging.NewRequestID()
		}
		ctx := logging.WithRequestID(ss.Context(), id)
		ctx = logging.WithLogger(ctx, logging.FromContext(ctx).With("request_id", id))
		return handler(srv, &serverStream{ServerStream: ss, ctx: ctx})
	}
}

func traceStreamInterceptor() ggrpc.StreamServerInterceptor {
	return func(srv any, ss ggrpc.ServerStream, info *ggrpc.StreamServerInfo, handler ggrpc.StreamHandler) error {
		ctx := logging.WithTrace(ss.Context())
		return handler(srv, &serverStream{ServerStream: ss, ctx: ctx})
	}
}

func authStreamInterceptor(authenticator *auth.Authenticator) ggrpc.StreamServerInterceptor {
	return func(srv any, ss ggrpc.ServerStream, info *ggrpc.StreamServerInfo, handler ggrpc.StreamHandler) error {
		if !authenticator.Enabled() || isPublicMethod(info.FullMethod) {
			return handler(srv, ss)
		}

		identity, err := authenticator.Authenticate(credentialsFromMetadata(ss.Context()))
		if err != nil {
			return ToStatus(err)
		}

		ctx := auth.WithIdentity(ss.Context(), identity)
		ctx = logging.WithLogger(ctx, logging.FromContext(ctx).With(
			"subject", identity.Subject,
			"auth_method", string(identity.Method),
		))
		return handler(srv, &serverStream{ServerStream: ss, ctx: ctx})
	}
}

func loggingStreamInterceptor(recorder store.Recorder, accessLogger *slog.Logger) ggrpc.StreamServerInterceptor {
	return func(srv any, ss ggrpc.ServerStream, info *ggrpc.StreamServerInfo, handler ggrpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)

		fields := []any{
			"method", info.FullMethod,
			"code", status.Code(err).String(),
			"latency_ms", float64(time.Since(start).Microseconds()) / 1000.0,
			"peer", peerAddr(ss.Context()),
			"stream", true,
		}
		if accessLogger != nil {
			accessLogger.Info("grpc request", append(logging.RequestAttrs(ss.Context()), fields...)...)
		} else {
			logging.FromContext(ss.Context()).Info("grpc request", fields...)
		}

		if recorder != nil {
			_ = recorder.Incr(ss.Context(), "grpc.requests:"+info.FullMethod, 1)
		}
		return err
	}
}

func metricsStreamInterceptor(metrics *observability.Metrics) ggrpc.StreamServerInterceptor {
	return func(srv any, ss ggrpc.ServerStream, info *ggrpc.StreamServerInfo, handler ggrpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		if metrics != nil {
			metrics.ObserveGRPC(info.FullMethod, status.Code(err).String(), time.Since(start))
		}
		return err
	}
}

func rateLimitStreamInterceptor(limiter *ratelimit.Limiter) ggrpc.StreamServerInterceptor {
	return func(srv any, ss ggrpc.ServerStream, info *ggrpc.StreamServerInfo, handler ggrpc.StreamHandler) error {
		if limiter != nil && !limiter.Allow(peerKey(ss.Context())) {
			return status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}
		return handler(srv, ss)
	}
}

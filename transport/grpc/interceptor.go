package grpc

import (
	"context"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"example.com/gosvc/auth"
	"example.com/gosvc/logging"
	"example.com/gosvc/observability"
	"example.com/gosvc/ratelimit"
	"example.com/gosvc/reqid"
	"example.com/gosvc/store"
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
			id = reqid.New()
		}
		ctx = reqid.With(ctx, id)
		ctx = logging.WithLogger(ctx, logging.FromContext(ctx).With("request_id", id))
		return handler(ctx, req)
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

func loggingInterceptor(recorder store.Recorder) ggrpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		logging.FromContext(ctx).Info("grpc request",
			"method", info.FullMethod,
			"code", status.Code(err).String(),
			"latency_ms", float64(time.Since(start).Microseconds())/1000.0,
			"peer", peerAddr(ctx),
		)
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
		if limiter != nil && !limiter.Allow(peerAddr(ctx)) {
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

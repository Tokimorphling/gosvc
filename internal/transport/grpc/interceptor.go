package grpc

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"example.com/gosvc/internal/logging"
	"example.com/gosvc/internal/observability"
	"example.com/gosvc/internal/ratelimit"
	"example.com/gosvc/internal/reqid"
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

func loggingInterceptor() ggrpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		logging.FromContext(ctx).Info("grpc request",
			"method", info.FullMethod,
			"code", status.Code(err).String(),
			"latency_ms", float64(time.Since(start).Microseconds())/1000.0,
			"peer", peerAddr(ctx),
		)
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

func peerAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return "unknown"
}

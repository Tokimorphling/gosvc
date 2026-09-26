// Package grpc contains the gRPC transport.
package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/trace"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	greeterv1 "example.com/gosvc/api/greeter/v1"
	"example.com/gosvc/internal/auth"
	"example.com/gosvc/internal/config"
	apphealth "example.com/gosvc/internal/health"
	"example.com/gosvc/internal/observability"
	"example.com/gosvc/internal/ratelimit"
	"example.com/gosvc/internal/service/greeter"
	"example.com/gosvc/internal/store"
)

// Options wires the gRPC server.
type Options struct {
	Config        *config.Config
	Service       *greeter.Service
	Logger        *slog.Logger
	Metrics       *observability.Metrics
	Limiter       *ratelimit.Limiter
	Authenticator *auth.Authenticator
	Tracer        trace.Tracer
	Recorder      store.Recorder
	Ready         *apphealth.Ready
}

// Server serves the Greeter service plus gRPC health and reflection.
type Server struct {
	server   *ggrpc.Server
	listener net.Listener
	health   *health.Server
	cfg      config.GRPCConfig
	logger   *slog.Logger
	ready    *apphealth.Ready
}

// New binds the listener and registers services and interceptors.
func New(opts Options) (*Server, error) {
	listener, err := net.Listen("tcp", opts.Config.GRPC.Addr())
	if err != nil {
		return nil, fmt.Errorf("listen grpc: %w", err)
	}

	serverOptions := []ggrpc.ServerOption{
		ggrpc.ChainUnaryInterceptor(
			recoveryInterceptor(opts.Logger),
			requestIDInterceptor(),
			authInterceptor(opts.Authenticator),
			loggingInterceptor(opts.Recorder),
			metricsInterceptor(opts.Metrics),
			rateLimitInterceptor(opts.Limiter),
		),
	}
	if opts.Tracer != nil {
		serverOptions = append(serverOptions, ggrpc.StatsHandler(otelgrpc.NewServerHandler()))
	}

	server := ggrpc.NewServer(serverOptions...)
	greeterv1.RegisterGreeterServer(server, newGreeterServer(opts.Service, opts.Logger))

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(server, healthServer)
	reflection.Register(server)

	return &Server{
		server:   server,
		listener: listener,
		health:   healthServer,
		cfg:      opts.Config.GRPC,
		logger:   opts.Logger,
		ready:    opts.Ready,
	}, nil
}

// Addr returns the effective listen address.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Serve blocks until ctx is cancelled or the server fails.
func (s *Server) Serve(ctx context.Context) error {
	s.health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	go func() {
		<-ctx.Done()
		s.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)

		done := make(chan struct{})
		go func() {
			s.server.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(s.cfg.ShutdownTimeout.D()):
			s.logger.Warn("grpc graceful stop timed out, forcing stop")
			s.server.Stop()
		}
	}()

	err := s.server.Serve(s.listener)
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("grpc serve: %w", err)
	}
	return nil
}

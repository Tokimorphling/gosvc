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

	"github.com/Tokimorphling/gosvc/auth"
	"github.com/Tokimorphling/gosvc/config"
	apphealth "github.com/Tokimorphling/gosvc/health"
	"github.com/Tokimorphling/gosvc/observability"
	"github.com/Tokimorphling/gosvc/ratelimit"
	"github.com/Tokimorphling/gosvc/store"
)

// Options wires the gRPC server.
type Options struct {
	Config        *config.Config
	Logger        *slog.Logger
	Metrics       *observability.Metrics
	Limiter       *ratelimit.Limiter
	Authenticator *auth.Authenticator
	Tracer        trace.Tracer
	Recorder      store.Recorder
	Ready         *apphealth.Ready
	// AccessLogger receives request logs. When nil they share the application
	// logger.
	AccessLogger *slog.Logger
}

// Server serves the health and reflection services plus whatever the
// application registers through gosvc.App.RegisterGRPC.
type Server struct {
	server   *ggrpc.Server
	listener net.Listener
	health   *health.Server
	cfg      config.GRPCConfig
	logger   *slog.Logger
	ready    *apphealth.Ready
}

// New binds the listener and installs interceptors.
func New(opts Options) (*Server, error) {
	listener, err := net.Listen("tcp", opts.Config.GRPC.Addr())
	if err != nil {
		return nil, fmt.Errorf("listen grpc: %w", err)
	}

	serverOptions := []ggrpc.ServerOption{
		ggrpc.ChainUnaryInterceptor(
			recoveryInterceptor(opts.Logger),
			requestIDInterceptor(),
			traceInterceptor(),
			authInterceptor(opts.Authenticator),
			loggingInterceptor(opts.Recorder, opts.AccessLogger),
			metricsInterceptor(opts.Metrics),
			rateLimitInterceptor(opts.Limiter),
		),
		ggrpc.ChainStreamInterceptor(
			recoveryStreamInterceptor(opts.Logger),
			requestIDStreamInterceptor(),
			traceStreamInterceptor(),
			authStreamInterceptor(opts.Authenticator),
			loggingStreamInterceptor(opts.Recorder, opts.AccessLogger),
			metricsStreamInterceptor(opts.Metrics),
			rateLimitStreamInterceptor(opts.Limiter),
		),
	}
	if opts.Tracer != nil {
		serverOptions = append(serverOptions, ggrpc.StatsHandler(otelgrpc.NewServerHandler()))
	}

	server := ggrpc.NewServer(serverOptions...)

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

// Server exposes the gRPC server for service registration. It must only be used
// before Serve starts.
func (s *Server) Server() *ggrpc.Server { return s.server }

// Addr returns the effective listen address.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Close releases a listener bound by New before Serve is started.
func (s *Server) Close() error { return s.listener.Close() }

// Serve blocks until ctx is cancelled or the server fails. When ctx is
// cancelled it waits for the graceful drain to finish before returning, so
// callers (gosvc.App.Run) do not exit the process while in-flight RPCs are
// still being served.
func (s *Server) Serve(ctx context.Context) error {
	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	done := make(chan struct{})
	go func() {
		defer close(done)
		<-serveCtx.Done()
		s.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)

		drainDone := make(chan struct{})
		go func() {
			defer close(drainDone)
			s.server.GracefulStop()
		}()
		timeout := time.NewTimer(s.cfg.ShutdownTimeout.D())
		defer timeout.Stop()
		select {
		case <-drainDone:
		case <-timeout.C:
			s.logger.Warn("grpc graceful stop timed out, forcing stop")
			s.server.Stop()
			<-drainDone
		}
	}()

	// Serve returns as soon as GracefulStop closes the listener; the drain
	// continues afterwards, so join it before reporting the server stopped.
	err := s.server.Serve(s.listener)
	cancel()
	<-done
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("grpc serve: %w", err)
	}
	return nil
}

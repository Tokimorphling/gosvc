package http

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/hlog"

	"example.com/gosvc/internal/config"
	"example.com/gosvc/internal/health"
	"example.com/gosvc/internal/jsonrpc"
	"example.com/gosvc/internal/observability"
	"example.com/gosvc/internal/ratelimit"
	"example.com/gosvc/internal/service/greeter"
)

// Options wires the HTTP server.
type Options struct {
	Config  *config.Config
	Service *greeter.Service
	Logger  *slog.Logger
	Level   *slog.LevelVar
	Metrics *observability.Metrics
	Limiter *ratelimit.Limiter
	Ready   *health.Ready
}

// Server serves REST and JSON-RPC over Hertz.
type Server struct {
	engine     *server.Hertz
	listener   net.Listener
	cfg        config.HTTPConfig
	logger     *slog.Logger
	service    *greeter.Service
	dispatcher *jsonrpc.Dispatcher
	ready      *health.Ready
}

// New binds the listener and wires routes and middleware. The listener is
// created eagerly so the effective address is known before serving starts.
func New(opts Options) (*Server, error) {
	listener, err := net.Listen("tcp", opts.Config.HTTP.Addr())
	if err != nil {
		return nil, fmt.Errorf("listen http: %w", err)
	}

	engine := server.New(
		server.WithListener(listener),
		server.WithReadTimeout(opts.Config.HTTP.ReadTimeout.D()),
		server.WithWriteTimeout(opts.Config.HTTP.WriteTimeout.D()),
		server.WithIdleTimeout(opts.Config.HTTP.IdleTimeout.D()),
		server.WithExitWaitTime(opts.Config.HTTP.ShutdownTimeout.D()),
		server.WithMaxRequestBodySize(opts.Config.HTTP.MaxBodyBytes),
		server.WithDisablePrintRoute(true),
	)

	s := &Server{
		engine:     engine,
		listener:   listener,
		cfg:        opts.Config.HTTP,
		logger:     opts.Logger,
		service:    opts.Service,
		dispatcher: jsonrpc.NewDispatcher(),
		ready:      opts.Ready,
	}
	if opts.Metrics != nil {
		s.dispatcher.SetObserver(opts.Metrics.ObserveJSONRPC)
	}

	// Route Hertz internal logs through the application logger.
	hlog.SetLogger(newHlogAdapter(opts.Logger, opts.Level))

	engine.Use(
		RequestID(),
		AccessLog(opts.Metrics),
		Recovery(opts.Logger),
		CORS(),
		RateLimit(opts.Limiter),
	)

	s.registerJSONRPCMethods()
	s.registerRoutes(engine)
	return s, nil
}

// Addr returns the effective listen address.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Serve blocks until ctx is cancelled or the server fails.
func (s *Server) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout.D())
		defer cancel()
		if err := s.engine.Shutdown(shutdownCtx); err != nil {
			s.logger.Warn("http graceful shutdown returned error", "error", err)
		}
	}()

	err := s.engine.Run()
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("http serve: %w", err)
	}
	return nil
}

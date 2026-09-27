package http

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"go.opentelemetry.io/otel/trace"

	"example.com/gosvc/auth"
	"example.com/gosvc/config"
	"example.com/gosvc/health"
	"example.com/gosvc/jsonrpc"
	"example.com/gosvc/observability"
	"example.com/gosvc/ratelimit"
	"example.com/gosvc/store"
)

// Options wires the HTTP server.
type Options struct {
	Config        *config.Config
	Logger        *slog.Logger
	Level         *slog.LevelVar
	Metrics       *observability.Metrics
	Limiter       *ratelimit.Limiter
	Authenticator *auth.Authenticator
	Tracer        trace.Tracer
	Recorder      store.Recorder
	Ready         *health.Ready
	// Dispatcher backs POST /rpc. When nil a private one is created, which is
	// only useful when the application registers no JSON-RPC methods.
	Dispatcher *jsonrpc.Dispatcher
	// PublicPaths bypass authentication in addition to /healthz and /readyz.
	PublicPaths []string
	// Version is reported by GET /healthz.
	Version string
	// AccessLogger receives request logs. When nil they share the application
	// logger.
	AccessLogger *slog.Logger
}

// Server serves the health endpoints, the JSON-RPC endpoint and the routes the
// application registers through gosvc.App.RegisterHTTP.
type Server struct {
	engine        *server.Hertz
	listener      net.Listener
	cfg           config.HTTPConfig
	logger        *slog.Logger
	dispatcher    *jsonrpc.Dispatcher
	authenticator *auth.Authenticator
	ready         *health.Ready
	version       string
}

// New binds the listener and wires middleware. The listener is created eagerly
// so the effective address is known before serving starts.
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

	dispatcher := opts.Dispatcher
	if dispatcher == nil {
		dispatcher = jsonrpc.NewDispatcher()
	}

	s := &Server{
		engine:        engine,
		listener:      listener,
		cfg:           opts.Config.HTTP,
		logger:        opts.Logger,
		dispatcher:    dispatcher,
		authenticator: opts.Authenticator,
		ready:         opts.Ready,
		version:       opts.Version,
	}

	// Route Hertz internal logs through the application logger.
	hlog.SetLogger(newHlogAdapter(opts.Logger, opts.Level))

	engine.Use(
		RequestID(),
		Tracing(opts.Tracer),
		AccessLog(opts.Metrics, opts.Recorder, opts.AccessLogger),
		Recovery(opts.Logger),
		CORS(),
		RateLimit(opts.Limiter),
		Auth(opts.Authenticator, append([]string{"/healthz", "/readyz"}, opts.PublicPaths...)...),
	)

	s.registerRoutes(engine)
	return s, nil
}

// Engine exposes the Hertz engine for route registration. It must only be used
// before Serve starts.
func (s *Server) Engine() *server.Hertz { return s.engine }

// Dispatcher exposes the JSON-RPC dispatcher used by POST /rpc.
func (s *Server) Dispatcher() *jsonrpc.Dispatcher { return s.dispatcher }

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

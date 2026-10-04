package http

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	hconfig "github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"go.opentelemetry.io/otel/trace"

	"github.com/Tokimorphling/gosvc/auth"
	"github.com/Tokimorphling/gosvc/config"
	"github.com/Tokimorphling/gosvc/health"
	"github.com/Tokimorphling/gosvc/internal/shutdown"
	"github.com/Tokimorphling/gosvc/internal/tlsutil"
	"github.com/Tokimorphling/gosvc/jsonrpc"
	"github.com/Tokimorphling/gosvc/observability"
	"github.com/Tokimorphling/gosvc/ratelimit"
	"github.com/Tokimorphling/gosvc/store"
)

// hlogOnce installs the hlog adapter exactly once per process. Hertz routes
// its internal logs through the process-global hlog package, so a second
// SetLogger call would race with engines that are still logging (during
// shutdown) and the last caller would silently win. The adapter resolves
// slog.Default() at call time, so it always follows the current default
// logger instead of a stale one.
var hlogOnce sync.Once

// Options wires the HTTP server.
type Options struct {
	Config        *config.Config
	Logger        *slog.Logger
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
	// Both request paths and route patterns ("/api/v1/orders/:id") match.
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
	tlsConfig, err := tlsutil.Load(opts.Config.HTTP.TLS)
	if err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("http tls: %w", err)
	}

	engineOptions := []hconfig.Option{
		server.WithListener(listener),
		server.WithReadTimeout(opts.Config.HTTP.ReadTimeout.D()),
		server.WithWriteTimeout(opts.Config.HTTP.WriteTimeout.D()),
		server.WithIdleTimeout(opts.Config.HTTP.IdleTimeout.D()),
		server.WithExitWaitTime(opts.Config.HTTP.ShutdownTimeout.D()),
		server.WithMaxRequestBodySize(opts.Config.HTTP.MaxBodyBytes),
		server.WithDisablePrintRoute(true),
		// Sense client disconnects so long-lived handlers (SSE streams) see
		// ctx.Done when the client goes away.
		server.WithSenseClientDisconnection(true),
	}
	if tlsConfig != nil {
		// WithTLS selects Hertz's standard transport; netpoll cannot accept
		// a tls.Listener. Keep the raw listener and let Hertz do the handshake.
		engineOptions = append(engineOptions, server.WithTLS(tlsConfig))
	}
	engine := server.New(engineOptions...)

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

	// Route Hertz internal logs through the application logger. See hlogOnce
	// for why this happens at most once per process.
	hlogOnce.Do(func() { hlog.SetLogger(newHlogAdapter()) })

	engine.Use(
		RequestID(),
		Tracing(opts.Tracer),
		AccessLog(opts.Metrics, opts.Recorder, opts.AccessLogger),
		Recovery(opts.Logger),
		CORS(opts.Config.HTTP.CORS),
		RateLimit(opts.Limiter),
		Auth(opts.Authenticator, append([]string{"/healthz", "/readyz"}, opts.PublicPaths...)...),
		RequestTimeout(opts.Config.HTTP.HandlerTimeout.D()),
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

// Close releases a listener bound by New before Serve is started.
func (s *Server) Close() error { return s.listener.Close() }

// Serve blocks until ctx is cancelled or the server fails. When ctx is
// cancelled it waits for the graceful drain to finish before returning, so
// callers (gosvc.App.Run) do not exit the process while in-flight requests
// are still being served.
func (s *Server) Serve(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}
	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	runReturned := make(chan struct{})
	go func() {
		defer close(done)
		<-serveCtx.Done()
		// Hertz ignores Shutdown until Run marks the engine as running.
		// Cancellation during startup must wait for that transition or for
		// Run to fail, otherwise Run may start serving after Shutdown returned.
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for !s.engine.IsRunning() {
			select {
			case <-runReturned:
				return
			case <-ticker.C:
			}
		}
		shutdownCtx, cancel := shutdown.Context(ctx, s.cfg.ShutdownTimeout.D())
		defer cancel()
		if err := s.engine.Shutdown(shutdownCtx); err != nil {
			s.logger.Warn("http graceful shutdown returned error", "error", err)
		}
	}()

	// engine.Run returns as soon as the listener is closed, which happens at
	// the beginning of engine.Shutdown; the drain continues afterwards.
	err := s.engine.Run()
	close(runReturned)
	cancel()
	<-done
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("http serve: %w", err)
	}
	return nil
}

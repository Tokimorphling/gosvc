// Package gosvc is a multi-protocol Go service runtime. It wires logging,
// configuration, authentication, tracing, metrics, rate limiting, storage and
// the HTTP/gRPC/TCP transports around handlers provided by the application.
//
// Typical usage:
//
//	cfg := &config.Config{}
//	if err := config.Load("config.json", cfg); err != nil {
//		log.Fatal(err)
//	}
//	logHandle, err := logging.New(cfg.Log, cfg.Service.Name, cfg.Service.Env, "v1.0.0")
//	if err != nil {
//		log.Fatal(err)
//	}
//	slog.SetDefault(logHandle.Logger())
//
//	app, err := gosvc.New(cfg, gosvc.WithLogger(logHandle), gosvc.WithHotReload("config.json", "MYAPP"))
//	if err != nil {
//		log.Fatal(err)
//	}
//	app.RegisterHTTP(func(h *server.Hertz) { h.GET("/hello", handler) })
//	app.RegisterJSONRPC(func(d *jsonrpc.Dispatcher) { d.Register("svc.echo", echo) })
//	app.RegisterGRPC(func(s *grpc.Server) { pb.RegisterService(s, impl) })
//
//	if err := app.Run(ctx); err != nil {
//		log.Fatal(err)
//	}
//
// Everything application specific lives outside this module: this package only
// owns infrastructure and lifecycle.
package gosvc

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"golang.org/x/sync/errgroup"
	ggrpc "google.golang.org/grpc"

	"example.com/gosvc/admin"
	"example.com/gosvc/auth"
	"example.com/gosvc/config"
	"example.com/gosvc/health"
	"example.com/gosvc/jsonrpc"
	"example.com/gosvc/logging"
	"example.com/gosvc/observability"
	"example.com/gosvc/ratelimit"
	"example.com/gosvc/reload"
	"example.com/gosvc/state"
	"example.com/gosvc/store"
	"example.com/gosvc/store/redisx"
	"example.com/gosvc/telemetry"
	grpctransport "example.com/gosvc/transport/grpc"
	httptransport "example.com/gosvc/transport/http"
	tcptransport "example.com/gosvc/transport/tcp"
	"example.com/gosvc/version"
)

// ErrStarted is returned when handlers are registered after Run has been called.
var ErrStarted = errors.New("gosvc: application already started")

// Option customises the runtime.
type Option func(*options)

type options struct {
	logger      *logging.Handle
	version     string
	hotReload   bool
	source      config.Source
	publicPaths []string
	onReload    func(*config.Config) error
}

// WithLogger supplies the logging handle. When omitted, the runtime builds one
// from cfg.Log.
func WithLogger(handle *logging.Handle) Option {
	return func(o *options) { o.logger = handle }
}

// WithVersion overrides the build version reported by telemetry and the admin
// /version endpoint.
func WithVersion(v string) Option {
	return func(o *options) { o.version = v }
}

// WithHotReload watches path and applies reloadable settings (log, auth,
// limiter) at runtime. envPrefix selects the environment variable prefix used
// when re-reading the file; empty means "GOSVC".
func WithHotReload(path, envPrefix string) Option {
	return func(o *options) {
		o.hotReload = true
		o.source = config.Source{Path: path, EnvPrefix: envPrefix}
	}
}

// WithPublicPaths lets the listed HTTP paths bypass authentication, in
// addition to /healthz and /readyz.
func WithPublicPaths(paths ...string) Option {
	return func(o *options) { o.publicPaths = append(o.publicPaths, paths...) }
}

// WithOnReload registers an application hook invoked after the runtime applied
// its own reloadable sections. Use it to reload application-specific settings.
func WithOnReload(fn func(*config.Config) error) Option {
	return func(o *options) { o.onReload = fn }
}

// App owns every transport, the shared dependencies and the lifecycle.
type App struct {
	cfg        state.Snapshot[*config.Config]
	opts       options
	log        *logging.Handle
	metrics    *observability.Metrics
	auth       *auth.Authenticator
	limiter    *ratelimit.Limiter
	ready      *health.Ready
	dispatcher *jsonrpc.Dispatcher

	http  *httptransport.Server
	grpc  *grpctransport.Server
	tcp   *tcptransport.Server
	admin *admin.Server

	telemetry *telemetry.Provider
	store     *redisx.Store
	recorder  *redisx.Recorder

	mu        sync.Mutex
	started   bool
	httpRegs  []func(*server.Hertz)
	grpcRegs  []func(*ggrpc.Server)
	rpcRegs   []func(*jsonrpc.Dispatcher)
	adminRegs []func(*http.ServeMux)
}

// New builds all components. Listeners are bound here so their addresses are
// known before Run starts serving traffic.
func New(cfg *config.Config, opts ...Option) (*App, error) {
	o := options{version: version.Version}
	for _, opt := range opts {
		opt(&o)
	}

	logHandle := o.logger
	if logHandle == nil {
		built, err := logging.New(cfg.Log, cfg.Service.Name, cfg.Service.Env, o.version)
		if err != nil {
			return nil, err
		}
		logHandle = built
	}
	logger := logHandle.Logger()

	a := &App{
		opts:    o,
		log:     logHandle,
		metrics: observability.New(cfg.Service.Name),
		ready:   &health.Ready{},
		limiter: ratelimit.New(cfg.Limiter.RPS, cfg.Limiter.Burst),
	}
	a.cfg.Store(cfg)

	authenticator, err := auth.New(cfg.Auth)
	if err != nil {
		return nil, err
	}
	a.auth = authenticator

	tracer, err := telemetry.New(context.Background(), cfg.Telemetry, cfg.Service.Name, cfg.Service.Env, o.version)
	if err != nil {
		return nil, err
	}
	a.telemetry = tracer

	if cfg.Storage.Redis.Enabled {
		a.store, err = redisx.New(context.Background(), cfg.Storage.Redis)
		if err != nil {
			return nil, err
		}
		a.recorder = redisx.NewRecorder(a.store, cfg.Storage.Redis.QueueSize, logger)
	}

	// One dispatcher shared by the HTTP /rpc endpoint and the TCP transport, so
	// methods are registered once.
	dispatcher := jsonrpc.NewDispatcher()
	dispatcher.SetObserver(a.metrics.ObserveJSONRPC)
	dispatcher.Register("system.methods", func(_ context.Context, _ json.RawMessage) (any, error) {
		return dispatcher.Methods(), nil
	})
	a.dispatcher = dispatcher

	httpServer, err := httptransport.New(httptransport.Options{
		Config:        cfg,
		Logger:        logger,
		Level:         logHandle.Level(),
		Metrics:       a.metrics,
		Limiter:       a.limiter,
		Authenticator: authenticator,
		Tracer:        tracer.Tracer,
		Recorder:      a.recorder,
		Ready:         a.ready,
		Dispatcher:    dispatcher,
		PublicPaths:   o.publicPaths,
	})
	if err != nil {
		return nil, err
	}
	a.http = httpServer

	grpcServer, err := grpctransport.New(grpctransport.Options{
		Config:        cfg,
		Logger:        logger,
		Metrics:       a.metrics,
		Limiter:       a.limiter,
		Authenticator: authenticator,
		Tracer:        tracer.Tracer,
		Recorder:      a.recorder,
		Ready:         a.ready,
	})
	if err != nil {
		return nil, err
	}
	a.grpc = grpcServer

	if cfg.TCP.Enabled {
		tcpServer, err := tcptransport.New(tcptransport.Options{
			Config:     cfg,
			Logger:     logger,
			Metrics:    a.metrics,
			Limiter:    a.limiter,
			Recorder:   a.recorder,
			Ready:      a.ready,
			Dispatcher: dispatcher,
		})
		if err != nil {
			return nil, err
		}
		a.tcp = tcpServer
	}

	var timeSeries store.TimeSeries
	if a.store != nil {
		timeSeries = a.store
	}
	adminServer, err := admin.New(admin.Options{
		Config:        cfg,
		Logger:        logger,
		Log:           logHandle,
		Metrics:       a.metrics,
		Ready:         a.ready,
		TimeSeries:    timeSeries,
		Reload:        a.Reload,
		CurrentConfig: a.CurrentConfig,
	})
	if err != nil {
		return nil, err
	}
	a.admin = adminServer

	return a, nil
}

// RegisterHTTP adds routes to the Hertz engine. It must be called before Run.
func (a *App) RegisterHTTP(fn func(*server.Hertz)) error {
	if fn == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.started {
		return ErrStarted
	}
	a.httpRegs = append(a.httpRegs, fn)
	return nil
}

// RegisterGRPC registers gRPC services. It must be called before Run.
func (a *App) RegisterGRPC(fn func(*ggrpc.Server)) error {
	if fn == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.started {
		return ErrStarted
	}
	a.grpcRegs = append(a.grpcRegs, fn)
	return nil
}

// RegisterJSONRPC registers JSON-RPC methods on the shared dispatcher used by
// both the HTTP /rpc endpoint and the TCP transport. It must be called before
// Run.
func (a *App) RegisterJSONRPC(fn func(*jsonrpc.Dispatcher)) error {
	if fn == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.started {
		return ErrStarted
	}
	a.rpcRegs = append(a.rpcRegs, fn)
	return nil
}

// RegisterAdmin adds routes to the admin mux. It must be called before Run.
func (a *App) RegisterAdmin(fn func(*http.ServeMux)) error {
	if fn == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.started {
		return ErrStarted
	}
	a.adminRegs = append(a.adminRegs, fn)
	return nil
}

// Run applies pending registrations, serves until ctx is cancelled or a server
// fails, then shuts down gracefully. It returns nil on a clean shutdown.
func (a *App) Run(ctx context.Context) error {
	cfg := a.current()
	logger := a.log.Logger()

	a.mu.Lock()
	if a.started {
		a.mu.Unlock()
		return errors.New("gosvc: Run called twice")
	}
	a.started = true
	for _, fn := range a.httpRegs {
		fn(a.http.Engine())
	}
	for _, fn := range a.grpcRegs {
		fn(a.grpc.Server())
	}
	for _, fn := range a.rpcRegs {
		fn(a.dispatcher)
	}
	if mux := a.admin.Mux(); mux != nil {
		for _, fn := range a.adminRegs {
			fn(mux)
		}
	}
	a.mu.Unlock()

	logger.Info("service starting",
		"http", a.http.Addr(),
		"grpc", a.grpc.Addr(),
		"admin", a.admin.Addr(),
		"tcp", a.TCPAddr(),
		"auth", cfg.Auth.Enabled,
		"tracing", cfg.Telemetry.Enabled,
		"redis", cfg.Storage.Redis.Enabled,
		"hotReload", a.opts.hotReload,
	)
	a.ready.Set(true)
	defer a.ready.Set(false)

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error { return a.http.Serve(ctx) })
	group.Go(func() error { return a.grpc.Serve(ctx) })
	group.Go(func() error { return a.admin.Serve(ctx) })
	if a.tcp != nil {
		group.Go(func() error { return a.tcp.Serve(ctx) })
	}
	if a.recorder != nil {
		group.Go(func() error {
			a.recorder.Run(ctx)
			return nil
		})
	}
	if a.opts.hotReload && a.opts.source.Path != "" {
		group.Go(func() error {
			return reload.New(a.opts.source.Path, logger, a.loadConfig, a.applyConfig).Run(ctx)
		})
	}
	group.Go(func() error {
		a.limiter.Cleanup(ctx)
		return nil
	})

	err := group.Wait()

	if a.telemetry.Enabled() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutdownErr := a.telemetry.Shutdown(shutdownCtx); shutdownErr != nil {
			logger.Warn("telemetry shutdown returned error", "error", shutdownErr)
		}
	}
	if a.store != nil {
		_ = a.store.Close()
	}

	logger.Info("service stopped")
	return err
}

// Reload re-reads the configuration file and applies the reloadable sections
// (log, auth, limiter), then calls the WithOnReload hook. It backs the admin
// POST /debug/reload endpoint.
func (a *App) Reload() error {
	if !a.opts.hotReload || a.opts.source.Path == "" {
		return errors.New("gosvc: hot reload is disabled")
	}
	next, err := a.loadConfig()
	if err != nil {
		return err
	}
	a.applyConfig(next)
	return nil
}

// loadConfig re-reads the configuration source using the generic loader, which
// keeps the same defaults < file < environment precedence as startup.
func (a *App) loadConfig() (*config.Config, error) {
	return a.opts.source.Load[config.Config]()
}

// CurrentConfig returns a redacted snapshot for the admin API.
func (a *App) CurrentConfig() *config.Config {
	cfg := a.current()
	if cfg == nil {
		return nil
	}
	return cfg.Redacted()
}

// Config returns the live configuration owned by the application.
func (a *App) Config() *config.Config { return a.current() }

// Logger returns the root application logger.
func (a *App) Logger() *slog.Logger { return a.log.Logger() }

// Log returns the logging handle (level control, sampling stats, reload).
func (a *App) Log() *logging.Handle { return a.log }

// Metrics returns the Prometheus metrics bundle.
func (a *App) Metrics() *observability.Metrics { return a.metrics }

// Auth returns the authenticator used by the HTTP and gRPC transports.
func (a *App) Auth() *auth.Authenticator { return a.auth }

// Recorder returns the time-series recorder, or nil when Redis is disabled.
func (a *App) Recorder() store.Recorder { return a.recorder }

// Store returns the Redis store, or nil when Redis is disabled.
func (a *App) Store() *redisx.Store { return a.store }

// HTTPAddr returns the effective REST/JSON-RPC address.
func (a *App) HTTPAddr() string { return a.http.Addr() }

// GRPCAddr returns the effective gRPC address.
func (a *App) GRPCAddr() string { return a.grpc.Addr() }

// TCPAddr returns the effective line-delimited JSON-RPC address, or "" when the
// TCP transport is disabled.
func (a *App) TCPAddr() string {
	if a.tcp == nil {
		return ""
	}
	return a.tcp.Addr()
}

// AdminAddr returns the effective admin address.
func (a *App) AdminAddr() string { return a.admin.Addr() }

func (a *App) current() *config.Config { return a.cfg.Load() }

// applyConfig applies a reloaded configuration. Log, auth and limiter are hot
// reloadable; everything else is reported as requiring a restart.
func (a *App) applyConfig(next *config.Config) {
	prev := a.current()
	logger := a.log.Logger()
	if prev == nil {
		a.cfg.Store(next)
		return
	}

	changed := make([]string, 0, 4)

	if !reflect.DeepEqual(prev.Log, next.Log) {
		if err := a.log.Reload(next.Log); err != nil {
			logger.Error("failed to apply log configuration", "error", err)
		} else {
			changed = append(changed, "log")
		}
	}

	if !reflect.DeepEqual(prev.Auth, next.Auth) {
		if err := a.auth.Reload(next.Auth); err != nil {
			logger.Error("failed to apply auth configuration", "error", err)
		} else {
			changed = append(changed, "auth")
		}
	}

	if prev.Limiter != next.Limiter {
		a.limiter.SetRate(next.Limiter.RPS, next.Limiter.Burst)
		changed = append(changed, "limiter")
	}

	restart := restartRequiredFields(prev, next)
	a.cfg.Store(next)

	if a.opts.onReload != nil {
		if err := a.opts.onReload(next); err != nil {
			logger.Error("application reload hook failed", "error", err)
		}
	}

	logger.Info("configuration reloaded", "changed", changed, "restartRequired", restart)
}

// restartRequiredFields lists the changed sections that cannot be applied
// without restarting the process.
func restartRequiredFields(prev, next *config.Config) []string {
	var fields []string
	if prev.Service != next.Service {
		fields = append(fields, "service")
	}
	if prev.HTTP != next.HTTP {
		fields = append(fields, "http")
	}
	if prev.GRPC != next.GRPC {
		fields = append(fields, "grpc")
	}
	if prev.TCP != next.TCP {
		fields = append(fields, "tcp")
	}
	if prev.Admin != next.Admin {
		fields = append(fields, "admin")
	}
	if prev.Telemetry != next.Telemetry {
		fields = append(fields, "telemetry")
	}
	if prev.Storage != next.Storage {
		fields = append(fields, "storage")
	}
	return fields
}

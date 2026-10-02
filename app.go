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
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"golang.org/x/sync/errgroup"
	ggrpc "google.golang.org/grpc"

	"github.com/Tokimorphling/gosvc/auth"
	"github.com/Tokimorphling/gosvc/config"
	"github.com/Tokimorphling/gosvc/health"
	"github.com/Tokimorphling/gosvc/internal/admin"
	"github.com/Tokimorphling/gosvc/internal/reload"
	"github.com/Tokimorphling/gosvc/internal/telemetry"
	"github.com/Tokimorphling/gosvc/internal/version"
	"github.com/Tokimorphling/gosvc/jsonrpc"
	"github.com/Tokimorphling/gosvc/logging"
	"github.com/Tokimorphling/gosvc/observability"
	"github.com/Tokimorphling/gosvc/ratelimit"
	"github.com/Tokimorphling/gosvc/state"
	"github.com/Tokimorphling/gosvc/store"
	"github.com/Tokimorphling/gosvc/store/postgres"
	"github.com/Tokimorphling/gosvc/store/redis"
	grpctransport "github.com/Tokimorphling/gosvc/transport/grpc"
	httptransport "github.com/Tokimorphling/gosvc/transport/http"
	tcptransport "github.com/Tokimorphling/gosvc/transport/tcp"
)

// ErrStarted is returned when handlers are registered after Run has been called.
var ErrStarted = errors.New("gosvc: application already started")

// ErrClosed is returned when an operation is attempted after App.Close.
var ErrClosed = errors.New("gosvc: application closed")

// ErrStorageDisabled is returned by WithStore and WithPostgres when their
// respective connection is not currently enabled.
var ErrStorageDisabled = errors.New("gosvc: storage is disabled")

// Option customises the runtime.
type Option func(*options)

type options struct {
	logger      *logging.Handle
	version     string
	hotReload   bool
	source      config.Source
	publicPaths []string
	onReload    func(*config.Config) error

	tcpCodec          tcptransport.Codec
	tcpCallbacks      tcptransport.Callbacks
	tcpDispatcher     *jsonrpc.Dispatcher
	jsonrpcMiddleware []jsonrpc.Middleware

	onShutdown []func()
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
		o.source = config.Source{Path: path, EnvPrefix: envPrefix, StrictRuntime: true}
	}
}

// WithPublicPaths lets the listed HTTP paths bypass authentication, in
// addition to /healthz and /readyz.
func WithPublicPaths(paths ...string) Option {
	return func(o *options) { o.publicPaths = append(o.publicPaths, paths...) }
}

// WithOnReload registers an application hook invoked after the runtime applied
// its own reloadable sections. Use it to reload application-specific settings.
// The hook runs inside the reload transaction and must not call Reload; before
// Run, it must not call Close either, as both wait for that transaction.
func WithOnReload(fn func(*config.Config) error) Option {
	return func(o *options) { o.onReload = fn }
}

// WithTCPCodec installs a custom frame dialect on the TCP transport (see
// transport/tcp.Codec). Nil keeps the strict JSON-RPC 2.0 behaviour.
func WithTCPCodec(codec tcptransport.Codec) Option {
	return func(o *options) { o.tcpCodec = codec }
}

// WithTCPCallbacks installs connection lifecycle callbacks on the TCP
// transport: OnConnect runs when a connection is established (and eagerly
// creates its push Session), OnDisconnect exactly once when it ends. Use it
// for reliable connection-keyed registry cleanup.
func WithTCPCallbacks(cb tcptransport.Callbacks) Option {
	return func(o *options) { o.tcpCallbacks = cb }
}

// WithJSONRPCMiddleware appends middlewares to the shared JSON-RPC
// dispatcher. They apply to every method on every transport that dispatches
// through it (HTTP /rpc, TCP default path and custom codecs), which makes
// them the natural place for per-method authorisation, validation or
// feature switches. Equivalent to calling Dispatcher.Use inside
// RegisterJSONRPC.
func WithJSONRPCMiddleware(mw ...jsonrpc.Middleware) Option {
	return func(o *options) { o.jsonrpcMiddleware = append(o.jsonrpcMiddleware, mw...) }
}

// WithTCPDispatcher gives the TCP transport a dedicated method table. The
// shared dispatcher stays with the HTTP /rpc endpoint; the TCP transport
// dispatches only on the given dispatcher, which the application builds and
// populates before gosvc.New. Use it to keep a wire-specific protocol (for
// example stratum mining.*) off HTTP /rpc or to apply different middleware.
// The runtime installs its default metrics observer unless the dispatcher
// already has one, so per-method RPC metrics are recorded without wiring;
// call SetObserver first to substitute a different observer. RegisterJSONRPC
// still registers on the shared dispatcher.
func WithTCPDispatcher(d *jsonrpc.Dispatcher) Option {
	return func(o *options) { o.tcpDispatcher = d }
}

// WithOnShutdown registers hooks that run when a started application shuts
// down, after serving has been asked to stop and before the transports close
// their connections: queues can still be drained to live clients there, which
// is the right phase for push.Broker.Shutdown, flushing producers or closing
// registries. The hooks run once, in the order given, and must return
// promptly — a blocked hook delays the shutdown of every transport until its
// own timeout. They do not run when the application is closed before Run.
func WithOnShutdown(fn ...func()) Option {
	return func(o *options) {
		for _, hook := range fn {
			if hook != nil {
				o.onShutdown = append(o.onShutdown, hook)
			}
		}
	}
}

// App owns every transport, the shared dependencies and the lifecycle.
type App struct {
	cfg        state.Snapshot[*config.Config]
	opts       options
	log        *logging.Handle
	ownsLog    bool
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

	storageMu sync.RWMutex
	storage   *storageState
	recorders *store.Holder

	mu        sync.Mutex
	reloadMu  sync.Mutex
	started   bool
	closed    bool
	runCancel context.CancelFunc
	closeOnce sync.Once
	closeErr  error
	httpRegs  []func(*server.Hertz)
	grpcRegs  []func(*ggrpc.Server)
	rpcRegs   []func(*jsonrpc.Dispatcher)
	adminRegs []func(*http.ServeMux)
}

// New builds all components. Listeners are bound here so their addresses are
// known before Run starts serving traffic.
func New(cfg *config.Config, opts ...Option) (app *App, err error) {
	if cfg == nil {
		return nil, errors.New("gosvc: nil config")
	}
	cfg = cfg.Clone()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("gosvc: invalid config: %w", err)
	}
	o := options{version: version.Version}
	for _, opt := range opts {
		if opt == nil {
			return nil, errors.New("gosvc: nil option")
		}
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
		ownsLog: o.logger == nil,
		metrics: observability.New(cfg.Service.Name),
		ready:   &health.Ready{},
		limiter: ratelimit.New(cfg.Limiter.RPS, cfg.Limiter.Burst),
	}
	defer func() {
		if err != nil {
			_ = a.closeResources()
		}
	}()
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

	a.recorders = store.NewHolder(nil)

	storage, err := a.startStorage(context.Background(), cfg.Storage)
	if err != nil {
		return nil, err
	}
	a.storage = storage
	a.recorders.Set(store.Nilable(storage.recorder))

	// Readiness reflects the database when enabled, and pool metrics are
	// exported through a provider so a reload can swap the pool.
	a.ready.AddCheck("postgres", func(ctx context.Context) error {
		err := a.WithPostgres(func(db *postgres.DB) error { return db.Ping(ctx) })
		if errors.Is(err, ErrStorageDisabled) {
			return health.ErrSkipped
		}
		return err
	})
	if err := a.metrics.RegisterDBPoolProvider("postgres", func() *sql.DB {
		if db := a.Postgres(); db != nil {
			return db.DB
		}
		return nil
	}); err != nil {
		logger.Warn("failed to register database pool metrics", "error", err)
	}

	// One dispatcher shared by the HTTP /rpc endpoint and the TCP transport, so
	// methods are registered once. Middlewares from WithJSONRPCMiddleware wrap
	// every method, including the built-ins registered below.
	dispatcher := jsonrpc.NewDispatcher()
	if len(o.jsonrpcMiddleware) > 0 {
		dispatcher.Use(o.jsonrpcMiddleware...)
	}
	dispatcher.SetObserver(a.metrics.ObserveJSONRPC)
	dispatcher.Register("system.methods", func(_ context.Context, _ json.RawMessage) (any, error) {
		return dispatcher.Methods(), nil
	})
	a.dispatcher = dispatcher

	httpServer, err := httptransport.New(httptransport.Options{
		Config:        cfg,
		Logger:        logger,
		Metrics:       a.metrics,
		Limiter:       a.limiter,
		Authenticator: authenticator,
		Tracer:        tracer.Tracer,
		Recorder:      a.recorders,
		Ready:         a.ready,
		Dispatcher:    dispatcher,
		PublicPaths:   o.publicPaths,
		Version:       o.version,
		AccessLogger:  logHandle.Access(),
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
		Recorder:      a.recorders,
		Ready:         a.ready,
		AccessLogger:  logHandle.Access(),
	})
	if err != nil {
		return nil, err
	}
	a.grpc = grpcServer

	if cfg.TCP.Enabled {
		tcpDispatcher := dispatcher
		if o.tcpDispatcher != nil {
			tcpDispatcher = o.tcpDispatcher
			// The dedicated table gets the default observability unless the
			// application installed its own observer before gosvc.New;
			// middlewares stay per-table by design.
			tcpDispatcher.SetObserverIfAbsent(a.metrics.ObserveJSONRPC)
		}
		tcpServer, err := tcptransport.New(tcptransport.Options{
			Config:     cfg,
			Logger:     logger,
			Metrics:    a.metrics,
			Limiter:    a.limiter,
			Recorder:   a.recorders,
			Ready:      a.ready,
			Dispatcher: tcpDispatcher,
			Tracer:     tracer.Tracer,
			Codec:      o.tcpCodec,
			Callbacks:  o.tcpCallbacks,
		})
		if err != nil {
			return nil, err
		}
		a.tcp = tcpServer
	}

	adminServer, err := admin.New(admin.Options{
		Config:          cfg,
		Logger:          logger,
		Log:             logHandle,
		Metrics:         a.metrics,
		Ready:           a.ready,
		TimeSeriesQuery: a.queryTimeSeries,
		Reload:          a.Reload,
		CurrentConfig:   a.CurrentConfig,
		Version:         o.version,
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
	if a.closed {
		return ErrClosed
	}
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
	if a.closed {
		return ErrClosed
	}
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
	if a.closed {
		return ErrClosed
	}
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
	if a.closed {
		return ErrClosed
	}
	if a.started {
		return ErrStarted
	}
	a.adminRegs = append(a.adminRegs, fn)
	return nil
}

// Run applies pending registrations, serves until ctx is cancelled or a server
// fails, then shuts down gracefully. It returns nil on a clean shutdown.
func (a *App) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("gosvc: nil run context")
	}
	logger := a.log.Logger()

	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return ErrClosed
	}
	if a.started {
		a.mu.Unlock()
		return errors.New("gosvc: Run called twice")
	}
	runCtx, cancel := context.WithCancel(ctx)
	a.runCancel = cancel
	a.started = true
	httpRegs := append([]func(*server.Hertz){}, a.httpRegs...)
	grpcRegs := append([]func(*ggrpc.Server){}, a.grpcRegs...)
	rpcRegs := append([]func(*jsonrpc.Dispatcher){}, a.rpcRegs...)
	adminRegs := append([]func(*http.ServeMux){}, a.adminRegs...)
	a.mu.Unlock()
	defer func() {
		cancel()
		_ = a.closeResources()
	}()
	for _, fn := range httpRegs {
		fn(a.http.Engine())
	}
	for _, fn := range grpcRegs {
		fn(a.grpc.Server())
	}
	for _, fn := range rpcRegs {
		fn(a.dispatcher)
	}
	if mux := a.admin.Mux(); mux != nil {
		for _, fn := range adminRegs {
			fn(mux)
		}
	}
	if runCtx.Err() != nil {
		return a.closeResources()
	}
	cfg := a.current()

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

	// Shutdown runs in two phases. The errgroup context fires when serving
	// has been asked to stop (parent context cancelled, Close called) or a
	// transport failed. The WithOnShutdown hooks then run while the
	// transports still serve, so a drain (push.Broker.Shutdown) still
	// reaches live clients; only afterwards do the transports stop.
	group, groupCtx := errgroup.WithContext(runCtx)
	serveCtx, stopServing := context.WithCancel(context.WithoutCancel(groupCtx))
	go func() {
		<-groupCtx.Done()
		for _, hook := range a.opts.onShutdown {
			hook()
		}
		stopServing()
	}()
	group.Go(func() error { return a.http.Serve(serveCtx) })
	group.Go(func() error { return a.grpc.Serve(serveCtx) })
	group.Go(func() error { return a.admin.Serve(serveCtx) })
	if a.tcp != nil {
		group.Go(func() error { return a.tcp.Serve(serveCtx) })
	}
	if a.opts.hotReload && a.opts.source.Path != "" {
		group.Go(func() error {
			return reload.New(a.opts.source.Path, logger, a.Reload).Run(serveCtx)
		})
	}
	group.Go(func() error {
		a.limiter.Cleanup(serveCtx)
		return nil
	})

	err := group.Wait()

	logger.Info("service stopped")
	return errors.Join(err, a.closeResources())
}

// Close releases resources allocated by New. Before Run it closes the bound
// listeners, storage, tracing and internally created log sinks. During Run it
// requests a graceful shutdown; Run completes the drain and releases resources.
// Close is idempotent and safe to call from a handler. A Handle supplied
// through WithLogger stays caller-owned. A pre-Run WithOnReload hook must not
// call Close because the hook itself holds the reload transaction lock.
func (a *App) Close() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	a.closed = true
	cancel := a.runCancel
	a.mu.Unlock()
	if cancel != nil {
		cancel()
		return nil
	}
	return a.closeResources()
}

func (a *App) closeResources() error {
	a.closeOnce.Do(func() {
		a.mu.Lock()
		a.closed = true
		a.mu.Unlock()
		a.reloadMu.Lock()
		defer a.reloadMu.Unlock()
		if a.ready != nil {
			a.ready.Set(false)
		}
		var errs []error
		closeListener := func(err error) {
			if err != nil && !errors.Is(err, net.ErrClosed) {
				errs = append(errs, err)
			}
		}
		if a.admin != nil {
			closeListener(a.admin.Close())
		}
		if a.tcp != nil {
			closeListener(a.tcp.Close())
		}
		if a.grpc != nil {
			closeListener(a.grpc.Close())
		}
		if a.http != nil {
			closeListener(a.http.Close())
		}
		if a.telemetry != nil && a.telemetry.Enabled() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			errs = append(errs, a.telemetry.Shutdown(shutdownCtx))
			cancel()
		}
		a.storageMu.Lock()
		state := a.storage
		a.storage = nil
		if a.recorders != nil {
			a.recorders.Set(nil)
		}
		idle := retireStorage(state)
		a.storageMu.Unlock()
		if idle != nil {
			<-idle
		}
		a.stopStorage(state)
		if a.ownsLog {
			errs = append(errs, a.log.Close())
		}
		a.closeErr = errors.Join(errs...)
	})
	return a.closeErr
}

// Reload re-reads the configuration file and applies the reloadable sections
// (log, auth, limiter), then calls the WithOnReload hook. It backs the admin
// POST /debug/reload endpoint.
func (a *App) Reload() error {
	if !a.opts.hotReload || a.opts.source.Path == "" {
		return errors.New("gosvc: hot reload is disabled")
	}
	a.reloadMu.Lock()
	defer a.reloadMu.Unlock()
	a.mu.Lock()
	closed := a.closed
	a.mu.Unlock()
	if closed {
		return ErrClosed
	}
	next, err := a.loadConfig()
	if err != nil {
		return err
	}
	return a.applyConfigLocked(next)
}

// loadConfig re-reads the configuration source using the generic loader, which
// keeps the same defaults < file < environment precedence as startup.
func (a *App) loadConfig() (*config.Config, error) {
	next, meta, err := a.opts.source.LoadWithMetadata[config.Config]()
	if err != nil {
		return nil, err
	}
	// A removed or misspelled [auth] section must not silently disable an
	// authenticator that was active. An intentional disable is explicit.
	if prev := a.current(); prev != nil && prev.Auth.Enabled && !next.Auth.Enabled && !meta.IsDefined("auth", "enabled") {
		return nil, errors.New("gosvc: disabling auth requires explicit auth.enabled = false")
	}
	return next, nil
}

// CurrentConfig returns a redacted snapshot for the admin API.
func (a *App) CurrentConfig() *config.Config {
	cfg := a.current()
	if cfg == nil {
		return nil
	}
	return cfg.Redacted()
}

// Config returns an independent snapshot of settings currently in effect.
func (a *App) Config() *config.Config { return a.current().Clone() }

// Logger returns the root application logger.
func (a *App) Logger() *slog.Logger { return a.log.Logger() }

// Log returns the logging handle (level control, sampling stats, reload).
func (a *App) Log() *logging.Handle { return a.log }

// Metrics returns the Prometheus metrics bundle.
func (a *App) Metrics() *observability.Metrics { return a.metrics }

// Auth returns the authenticator used by the HTTP and gRPC transports.
func (a *App) Auth() *auth.Authenticator { return a.auth }

// JSONRPCDispatcher returns the shared JSON-RPC dispatcher used by the HTTP
// /rpc endpoint and the TCP transport, so HTTP registration callbacks can add
// methods in the same place they wire routes.
func (a *App) JSONRPCDispatcher() *jsonrpc.Dispatcher { return a.dispatcher }

// Recorder returns a snapshot of the time-series recorder, or nil when Redis
// is disabled. Use StableRecorder when retaining the recorder across reloads.
func (a *App) Recorder() store.Recorder { return a.recorders.Current() }

// StableRecorder returns a recorder whose target follows storage reloads. It
// is a no-op while Redis is disabled, and is safe to cache for App's lifetime.
func (a *App) StableRecorder() store.Recorder { return a.recorders }

// Store returns a snapshot of the active Redis store, or nil when disabled.
// A reload may close it immediately after return. Use WithStore for operations
// that must remain safe while storage is reloaded.
func (a *App) Store() *redis.Store {
	if current := a.currentStorage(); current != nil {
		return current.store
	}
	return nil
}

// Postgres returns a snapshot of the active pool, or nil when disabled. A
// reload may close it immediately after return. Use WithPostgres for operations
// that must remain safe while storage is reloaded.
func (a *App) Postgres() *postgres.DB {
	if current := a.currentStorage(); current != nil {
		return current.postgres
	}
	return nil
}

// WithStore leases the active Redis store for fn. A reload waits for that
// generation's callbacks to finish before closing it. Nested WithStore and
// WithPostgres calls are safe. The callback must not call Reload or Close,
// since these operations wait for its lease to finish. Disabled storage
// returns ErrStorageDisabled without invoking fn.
func (a *App) WithStore(fn func(*redis.Store) error) error {
	if fn == nil {
		return errors.New("gosvc: nil storage callback")
	}
	a.storageMu.Lock()
	state := a.storage
	if state == nil || state.store == nil {
		a.storageMu.Unlock()
		return ErrStorageDisabled
	}
	state.leases++
	a.storageMu.Unlock()
	defer a.releaseStorage(state)
	return fn(state.store)
}

// WithPostgres leases the active PostgreSQL pool for fn. A reload waits for
// that generation's callbacks to finish before closing it. Nested storage
// leases are safe; callbacks must not call Reload or Close.
func (a *App) WithPostgres(fn func(*postgres.DB) error) error {
	if fn == nil {
		return errors.New("gosvc: nil storage callback")
	}
	a.storageMu.Lock()
	state := a.storage
	if state == nil || state.postgres == nil {
		a.storageMu.Unlock()
		return ErrStorageDisabled
	}
	state.leases++
	a.storageMu.Unlock()
	defer a.releaseStorage(state)
	return fn(state.postgres)
}

func (a *App) releaseStorage(state *storageState) {
	a.storageMu.Lock()
	state.leases--
	if state.leases == 0 && state.idle != nil {
		close(state.idle)
		state.idle = nil
	}
	a.storageMu.Unlock()
}

func (a *App) queryTimeSeries(ctx context.Context, metric string, from, to time.Time) ([]store.Bucket, error) {
	var buckets []store.Bucket
	err := a.WithStore(func(redisStore *redis.Store) error {
		var queryErr error
		buckets, queryErr = redisStore.Range(ctx, metric, from, to)
		return queryErr
	})
	if errors.Is(err, ErrStorageDisabled) {
		return nil, admin.ErrTimeSeriesDisabled
	}
	return buckets, err
}

func (a *App) currentStorage() *storageState {
	a.storageMu.RLock()
	defer a.storageMu.RUnlock()
	return a.storage
}

// Health exposes readiness so applications can register their own dependency
// checks.
func (a *App) Health() *health.Ready { return a.ready }

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

// applyConfig applies reloadable sections and publishes only values that took
// effect. A failed section remains at its previous value so future reloads
// retry it. The watcher and manual reload are serialized.
func (a *App) applyConfig(next *config.Config) error {
	a.reloadMu.Lock()
	defer a.reloadMu.Unlock()
	return a.applyConfigLocked(next)
}

// applyConfigLocked requires reloadMu. Reload holds it across loading,
// security checks and application so concurrent triggers cannot bypass the
// explicit-auth-disable guard using a stale effective config.
func (a *App) applyConfigLocked(next *config.Config) error {
	if next == nil {
		return errors.New("gosvc: nil reload config")
	}
	a.mu.Lock()
	closed := a.closed
	a.mu.Unlock()
	if closed {
		return ErrClosed
	}

	prev := a.current()
	logger := a.log.Logger()
	if prev == nil {
		return errors.New("gosvc: no active config")
	}

	changed := make([]string, 0, 4)
	effective := prev.Clone()
	var failures []error
	restart := restartRequiredFields(prev, next)
	if len(restart) > 0 {
		failures = append(failures, fmt.Errorf("restart required for: %v", restart))
	}
	// Access logging is wired at construction. Its enabled state cannot be
	// changed by Reload, even when other log settings can be applied.
	logConfig := next.Log
	logConfig.Access.Enabled = prev.Log.Access.Enabled
	if !prev.Log.Access.Enabled {
		logConfig.Access = prev.Log.Access
	}

	if !reflect.DeepEqual(prev.Log, logConfig) {
		if err := a.log.Reload(logConfig); err != nil {
			logger.Error("failed to apply log configuration", "error", err)
			failures = append(failures, fmt.Errorf("log: %w", err))
		} else {
			changed = append(changed, "log")
			effective.Log = logConfig
		}
	}

	if !reflect.DeepEqual(prev.Auth, next.Auth) {
		if err := a.auth.Reload(next.Auth); err != nil {
			logger.Error("failed to apply auth configuration", "error", err)
			failures = append(failures, fmt.Errorf("auth: %w", err))
		} else {
			changed = append(changed, "auth")
			effective.Auth = next.Auth
		}
	}

	if prev.Limiter != next.Limiter {
		a.limiter.SetRate(next.Limiter.RPS, next.Limiter.Burst)
		changed = append(changed, "limiter")
		effective.Limiter = next.Limiter
	}

	if !reflect.DeepEqual(prev.Storage, next.Storage) {
		if err := a.reloadStorage(next.Storage); err != nil {
			logger.Error("failed to rebuild storage, keeping the current connections", "error", err)
			failures = append(failures, fmt.Errorf("storage: %w", err))
		} else {
			changed = append(changed, "storage")
			effective.Storage = next.Storage
		}
	}

	a.cfg.Store(effective.Clone())

	if a.opts.onReload != nil {
		if err := a.opts.onReload(effective.Clone()); err != nil {
			logger.Error("application reload hook failed", "error", err)
			failures = append(failures, fmt.Errorf("reload hook: %w", err))
		}
	}

	logger.Info("configuration reloaded", "changed", changed, "restartRequired", restart)
	return errors.Join(failures...)
}

// restartRequiredFields lists the changed sections that cannot be applied
// without restarting the process.
func restartRequiredFields(prev, next *config.Config) []string {
	var fields []string
	if prev.Service != next.Service {
		fields = append(fields, "service")
	}
	// HTTP contains a slice (CORS origins), so it is compared with DeepEqual.
	if !reflect.DeepEqual(prev.HTTP, next.HTTP) {
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
	if prev.Log.Access.Enabled != next.Log.Access.Enabled {
		fields = append(fields, "log.access.enabled")
	} else if !prev.Log.Access.Enabled && !reflect.DeepEqual(prev.Log.Access, next.Log.Access) {
		fields = append(fields, "log.access")
	}
	return fields
}

// storageState holds the optional storage connections plus the recorder
// goroutine that belongs to them.
type storageState struct {
	store    *redis.Store
	recorder *redis.Recorder
	postgres *postgres.DB
	cancel   context.CancelFunc
	// leases and idle are protected by App.storageMu. A retired generation
	// stays open until every callback that acquired it has returned.
	leases int
	idle   chan struct{}
}

// retireStorage is called with storageMu held. A non-nil channel signals
// when callbacks using the old generation have all returned.
func retireStorage(state *storageState) <-chan struct{} {
	if state == nil || state.leases == 0 {
		return nil
	}
	state.idle = make(chan struct{})
	return state.idle
}

// startStorage opens the configured storage connections. On failure nothing is
// left behind.
func (a *App) startStorage(ctx context.Context, cfg config.StorageConfig) (*storageState, error) {
	state := &storageState{}

	if cfg.Redis.Enabled {
		redisStore, err := redis.New(ctx, cfg.Redis)
		if err != nil {
			return nil, err
		}
		state.store = redisStore

		recorderCtx, cancel := context.WithCancel(ctx)
		state.cancel = cancel
		state.recorder = redis.NewRecorder(redisStore, cfg.Redis.QueueSize, a.log.Logger())
		go state.recorder.Run(recorderCtx)
	}

	if cfg.Postgres.Enabled {
		db, err := postgres.New(ctx, cfg.Postgres)
		if err != nil {
			a.stopStorage(state)
			return nil, err
		}
		state.postgres = db
	}

	return state, nil
}

// stopStorage flushes the recorder and then closes the connections.
func (a *App) stopStorage(state *storageState) {
	if state == nil {
		return
	}
	if state.recorder != nil {
		state.cancel()
		state.recorder.Wait()
	}
	if state.store != nil {
		_ = state.store.Close()
	}
	if state.postgres != nil {
		_ = state.postgres.Close()
	}
}

// reloadStorage builds the new storage connections, swaps them in and then tears
// down the previous ones. A failure keeps the current state.
func (a *App) reloadStorage(cfg config.StorageConfig) error {
	next, err := a.startStorage(context.Background(), cfg)
	if err != nil {
		return err
	}

	a.storageMu.Lock()
	previous := a.storage
	// Holder.Set waits for in-flight recordings before old recorder shutdown.
	a.recorders.Set(store.Nilable(next.recorder))
	a.storage = next
	idle := retireStorage(previous)
	a.storageMu.Unlock()

	if idle != nil {
		<-idle
	}
	a.stopStorage(previous)
	return nil
}

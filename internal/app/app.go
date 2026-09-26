// Package app wires all components and owns the service lifecycle.
package app

import (
	"context"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	adminapi "example.com/gosvc/internal/admin"
	"example.com/gosvc/internal/auth"
	"example.com/gosvc/internal/config"
	"example.com/gosvc/internal/health"
	"example.com/gosvc/internal/observability"
	"example.com/gosvc/internal/ratelimit"
	"example.com/gosvc/internal/service/greeter"
	"example.com/gosvc/internal/store"
	"example.com/gosvc/internal/store/redisx"
	"example.com/gosvc/internal/telemetry"
	grpcapi "example.com/gosvc/internal/transport/grpc"
	httpapi "example.com/gosvc/internal/transport/http"
	tcpapi "example.com/gosvc/internal/transport/tcp"
	"example.com/gosvc/internal/version"
)

// App owns every transport and the shared dependencies.
type App struct {
	cfg       *config.Config
	logger    *slog.Logger
	ready     *health.Ready
	limiter   *ratelimit.Limiter
	http      *httpapi.Server
	grpc      *grpcapi.Server
	tcp       *tcpapi.Server
	admin     *adminapi.Server
	telemetry *telemetry.Provider
	store     *redisx.Store
	recorder  *redisx.Recorder
}

// New builds all components. Listeners are bound here so their addresses are
// known before Run starts serving traffic.
func New(cfg *config.Config, logger *slog.Logger, level *slog.LevelVar) (*App, error) {
	ready := &health.Ready{}
	metrics := observability.New(cfg.Service.Name)
	service := greeter.New(cfg.Service.Name, version.Version)
	limiter := ratelimit.New(cfg.Limiter.RPS, cfg.Limiter.Burst)

	authenticator, err := auth.New(cfg.Auth)
	if err != nil {
		return nil, err
	}

	tracer, err := telemetry.New(context.Background(), cfg.Telemetry, cfg.Service.Name, cfg.Service.Env, version.Version)
	if err != nil {
		return nil, err
	}

	var (
		redisStore *redisx.Store
		recorder   *redisx.Recorder
	)
	if cfg.Storage.Redis.Enabled {
		redisStore, err = redisx.New(context.Background(), cfg.Storage.Redis)
		if err != nil {
			return nil, err
		}
		recorder = redisx.NewRecorder(redisStore, cfg.Storage.Redis.QueueSize, logger)
	}

	httpServer, err := httpapi.New(httpapi.Options{
		Config:        cfg,
		Service:       service,
		Logger:        logger,
		Level:         level,
		Metrics:       metrics,
		Limiter:       limiter,
		Authenticator: authenticator,
		Tracer:        tracer.Tracer,
		Recorder:      recorder,
		Ready:         ready,
	})
	if err != nil {
		return nil, err
	}

	grpcServer, err := grpcapi.New(grpcapi.Options{
		Config:        cfg,
		Service:       service,
		Logger:        logger,
		Metrics:       metrics,
		Limiter:       limiter,
		Authenticator: authenticator,
		Tracer:        tracer.Tracer,
		Recorder:      recorder,
		Ready:         ready,
	})
	if err != nil {
		return nil, err
	}

	var tcpServer *tcpapi.Server
	if cfg.TCP.Enabled {
		tcpServer, err = tcpapi.New(tcpapi.Options{
			Config:   cfg,
			Service:  service,
			Logger:   logger,
			Metrics:  metrics,
			Limiter:  limiter,
			Recorder: recorder,
			Ready:    ready,
		})
		if err != nil {
			return nil, err
		}
	}

	var timeSeries store.TimeSeries
	if redisStore != nil {
		timeSeries = redisStore
	}
	adminServer, err := adminapi.New(adminapi.Options{
		Config:     cfg,
		Logger:     logger,
		Level:      level,
		Metrics:    metrics,
		Ready:      ready,
		TimeSeries: timeSeries,
	})
	if err != nil {
		return nil, err
	}

	return &App{
		cfg:       cfg,
		logger:    logger,
		ready:     ready,
		limiter:   limiter,
		http:      httpServer,
		grpc:      grpcServer,
		tcp:       tcpServer,
		admin:     adminServer,
		telemetry: tracer,
		store:     redisStore,
		recorder:  recorder,
	}, nil
}

// Run serves until ctx is cancelled or a server fails, then shuts down
// gracefully. It returns nil on a clean shutdown.
func (a *App) Run(ctx context.Context) error {
	a.logger.Info("service starting",
		"version", version.Full(),
		"env", a.cfg.Service.Env,
		"http", a.http.Addr(),
		"grpc", a.grpc.Addr(),
		"admin", a.admin.Addr(),
		"tcp", a.TCPAddr(),
		"auth", a.cfg.Auth.Enabled,
		"tracing", a.cfg.Telemetry.Enabled,
		"redis", a.cfg.Storage.Redis.Enabled,
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
	group.Go(func() error {
		a.limiter.Cleanup(ctx)
		return nil
	})

	err := group.Wait()

	if a.telemetry.Enabled() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutdownErr := a.telemetry.Shutdown(shutdownCtx); shutdownErr != nil {
			a.logger.Warn("telemetry shutdown returned error", "error", shutdownErr)
		}
	}
	if a.store != nil {
		_ = a.store.Close()
	}

	a.logger.Info("service stopped")
	return err
}

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

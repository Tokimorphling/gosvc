// Package app wires all components and owns the service lifecycle.
package app

import (
	"context"
	"log/slog"

	"golang.org/x/sync/errgroup"

	adminapi "example.com/gosvc/internal/admin"
	"example.com/gosvc/internal/config"
	"example.com/gosvc/internal/health"
	"example.com/gosvc/internal/observability"
	"example.com/gosvc/internal/ratelimit"
	"example.com/gosvc/internal/service/greeter"
	grpcapi "example.com/gosvc/internal/transport/grpc"
	httpapi "example.com/gosvc/internal/transport/http"
	"example.com/gosvc/internal/version"
)

// App owns every transport and the shared dependencies.
type App struct {
	cfg     *config.Config
	logger  *slog.Logger
	ready   *health.Ready
	limiter *ratelimit.Limiter
	http    *httpapi.Server
	grpc    *grpcapi.Server
	admin   *adminapi.Server
}

// New builds all components. Listeners are bound here so their addresses are
// known before Run starts serving traffic.
func New(cfg *config.Config, logger *slog.Logger, level *slog.LevelVar) (*App, error) {
	ready := &health.Ready{}
	metrics := observability.New(cfg.Service.Name)
	service := greeter.New(cfg.Service.Name, version.Version)
	limiter := ratelimit.New(cfg.Limiter.RPS, cfg.Limiter.Burst)

	httpServer, err := httpapi.New(httpapi.Options{
		Config:  cfg,
		Service: service,
		Logger:  logger,
		Level:   level,
		Metrics: metrics,
		Limiter: limiter,
		Ready:   ready,
	})
	if err != nil {
		return nil, err
	}

	grpcServer, err := grpcapi.New(grpcapi.Options{
		Config:  cfg,
		Service: service,
		Logger:  logger,
		Metrics: metrics,
		Limiter: limiter,
		Ready:   ready,
	})
	if err != nil {
		return nil, err
	}

	adminServer, err := adminapi.New(adminapi.Options{
		Config:  cfg,
		Logger:  logger,
		Metrics: metrics,
		Ready:   ready,
	})
	if err != nil {
		return nil, err
	}

	return &App{
		cfg:     cfg,
		logger:  logger,
		ready:   ready,
		limiter: limiter,
		http:    httpServer,
		grpc:    grpcServer,
		admin:   adminServer,
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
	)
	a.ready.Set(true)
	defer a.ready.Set(false)

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error { return a.http.Serve(ctx) })
	group.Go(func() error { return a.grpc.Serve(ctx) })
	group.Go(func() error { return a.admin.Serve(ctx) })
	group.Go(func() error {
		a.limiter.Cleanup(ctx)
		return nil
	})

	err := group.Wait()
	a.logger.Info("service stopped")
	return err
}

// HTTPAddr returns the effective REST/JSON-RPC address.
func (a *App) HTTPAddr() string { return a.http.Addr() }

// GRPCAddr returns the effective gRPC address.
func (a *App) GRPCAddr() string { return a.grpc.Addr() }

// AdminAddr returns the effective admin address.
func (a *App) AdminAddr() string { return a.admin.Addr() }

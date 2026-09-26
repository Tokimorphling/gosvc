// Package app wires all components and owns the service lifecycle.
package app

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	adminapi "example.com/gosvc/internal/admin"
	"example.com/gosvc/internal/auth"
	"example.com/gosvc/internal/config"
	"example.com/gosvc/internal/health"
	"example.com/gosvc/internal/logging"
	"example.com/gosvc/internal/observability"
	"example.com/gosvc/internal/ratelimit"
	"example.com/gosvc/internal/reload"
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
	cfg        atomic.Pointer[config.Config]
	configPath string
	log        *logging.Handle
	ready      *health.Ready
	limiter    *ratelimit.Limiter
	auth       *auth.Authenticator
	http       *httpapi.Server
	grpc       *grpcapi.Server
	tcp        *tcpapi.Server
	admin      *adminapi.Server
	telemetry  *telemetry.Provider
	store      *redisx.Store
	recorder   *redisx.Recorder
}

// New builds all components. Listeners are bound here so their addresses are
// known before Run starts serving traffic. configPath may be empty, in which
// case hot reload is disabled.
func New(cfg *config.Config, log *logging.Handle, configPath string) (*App, error) {
	logger := log.Logger()

	a := &App{
		configPath: configPath,
		log:        log,
		ready:      &health.Ready{},
		limiter:    ratelimit.New(cfg.Limiter.RPS, cfg.Limiter.Burst),
	}
	a.cfg.Store(cfg)

	metrics := observability.New(cfg.Service.Name)
	service := greeter.New(cfg.Service.Name, version.Version)

	authenticator, err := auth.New(cfg.Auth)
	if err != nil {
		return nil, err
	}
	a.auth = authenticator

	tracer, err := telemetry.New(context.Background(), cfg.Telemetry, cfg.Service.Name, cfg.Service.Env, version.Version)
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

	httpServer, err := httpapi.New(httpapi.Options{
		Config:        cfg,
		Service:       service,
		Logger:        logger,
		Level:         log.Level(),
		Metrics:       metrics,
		Limiter:       a.limiter,
		Authenticator: authenticator,
		Tracer:        tracer.Tracer,
		Recorder:      a.recorder,
		Ready:         a.ready,
	})
	if err != nil {
		return nil, err
	}
	a.http = httpServer

	grpcServer, err := grpcapi.New(grpcapi.Options{
		Config:        cfg,
		Service:       service,
		Logger:        logger,
		Metrics:       metrics,
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
		tcpServer, err := tcpapi.New(tcpapi.Options{
			Config:   cfg,
			Service:  service,
			Logger:   logger,
			Metrics:  metrics,
			Limiter:  a.limiter,
			Recorder: a.recorder,
			Ready:    a.ready,
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
	adminServer, err := adminapi.New(adminapi.Options{
		Config:        cfg,
		Logger:        logger,
		Log:           log,
		Metrics:       metrics,
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

// Run serves until ctx is cancelled or a server fails, then shuts down
// gracefully. It returns nil on a clean shutdown.
func (a *App) Run(ctx context.Context) error {
	cfg := a.current()
	logger := a.log.Logger()

	logger.Info("service starting",
		"version", version.Full(),
		"env", cfg.Service.Env,
		"http", a.http.Addr(),
		"grpc", a.grpc.Addr(),
		"admin", a.admin.Addr(),
		"tcp", a.TCPAddr(),
		"auth", cfg.Auth.Enabled,
		"tracing", cfg.Telemetry.Enabled,
		"redis", cfg.Storage.Redis.Enabled,
		"hotReload", a.configPath != "",
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
	if a.configPath != "" {
		group.Go(func() error {
			return reload.New(a.configPath, logger, a.applyConfig).Run(ctx)
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

// Reload re-reads the configuration file and applies the reloadable parts. It
// backs the admin POST /debug/reload endpoint.
func (a *App) Reload() error {
	if a.configPath == "" {
		return errors.New("no configuration file to reload")
	}
	next, err := config.Load(a.configPath)
	if err != nil {
		return err
	}
	a.applyConfig(next)
	return nil
}

// CurrentConfig returns a redacted snapshot for the admin API.
func (a *App) CurrentConfig() *config.Config {
	cfg := a.current()
	if cfg == nil {
		return nil
	}
	return cfg.Redacted()
}

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

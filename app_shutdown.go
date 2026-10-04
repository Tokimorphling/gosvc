package gosvc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/Tokimorphling/gosvc/config"
)

// WithShutdownTimeout sets the shared budget for application hooks and
// transport draining. Zero uses the largest enabled transport shutdown timeout.
// Hooks and handlers must honour cancellation; Go cannot terminate a goroutine
// that ignores its context.
func WithShutdownTimeout(timeout time.Duration) Option {
	return func(o *options) { o.shutdownTimeout = timeout }
}

// WithOnShutdownContext registers ordered hooks that receive the shutdown
// deadline. Errors are collected and returned by Run; panics become errors.
// On timeout the runtime stops waiting and proceeds to stop the transports.
// Hooks must release their resources promptly when ctx is cancelled.
func WithOnShutdownContext(fn ...func(context.Context) error) Option {
	return func(o *options) {
		for _, hook := range fn {
			if hook != nil {
				o.onShutdown = append(o.onShutdown, hook)
			}
		}
	}
}

func (a *App) shutdownTimeout(cfg *config.Config) time.Duration {
	if a.opts.shutdownTimeout > 0 {
		return a.opts.shutdownTimeout
	}
	timeout := max(cfg.HTTP.ShutdownTimeout.D(), cfg.GRPC.ShutdownTimeout.D())
	if cfg.TCP.Enabled {
		timeout = max(timeout, cfg.TCP.ShutdownTimeout.D())
	}
	return timeout
}

func (a *App) runShutdownHooks(ctx context.Context) error {
	if len(a.opts.onShutdown) == 0 {
		return nil
	}
	done := make(chan error, 1)
	go func() {
		var errs []error
		for i, hook := range a.opts.onShutdown {
			if err := ctx.Err(); err != nil {
				errs = append(errs, err)
				break
			}
			if err := callShutdownHook(ctx, hook); err != nil {
				errs = append(errs, fmt.Errorf("shutdown hook %d: %w", i+1, err))
			}
		}
		done <- errors.Join(errs...)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("shutdown hooks: %w", ctx.Err())
	}
}

func callShutdownHook(ctx context.Context, hook func(context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return hook(ctx)
}

// WithOnShutdown registers hooks that run when a started application shuts
// down, after serving has been asked to stop and before the transports close
// their connections: queues can still be drained to live clients there, which
// is the right phase for push.Broker.Shutdown, flushing producers or closing
// registries. The hooks run once, in the order given, and must return
// promptly. All hooks share the shutdown budget; a blocked hook cannot prevent
// transport shutdown but may outlive Run. Prefer WithOnShutdownContext for
// cancellable cleanup. Hooks do not run when closed before Run.
func WithOnShutdown(fn ...func()) Option {
	return func(o *options) {
		for _, hook := range fn {
			if hook != nil {
				o.onShutdown = append(o.onShutdown, func(context.Context) error {
					hook()
					return nil
				})
			}
		}
	}
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

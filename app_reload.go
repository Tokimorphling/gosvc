// Hot reload: re-read the configuration source and apply the reloadable
// sections, leaving anything restart-required at its previous value and
// reporting it as a failure so the operator notices.

package gosvc

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/Tokimorphling/gosvc/config"
)

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

	if prev.Log != logConfig {
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
// without restarting the process. Sections are compared with == wherever they
// are comparable; reflect.DeepEqual is reserved for the sections carrying
// slices (HTTP CORS origins, auth API keys, Redis addresses).
func restartRequiredFields(prev, next *config.Config) []string {
	var fields []string
	if prev.Service != next.Service {
		fields = append(fields, "service")
	}
	if !reflect.DeepEqual(prev.HTTP, next.HTTP) { // CORS.AllowOrigins is a slice
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
	} else if !prev.Log.Access.Enabled && prev.Log.Access != next.Log.Access {
		fields = append(fields, "log.access")
	}
	return fields
}

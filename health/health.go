// Package health tracks process readiness and optional dependency probes.
package health

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Tokimorphling/gosvc/state"
)

// Check probes one dependency. It must respect ctx and return quickly.
type Check func(ctx context.Context) error

// ErrSkipped lets a check opt out without affecting readiness, for example when
// the dependency is disabled by configuration.
var ErrSkipped = errors.New("check skipped")

// Ready tracks the readiness flag plus named dependency checks. Health
// endpoints use Check; IsReady is the lifecycle flag for fast admission checks.
type Ready struct {
	ready atomic.Bool

	mu     sync.Mutex // registration only
	checks state.Snapshot[[]namedCheck]
}

type namedCheck struct {
	name  string
	probe Check
}

// Set updates the readiness flag.
func (r *Ready) Set(v bool) { r.ready.Store(v) }

// IsReady reports whether the service flag is set. Dependency checks are not
// evaluated here.
func (r *Ready) IsReady() bool { return r.ready.Load() }

// AddCheck registers a named dependency probe.
func (r *Ready) AddCheck(name string, check Check) {
	if r == nil || check == nil || name == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	checks := slices.Clone(r.checks.Load())
	for i := range checks {
		if checks[i].name == name {
			checks[i].probe = check
			r.checks.Store(checks)
			return
		}
	}
	r.checks.Store(append(checks, namedCheck{name: name, probe: check}))
}

// Check evaluates the readiness flag and every dependency probe. It returns
// whether the service is ready plus per-check detail (nil when no checks are
// registered).
func (r *Ready) Check(ctx context.Context) (bool, map[string]string) {
	if r == nil {
		return true, nil
	}
	checks := r.checks.Load()
	var details map[string]string
	if len(checks) > 0 {
		details = make(map[string]string, len(checks))
	}
	return r.evaluate(ctx, checks, details), details
}

// Healthy evaluates the same probes as Check without allocating a diagnostic
// map. Use it for boolean/status-only readiness responses. IsReady only reads
// the lifecycle flag and never probes dependencies.
func (r *Ready) Healthy(ctx context.Context) bool {
	if r == nil {
		return true
	}
	return r.evaluate(ctx, r.checks.Load(), nil)
}

func (r *Ready) evaluate(ctx context.Context, checks []namedCheck, details map[string]string) bool {
	ready := r.IsReady()
	if len(checks) == 0 {
		return ready && ctx.Err() == nil
	}
	// Preserve an already tighter caller budget without another timer/context.
	const timeout = 3 * time.Second
	if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > timeout {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	for _, check := range checks {
		err := check.probe(ctx)
		var detail string
		switch {
		case err == nil:
			detail = "ok"
		case errors.Is(err, ErrSkipped):
			detail = "skipped"
		default:
			ready = false
			if details != nil {
				detail = err.Error()
			}
		}
		if details != nil {
			details[check.name] = detail
		}
	}
	// Shutdown may have started while a dependency probe was running.
	return ready && r.IsReady() && ctx.Err() == nil
}

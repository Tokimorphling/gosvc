// Package health tracks process readiness and optional dependency probes.
package health

import (
	"context"
	"sync"
	"sync/atomic"
)

// Check probes one dependency. It must respect ctx and return quickly.
type Check func(ctx context.Context) error

// Ready tracks the readiness flag plus named dependency checks. Transports use
// IsReady for their fast path; the admin /readyz endpoint uses Check to report
// per-dependency detail.
type Ready struct {
	ready atomic.Bool

	mu     sync.RWMutex
	checks map[string]Check
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
	if r.checks == nil {
		r.checks = make(map[string]Check)
	}
	r.checks[name] = check
}

// Check evaluates the readiness flag and every dependency probe. It returns
// whether the service is ready plus per-check detail (nil when no checks are
// registered).
func (r *Ready) Check(ctx context.Context) (bool, map[string]string) {
	if r == nil {
		return true, nil
	}

	ready := r.IsReady()

	r.mu.RLock()
	if len(r.checks) == 0 {
		r.mu.RUnlock()
		return ready, nil
	}
	checks := make(map[string]Check, len(r.checks))
	for name, check := range r.checks {
		checks[name] = check
	}
	r.mu.RUnlock()

	details := make(map[string]string, len(checks))
	for name, check := range checks {
		if err := check(ctx); err != nil {
			ready = false
			details[name] = err.Error()
			continue
		}
		details[name] = "ok"
	}
	return ready, details
}

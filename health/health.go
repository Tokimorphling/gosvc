// Package health tracks process readiness.
package health

import "sync/atomic"

// Ready is a concurrency-safe readiness flag.
type Ready struct {
	ready atomic.Bool
}

// Set updates the readiness state.
func (r *Ready) Set(v bool) { r.ready.Store(v) }

// IsReady reports whether the service is ready to receive traffic.
func (r *Ready) IsReady() bool { return r.ready.Load() }

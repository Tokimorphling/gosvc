// Package ratelimit implements a per-key token bucket limiter with runtime
// reconfiguration.
package ratelimit

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const visitorTTL = 10 * time.Minute

// Limiter keeps one token bucket per key (for example a client IP).
//
// A Limiter is always safe to use: when the configured rate is <= 0 it allows
// everything. SetRate swaps the parameters at runtime (config reload) and drops
// the existing buckets so the new rate applies immediately.
type Limiter struct {
	mu       sync.Mutex
	visitors map[string]*visitor
	limit    rate.Limit
	burst    int
	ttl      time.Duration
}

type visitor struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// New builds a limiter. rps <= 0 disables limiting (Allow always returns true).
func New(rps float64, burst int) *Limiter {
	l := &Limiter{ttl: visitorTTL}
	l.SetRate(rps, burst)
	return l
}

// SetRate updates the bucket parameters and clears existing buckets. It is safe
// to call concurrently with Allow.
func (l *Limiter) SetRate(rps float64, burst int) {
	if l == nil {
		return
	}
	if rps > 0 {
		if burst <= 0 {
			burst = int(rps)
		}
		if burst <= 0 {
			burst = 1
		}
	} else {
		rps = 0
		burst = 1
	}

	l.mu.Lock()
	l.limit = rate.Limit(rps)
	l.burst = burst
	l.visitors = make(map[string]*visitor)
	l.mu.Unlock()
}

// Allow reports whether the key may proceed.
func (l *Limiter) Allow(key string) bool {
	if l == nil {
		return true
	}

	l.mu.Lock()
	if l.limit <= 0 {
		l.mu.Unlock()
		return true
	}
	v, ok := l.visitors[key]
	if !ok {
		v = &visitor{limiter: rate.NewLimiter(l.limit, l.burst)}
		l.visitors[key] = v
	}
	v.lastSeen = time.Now()
	l.mu.Unlock()

	return v.limiter.Allow()
}

// Cleanup drops idle visitors until ctx is done. It is safe to call on a nil
// Limiter.
func (l *Limiter) Cleanup(ctx context.Context) {
	if l == nil {
		return
	}
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-l.ttl)
			l.mu.Lock()
			for key, v := range l.visitors {
				if v.lastSeen.Before(cutoff) {
					delete(l.visitors, key)
				}
			}
			l.mu.Unlock()
		}
	}
}

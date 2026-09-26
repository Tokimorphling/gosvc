// Package ratelimit implements a per-key token bucket limiter.
package ratelimit

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const visitorTTL = 10 * time.Minute

// Limiter keeps one token bucket per key (for example a client IP).
// A nil *Limiter allows everything, so callers can treat "disabled" as nil.
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

// New returns nil when rps <= 0 (rate limiting disabled).
func New(rps float64, burst int) *Limiter {
	if rps <= 0 {
		return nil
	}
	if burst <= 0 {
		burst = int(rps)
	}
	if burst <= 0 {
		burst = 1
	}
	return &Limiter{
		visitors: make(map[string]*visitor),
		limit:    rate.Limit(rps),
		burst:    burst,
		ttl:      visitorTTL,
	}
}

// Allow reports whether the key may proceed.
func (l *Limiter) Allow(key string) bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
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

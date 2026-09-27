// Package ratelimit implements a per-key token bucket limiter with runtime
// reconfiguration.
//
// Buckets live in shards selected by a hash of the key so concurrent requests
// from different clients contend on different locks instead of one global
// mutex.
package ratelimit

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

const (
	visitorTTL = 10 * time.Minute
	shardCount = 16 // power of two; must stay in sync with the shard mask below
	shardMask  = shardCount - 1
)

// Limiter keeps one token bucket per key (for example a client IP).
//
// A Limiter is always safe to use: when the configured rate is <= 0 it allows
// everything. SetRate swaps the parameters at runtime (config reload) and drops
// the existing buckets so the new rate applies immediately.
type Limiter struct {
	shards [shardCount]shard

	// limit and burst are accessed atomically so SetRate can run while
	// requests are in flight.
	limitBits atomic.Uint64 // float64 bits of rate.Limit
	burst     atomic.Int64

	ttl time.Duration
}

type shard struct {
	mu       sync.Mutex
	visitors map[string]*visitor
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

// shardIndex maps a key onto a shard using FNV-1a, without allocating.
func shardIndex(key string) int {
	const prime = 16777619
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= prime
	}
	return int(h & shardMask)
}

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

	l.limitBits.Store(math.Float64bits(float64(rate.Limit(rps))))
	l.burst.Store(int64(burst))

	for i := range l.shards {
		s := &l.shards[i]
		s.mu.Lock()
		s.visitors = make(map[string]*visitor)
		s.mu.Unlock()
	}
}

// rate returns the current limit and burst parameters.
func (l *Limiter) rate() (rate.Limit, int) {
	return rate.Limit(math.Float64frombits(l.limitBits.Load())), int(l.burst.Load())
}

// Allow reports whether the key may proceed.
func (l *Limiter) Allow(key string) bool {
	if l == nil {
		return true
	}
	limit, burst := l.rate()
	if limit <= 0 {
		return true
	}

	s := &l.shards[shardIndex(key)]
	s.mu.Lock()
	v, ok := s.visitors[key]
	if !ok {
		v = &visitor{limiter: rate.NewLimiter(limit, burst)}
		s.visitors[key] = v
	}
	v.lastSeen = time.Now()
	s.mu.Unlock()

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
			for i := range l.shards {
				s := &l.shards[i]
				s.mu.Lock()
				for key, v := range s.visitors {
					if v.lastSeen.Before(cutoff) {
						delete(s.visitors, key)
					}
				}
				s.mu.Unlock()
			}
		}
	}
}

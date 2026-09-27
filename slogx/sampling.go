package slogx

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// samplerShards must stay a power of two; counters are sharded so
	// concurrent records contend on different locks.
	samplerShards = 16
	samplerMask   = samplerShards - 1
)

// SamplingOptions configures a SamplingHandler.
type SamplingOptions struct {
	// Initial is how many records per (level, message) are always emitted in
	// each tick window.
	Initial int
	// Thereafter emits one out of every Thereafter records once Initial has
	// been exceeded within the window.
	Thereafter int
	// Tick is the sampling window; counters reset every tick.
	Tick time.Duration
	// ExemptLevel and above are never sampled (warn and error by default).
	ExemptLevel slog.Level
}

// SamplingStats is a snapshot of a SamplingHandler's counters.
type SamplingStats struct {
	Emitted    uint64            `json:"emitted"`
	Dropped    uint64            `json:"dropped"`
	ByLevel    map[string]uint64 `json:"droppedByLevel"`
	WindowFrom time.Time         `json:"windowStartedAt"`
}

type samplerShard struct {
	mu       sync.Mutex
	window   time.Time
	counters map[string]int
	dropped  map[string]uint64
}

// SamplingHandler reduces log volume under high QPS: within each tick window it
// emits the first Initial records for a given (level, message) pair and then
// only one out of every Thereafter records. Records at ExemptLevel or above are
// always emitted.
//
// It is the presentation-layer counterpart of the tracing sample ratio: keep
// levels cheap for repeated request logs while never losing warnings/errors.
// Counters are sharded by (level, message) hash to keep the hot path scalable.
type SamplingHandler struct {
	inner  slog.Handler
	opts   SamplingOptions
	shards [samplerShards]samplerShard

	emitted  atomic.Uint64
	droppedN atomic.Uint64
}

// NewSamplingHandler wraps inner with per-message sampling.
func NewSamplingHandler(inner slog.Handler, opts SamplingOptions) *SamplingHandler {
	if opts.Initial < 0 {
		opts.Initial = 0
	}
	if opts.Thereafter < 1 {
		opts.Thereafter = 1
	}
	if opts.Tick <= 0 {
		opts.Tick = time.Second
	}
	if opts.ExemptLevel == 0 {
		opts.ExemptLevel = slog.LevelWarn
	}
	h := &SamplingHandler{inner: inner, opts: opts}
	now := time.Now()
	for i := range h.shards {
		h.shards[i] = samplerShard{
			window:   now,
			counters: make(map[string]int),
			dropped:  make(map[string]uint64),
		}
	}
	return h
}

// Enabled implements slog.Handler.
func (h *SamplingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

// Handle implements slog.Handler.
func (h *SamplingHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level >= h.opts.ExemptLevel || h.shouldEmit(r) {
		h.emitted.Add(1)
		return h.inner.Handle(ctx, r)
	}
	return nil
}

// WithAttrs implements slog.Handler. Derived handlers share the sampler state
// so per-request loggers (request_id attributes) do not escape sampling.
func (h *SamplingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &samplingProxy{sampler: h, inner: h.inner.WithAttrs(attrs)}
}

// WithGroup implements slog.Handler.
func (h *SamplingHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &samplingProxy{sampler: h, inner: h.inner.WithGroup(name)}
}

// Stats returns a snapshot of the sampler counters.
func (h *SamplingHandler) Stats() SamplingStats {
	stats := SamplingStats{
		Emitted: h.emitted.Load(),
		Dropped: h.droppedN.Load(),
		ByLevel: make(map[string]uint64),
	}
	for i := range h.shards {
		s := &h.shards[i]
		s.mu.Lock()
		for level, count := range s.dropped {
			stats.ByLevel[level] += count
		}
		if s.window.Before(stats.WindowFrom) || stats.WindowFrom.IsZero() {
			stats.WindowFrom = s.window
		}
		s.mu.Unlock()
	}
	return stats
}

// shardFor picks the shard for a (level, message) pair without allocating.
func (h *SamplingHandler) shardFor(level slog.Level, message string) *samplerShard {
	const prime = 16777619
	hash := uint32(level) // seed with the level bits (wraps negative levels)
	for i := 0; i < len(message); i++ {
		hash ^= uint32(message[i])
		hash *= prime
	}
	return &h.shards[hash&samplerMask]
}

// shouldEmit counts the record and decides whether it survives sampling.
func (h *SamplingHandler) shouldEmit(r slog.Record) bool {
	level := r.Level.String()
	s := h.shardFor(r.Level, r.Message)

	s.mu.Lock()
	now := time.Now()
	if now.Sub(s.window) >= h.opts.Tick {
		s.window = now
		s.counters = make(map[string]int)
	}
	key := level + "|" + r.Message
	n := s.counters[key] + 1
	s.counters[key] = n
	emit := n <= h.opts.Initial || (n-h.opts.Initial)%h.opts.Thereafter == 0
	if !emit {
		s.dropped[level]++
	}
	s.mu.Unlock()

	if !emit {
		h.droppedN.Add(1)
	}
	return emit
}

// samplingProxy applies attrs/groups from a derived logger while keeping the
// shared sampler counters.
type samplingProxy struct {
	sampler *SamplingHandler
	inner   slog.Handler
}

func (p *samplingProxy) Enabled(ctx context.Context, level slog.Level) bool {
	return p.inner.Enabled(ctx, level)
}

func (p *samplingProxy) Handle(ctx context.Context, r slog.Record) error {
	if r.Level >= p.sampler.opts.ExemptLevel || p.sampler.shouldEmit(r) {
		p.sampler.emitted.Add(1)
		return p.inner.Handle(ctx, r)
	}
	return nil
}

func (p *samplingProxy) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &samplingProxy{sampler: p.sampler, inner: p.inner.WithAttrs(attrs)}
}

func (p *samplingProxy) WithGroup(name string) slog.Handler {
	if name == "" {
		return p
	}
	return &samplingProxy{sampler: p.sampler, inner: p.inner.WithGroup(name)}
}

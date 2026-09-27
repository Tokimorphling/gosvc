package slogx

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
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

// SamplingHandler reduces log volume under high QPS: within each tick window it
// emits the first Initial records for a given (level, message) pair and then
// only one out of every Thereafter records. Records at ExemptLevel or above are
// always emitted.
//
// It is the presentation-layer counterpart of the tracing sample ratio: keep
// levels cheap for repeated request logs while never losing warnings/errors.
type SamplingHandler struct {
	inner slog.Handler
	opts  SamplingOptions

	mu       sync.Mutex
	window   time.Time
	counters map[string]int
	dropped  map[string]uint64

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
	return &SamplingHandler{
		inner:    inner,
		opts:     opts,
		window:   time.Now(),
		counters: make(map[string]int),
		dropped:  make(map[string]uint64),
	}
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
	h.mu.Lock()
	byLevel := make(map[string]uint64, len(h.dropped))
	for level, count := range h.dropped {
		byLevel[level] = count
	}
	window := h.window
	h.mu.Unlock()

	return SamplingStats{
		Emitted:    h.emitted.Load(),
		Dropped:    h.droppedN.Load(),
		ByLevel:    byLevel,
		WindowFrom: window,
	}
}

// shouldEmit counts the record and decides whether it survives sampling.
func (h *SamplingHandler) shouldEmit(r slog.Record) bool {
	now := time.Now()
	key := r.Level.String() + "|" + r.Message

	h.mu.Lock()
	if now.Sub(h.window) >= h.opts.Tick {
		h.window = now
		h.counters = make(map[string]int)
	}
	n := h.counters[key] + 1
	h.counters[key] = n
	emit := n <= h.opts.Initial || (n-h.opts.Initial)%h.opts.Thereafter == 0
	if !emit {
		h.dropped[r.Level.String()]++
	}
	h.mu.Unlock()

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

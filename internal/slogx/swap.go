package slogx

import (
	"context"
	"log/slog"
	"sync"
)

// SwapHandler is an slog.Handler whose inner handler can be replaced at runtime,
// for example when the log format or sinks change on a configuration reload.
//
// The root logger keeps working across swaps. Handlers derived via WithAttrs or
// WithGroup are bound to the inner handler that was current at derivation time;
// that is fine for request-scoped loggers, whose lifetime is short.
type SwapHandler struct {
	mu    sync.RWMutex
	inner slog.Handler
}

// NewSwapHandler wraps the initial handler.
func NewSwapHandler(inner slog.Handler) *SwapHandler {
	return &SwapHandler{inner: inner}
}

// Swap replaces the inner handler. A nil handler is ignored.
func (h *SwapHandler) Swap(inner slog.Handler) {
	if h == nil || inner == nil {
		return
	}
	h.mu.Lock()
	h.inner = inner
	h.mu.Unlock()
}

// Current returns the active inner handler.
func (h *SwapHandler) Current() slog.Handler {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.inner
}

// Enabled implements slog.Handler.
func (h *SwapHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.Current().Enabled(ctx, level)
}

// Handle implements slog.Handler.
func (h *SwapHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.Current().Handle(ctx, r)
}

// WithAttrs implements slog.Handler.
func (h *SwapHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h.Current().WithAttrs(attrs)
}

// WithGroup implements slog.Handler.
func (h *SwapHandler) WithGroup(name string) slog.Handler {
	return h.Current().WithGroup(name)
}

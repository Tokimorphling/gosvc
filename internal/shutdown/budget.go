// Package shutdown shares one deadline between application hooks and transport
// drains. Standalone transports retain their configured shutdown timeout.
package shutdown

import (
	"context"
	"sync"
	"time"
)

type contextKey struct{}

// Budget is started once when shutdown is requested, before serving is stopped.
type Budget struct {
	mu       sync.RWMutex
	deadline time.Time
}

func WithBudget(ctx context.Context) (context.Context, *Budget) {
	b := &Budget{}
	return context.WithValue(ctx, contextKey{}, b), b
}

func (b *Budget) Start(timeout time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.deadline.IsZero() {
		b.deadline = time.Now().Add(timeout)
	}
}

// Context detaches cancellation from the serving context and applies the
// earlier of the shared shutdown deadline and the transport's own timeout.
func Context(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(timeout)
	if b, ok := ctx.Value(contextKey{}).(*Budget); ok {
		b.mu.RLock()
		if !b.deadline.IsZero() && b.deadline.Before(deadline) {
			deadline = b.deadline
		}
		b.mu.RUnlock()
	}
	return context.WithDeadline(context.WithoutCancel(ctx), deadline)
}

package redis

import (
	"context"
	"log/slog"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"example.com/gosvc/store"
)

const defaultFlushInterval = 5 * time.Second

// Recorder aggregates samples in memory and flushes them to Redis in batches.
// Incr never blocks: when the queue is full the sample is dropped.
type Recorder struct {
	store    *Store
	logger   *slog.Logger
	samples  chan sample
	interval time.Duration

	mu         sync.Mutex
	aggregated map[string]float64

	done chan struct{}
}

type sample struct {
	metric string
	delta  float64
}

// NewRecorder builds a recorder for the store. It returns nil when store is nil.
func NewRecorder(store *Store, queueSize int, logger *slog.Logger) *Recorder {
	if store == nil {
		return nil
	}
	if queueSize < 1 {
		queueSize = 1024
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Recorder{
		store:      store,
		logger:     logger,
		samples:    make(chan sample, queueSize),
		interval:   defaultFlushInterval,
		aggregated: make(map[string]float64),
		done:       make(chan struct{}),
	}
}

// Incr enqueues one sample without blocking.
func (r *Recorder) Incr(_ context.Context, metric string, delta float64) error {
	if r == nil {
		return nil
	}
	select {
	case r.samples <- sample{metric: metric, delta: delta}:
		return nil
	default:
		return store.ErrDropped
	}
}

// Run consumes samples and flushes aggregated counters until ctx is done, then
// performs a final flush. Call it in a goroutine.
func (r *Recorder) Run(ctx context.Context) {
	if r == nil {
		return
	}
	defer close(r.done)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Drain what is still queued so a shutdown does not lose samples.
			for {
				select {
				case s := <-r.samples:
					r.mu.Lock()
					r.aggregated[s.metric] += s.delta
					r.mu.Unlock()
				default:
					r.flush(context.Background())
					return
				}
			}
		case s := <-r.samples:
			r.mu.Lock()
			r.aggregated[s.metric] += s.delta
			r.mu.Unlock()
		case <-ticker.C:
			r.flush(ctx)
		}
	}
}

// Wait blocks until Run returned.
func (r *Recorder) Wait() {
	if r == nil {
		return
	}
	<-r.done
}

func (r *Recorder) flush(ctx context.Context) {
	r.mu.Lock()
	if len(r.aggregated) == 0 {
		r.mu.Unlock()
		return
	}
	batch := r.aggregated
	r.aggregated = make(map[string]float64)
	r.mu.Unlock()

	now := time.Now()
	pipe := r.store.client.Pipeline()
	for metric, delta := range batch {
		key := r.store.key(metric, now)
		pipe.IncrByFloat(ctx, key, delta)
		pipe.Expire(ctx, key, r.store.ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		r.logger.Warn("failed to flush time-series samples", "error", err, "metrics", len(batch))
	}
}

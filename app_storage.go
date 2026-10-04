// Storage lifecycle: the optional Redis and PostgreSQL connections, their
// generations across hot reloads, and the lease pattern that keeps a
// generation's connections open until every in-flight callback has returned.

package gosvc

import (
	"context"
	"errors"
	"time"

	"github.com/Tokimorphling/gosvc/config"
	"github.com/Tokimorphling/gosvc/internal/admin"
	"github.com/Tokimorphling/gosvc/store"
	"github.com/Tokimorphling/gosvc/store/postgres"
	"github.com/Tokimorphling/gosvc/store/redis"
)

// storageState holds the optional storage connections plus the recorder
// goroutine that belongs to them.
type storageState struct {
	store    *redis.Store
	recorder *redis.Recorder
	postgres *postgres.DB
	cancel   context.CancelFunc
	// leases and idle are protected by App.storageMu. A retired generation
	// stays open until every callback that acquired it has returned.
	leases int
	idle   chan struct{}
}

// retireStorage is called with storageMu held. A non-nil channel signals
// when callbacks using the old generation have all returned.
func retireStorage(state *storageState) <-chan struct{} {
	if state == nil || state.leases == 0 {
		return nil
	}
	state.idle = make(chan struct{})
	return state.idle
}

// startStorage opens the configured storage connections. On failure nothing is
// left behind.
func (a *App) startStorage(ctx context.Context, cfg config.StorageConfig) (*storageState, error) {
	state := &storageState{}

	if cfg.Redis.Enabled {
		redisStore, err := redis.New(ctx, cfg.Redis)
		if err != nil {
			return nil, err
		}
		state.store = redisStore

		recorderCtx, cancel := context.WithCancel(ctx)
		state.cancel = cancel
		state.recorder = redis.NewRecorder(redisStore, cfg.Redis.QueueSize, a.log.Logger())
		go state.recorder.Run(recorderCtx)
	}

	if cfg.Postgres.Enabled {
		db, err := postgres.New(ctx, cfg.Postgres)
		if err != nil {
			a.stopStorage(state)
			return nil, err
		}
		state.postgres = db
	}

	return state, nil
}

// stopStorage flushes the recorder and then closes the connections.
func (a *App) stopStorage(state *storageState) {
	if state == nil {
		return
	}
	if state.recorder != nil {
		state.cancel()
		state.recorder.Wait()
	}
	if state.store != nil {
		_ = state.store.Close()
	}
	if state.postgres != nil {
		_ = state.postgres.Close()
	}
}

// reloadStorage builds the new storage connections, swaps them in and then tears
// down the previous ones. A failure keeps the current state.
func (a *App) reloadStorage(cfg config.StorageConfig) error {
	next, err := a.startStorage(context.Background(), cfg)
	if err != nil {
		return err
	}

	a.storageMu.Lock()
	previous := a.storage
	// Holder.Set waits for in-flight recordings before old recorder shutdown.
	a.recorders.Set(store.Nilable(next.recorder))
	a.storage = next
	idle := retireStorage(previous)
	a.storageMu.Unlock()

	if idle != nil {
		<-idle
	}
	a.stopStorage(previous)
	return nil
}

// Store returns a snapshot of the active Redis store, or nil when disabled.
// A reload may close it immediately after return. Use WithStore for operations
// that must remain safe while storage is reloaded.
func (a *App) Store() *redis.Store {
	if current := a.currentStorage(); current != nil {
		return current.store
	}
	return nil
}

// Postgres returns a snapshot of the active pool, or nil when disabled. A
// reload may close it immediately after return. Use WithPostgres for operations
// that must remain safe while storage is reloaded.
func (a *App) Postgres() *postgres.DB {
	if current := a.currentStorage(); current != nil {
		return current.postgres
	}
	return nil
}

// WithStore leases the active Redis store for fn. A reload waits for that
// generation's callbacks to finish before closing it. Nested WithStore and
// WithPostgres calls are safe. The callback must not call Reload or Close,
// since these operations wait for its lease to finish. Disabled storage
// returns ErrStorageDisabled without invoking fn.
func (a *App) WithStore(fn func(*redis.Store) error) error {
	return leaseStorage(a, func(s *storageState) *redis.Store { return s.store }, fn)
}

// WithPostgres leases the active PostgreSQL pool for fn. A reload waits for
// that generation's callbacks to finish before closing it. Nested storage
// leases are safe; callbacks must not call Reload or Close.
func (a *App) WithPostgres(fn func(*postgres.DB) error) error {
	return leaseStorage(a, func(s *storageState) *postgres.DB { return s.postgres }, fn)
}

// leaseStorage hands fn the connection picked from the active storage state,
// holding that generation's lease for the duration of the callback so a reload
// cannot close the connection mid-flight. pick must return a pointer type, so
// its zero value identifies "not enabled".
func leaseStorage[T any](a *App, pick func(*storageState) *T, fn func(*T) error) error {
	if fn == nil {
		return errors.New("gosvc: nil storage callback")
	}
	a.storageMu.Lock()
	state := a.storage
	var conn *T
	if state != nil {
		conn = pick(state)
	}
	if conn == nil {
		a.storageMu.Unlock()
		return ErrStorageDisabled
	}
	state.leases++
	a.storageMu.Unlock()
	defer a.releaseStorage(state)
	return fn(conn)
}

func (a *App) releaseStorage(state *storageState) {
	a.storageMu.Lock()
	state.leases--
	if state.leases == 0 && state.idle != nil {
		close(state.idle)
		state.idle = nil
	}
	a.storageMu.Unlock()
}

func (a *App) queryTimeSeries(ctx context.Context, metric string, from, to time.Time) ([]store.Bucket, error) {
	var buckets []store.Bucket
	err := a.WithStore(func(redisStore *redis.Store) error {
		var queryErr error
		buckets, queryErr = redisStore.Range(ctx, metric, from, to)
		return queryErr
	})
	if errors.Is(err, ErrStorageDisabled) {
		return nil, admin.ErrTimeSeriesDisabled
	}
	return buckets, err
}

func (a *App) currentStorage() *storageState {
	a.storageMu.RLock()
	defer a.storageMu.RUnlock()
	return a.storage
}

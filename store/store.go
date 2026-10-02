// Package store defines optional persistence used by the transports.
package store

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"
)

// ErrDropped is returned when a recorder drops a sample because its queue is
// saturated.
var ErrDropped = errors.New("recorder queue is full, sample dropped")

// Recorder records time-series samples. Implementations must be safe for
// concurrent use. A nil Recorder disables recording.
type Recorder interface {
	Incr(ctx context.Context, metric string, delta float64) error
}

// TimeSeries reads aggregated buckets.
type TimeSeries interface {
	Range(ctx context.Context, metric string, from, to time.Time) ([]Bucket, error)
}

// Bucket is one aggregated sample.
type Bucket struct {
	Time  time.Time `json:"time"`
	Value float64   `json:"value"`
}

// Nilable converts a concrete recorder that may be a nil pointer into a
// Recorder whose interface value is nil. It avoids the typed-nil trap:
//
//	var recorder *redis.Recorder // nil
//	holder.Set(recorder)         // stores a non-nil interface holding a nil pointer
//	holder.Set(store.Nilable(recorder)) // stores a real nil
func Nilable[T Recorder](r T) Recorder {
	value := reflect.ValueOf(r)
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Func, reflect.Slice, reflect.Chan:
		if value.IsNil() {
			return nil
		}
	}
	return r
}

// Holder is a stable Recorder whose implementation can be swapped at runtime,
// for example when the storage connection is rebuilt on a configuration reload.
// The zero value records nothing.
type Holder struct {
	mu       sync.RWMutex
	recorder Recorder
}

// NewHolder wraps recorder, which may be nil.
func NewHolder(recorder Recorder) *Holder { return &Holder{recorder: recorder} }

// Set replaces the underlying recorder. Passing nil disables recording.
func (h *Holder) Set(recorder Recorder) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.recorder = recorder
	h.mu.Unlock()
}

// Current returns the underlying recorder, which may be nil.
func (h *Holder) Current() Recorder {
	if h == nil {
		return nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.recorder
}

// Incr implements Recorder and is safe to call concurrently with Set.
func (h *Holder) Incr(ctx context.Context, metric string, delta float64) error {
	if h == nil {
		return nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	recorder := h.recorder
	if recorder == nil {
		return nil
	}
	return recorder.Incr(ctx, metric, delta)
}

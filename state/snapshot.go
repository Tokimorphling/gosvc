// Package state provides small concurrency helpers for read-mostly state.
//
// The pattern it implements is the one that keeps hot paths lock-free: publish
// an immutable value, read it atomically, replace it on write.
package state

import "sync/atomic"

// Snapshot holds a value that can be replaced atomically. Load never blocks;
// Store publishes a new value. The zero value is ready to use and Load returns
// the zero value of T until something is stored.
type Snapshot[T any] struct {
	value atomic.Pointer[T]
}

// NewSnapshot returns a Snapshot holding initial.
func NewSnapshot[T any](initial T) *Snapshot[T] {
	s := &Snapshot[T]{}
	s.Store(initial)
	return s
}

// Load returns the current value.
func (s *Snapshot[T]) Load() T {
	if p := s.value.Load(); p != nil {
		return *p
	}
	var zero T
	return zero
}

// Store replaces the current value.
func (s *Snapshot[T]) Store(v T) { s.value.Store(&v) }

// Update applies fn to the current value and stores the result, retrying until
// the compare-and-swap succeeds. fn may run more than once, so it must be pure.
func (s *Snapshot[T]) Update(fn func(current T) T) T {
	for {
		old := s.value.Load()
		var current T
		if old != nil {
			current = *old
		}
		next := fn(current)
		if s.value.CompareAndSwap(old, &next) {
			return next
		}
	}
}

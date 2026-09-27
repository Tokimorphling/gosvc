package state

import (
	"sync"
	"testing"
)

func TestSnapshotLoadStore(t *testing.T) {
	var s Snapshot[string]

	if got := s.Load(); got != "" {
		t.Fatalf("zero value Load = %q, want empty", got)
	}

	s.Store("a")
	if got := s.Load(); got != "a" {
		t.Fatalf("Load = %q, want a", got)
	}

	s.Store("b")
	if got := s.Load(); got != "b" {
		t.Fatalf("Load = %q, want b", got)
	}
}

func TestSnapshotUpdate(t *testing.T) {
	s := NewSnapshot(1)

	got := s.Update(func(current int) int { return current + 1 })
	if got != 2 || s.Load() != 2 {
		t.Fatalf("Update = %d, Load = %d, want 2", got, s.Load())
	}
}

func TestSnapshotConcurrentUpdates(t *testing.T) {
	s := NewSnapshot(0)

	const goroutines, perGoroutine = 8, 200
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Go(func() {
			for j := 0; j < perGoroutine; j++ {
				s.Update(func(current int) int { return current + 1 })
			}
		})
	}
	wg.Wait()

	if got, want := s.Load(), goroutines*perGoroutine; got != want {
		t.Fatalf("Load = %d, want %d", got, want)
	}
}

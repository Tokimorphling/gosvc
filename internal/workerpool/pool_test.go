package workerpool

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolRunsTasks(t *testing.T) {
	pool := New(2, 32)
	defer pool.Stop()

	var done atomic.Int64
	for i := 0; i < 10; i++ {
		if err := pool.Submit(func() { done.Add(1) }); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for done.Load() != 10 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := done.Load(); got != 10 {
		t.Fatalf("executed %d tasks, want 10", got)
	}
}

func TestPoolBackpressureAndClose(t *testing.T) {
	pool := New(1, 1)
	block := make(chan struct{})
	started := make(chan struct{})

	if err := pool.Submit(func() { close(started); <-block }); err != nil {
		t.Fatalf("first Submit: %v", err)
	}
	<-started // the worker is now busy, the queue is empty

	if err := pool.Submit(func() {}); err != nil {
		t.Fatalf("second Submit: %v", err)
	}
	if err := pool.Submit(func() {}); !errors.Is(err, ErrFull) {
		t.Fatalf("third Submit = %v, want ErrFull", err)
	}

	close(block)
	pool.Stop()

	if err := pool.Submit(func() {}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Submit after Stop = %v, want ErrClosed", err)
	}
}

func TestPoolRecoversPanics(t *testing.T) {
	pool := New(1, 1)
	defer pool.Stop()

	panicked := make(chan any, 1)
	pool.SetPanicHandler(func(r any) { panicked <- r })

	if err := pool.Submit(func() { panic("boom") }); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	select {
	case r := <-panicked:
		if r != "boom" {
			t.Fatalf("recovered = %v", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("panic handler was not called")
	}
}

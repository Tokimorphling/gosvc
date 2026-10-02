package workerpool

import (
	"errors"
	"sync"
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

func TestSerialTasksDoNotOccupyWorkers(t *testing.T) {
	pool := New(2, 8)
	defer pool.Stop()

	key := new(int)
	started := make(chan struct{})
	release := make(chan struct{})
	releaseSlow := sync.OnceFunc(func() { close(release) })
	defer releaseSlow()
	if err := pool.SubmitSerial(key, func() { close(started); <-release }); err != nil {
		t.Fatal(err)
	}
	<-started

	secondRan := make(chan struct{})
	if err := pool.SubmitSerial(key, func() { close(secondRan) }); err != nil {
		t.Fatal(err)
	}
	otherRan := make(chan struct{})
	if err := pool.SubmitSerial(new(int), func() { close(otherRan) }); err != nil {
		t.Fatal(err)
	}

	select {
	case <-otherRan:
	case <-time.After(time.Second):
		t.Fatal("another key waited for the first key's serial task")
	}
	select {
	case <-secondRan:
		t.Fatal("serial successor ran before its predecessor completed")
	default:
	}
	releaseSlow()
	select {
	case <-secondRan:
	case <-time.After(time.Second):
		t.Fatal("serial successor did not run after predecessor")
	}
}

func TestSerialBackpressureAndStopDrains(t *testing.T) {
	pool := New(2, 1)
	key := new(int)
	started := make(chan struct{})
	release := make(chan struct{})
	releaseSlow := sync.OnceFunc(func() { close(release) })
	defer releaseSlow()
	if err := pool.SubmitSerial(key, func() { close(started); <-release }); err != nil {
		t.Fatal(err)
	}
	<-started
	finished := make(chan struct{})
	if err := pool.SubmitSerial(key, func() { close(finished) }); err != nil {
		t.Fatal(err)
	}
	if err := pool.SubmitSerial(key, func() {}); !errors.Is(err, ErrFull) {
		t.Fatalf("SubmitSerial beyond capacity = %v, want ErrFull", err)
	}
	releaseSlow()
	pool.Stop()
	select {
	case <-finished:
	default:
		t.Fatal("Stop returned before draining the serial successor")
	}
	if err := pool.SubmitSerial(key, func() {}); !errors.Is(err, ErrClosed) {
		t.Fatalf("SubmitSerial after Stop = %v, want ErrClosed", err)
	}
}

func TestConcurrentStopWaitsForDrain(t *testing.T) {
	pool := New(1, 1)
	defer pool.Stop()
	started := make(chan struct{})
	release := make(chan struct{})
	releaseTask := sync.OnceFunc(func() { close(release) })
	defer releaseTask()
	if err := pool.Submit(func() { close(started); <-release }); err != nil {
		t.Fatal(err)
	}
	<-started

	firstDone := make(chan struct{})
	go func() { pool.Stop(); close(firstDone) }()
	deadline := time.Now().Add(time.Second)
	for {
		pool.mu.Lock()
		closed := pool.closed
		pool.mu.Unlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first Stop did not close submissions")
		}
		time.Sleep(time.Millisecond)
	}

	secondDone := make(chan struct{})
	go func() { pool.Stop(); close(secondDone) }()
	select {
	case <-secondDone:
		t.Fatal("second Stop returned before the worker drained")
	case <-time.After(30 * time.Millisecond):
	}
	releaseTask()
	for _, done := range []chan struct{}{firstDone, secondDone} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Stop did not return after the worker drained")
		}
	}
}

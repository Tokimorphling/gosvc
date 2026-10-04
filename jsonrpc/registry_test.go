package jsonrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMiddlewareConstructorCanInspectAndRegister(t *testing.T) {
	d := NewDispatcher()
	base := func(context.Context, json.RawMessage) (any, error) { return "ok", nil }
	d.Register("echo", base)
	d.Use(func(next HandlerFunc) HandlerFunc {
		if len(d.Methods()) != 1 {
			t.Error("missing registration")
		}
		if err := d.TryRegister("added", base); err != nil {
			t.Error(err)
		}
		return next
	})
	done := make(chan error, 1)
	go func() { _, err := d.Invoke(t.Context(), "echo", nil); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("middleware constructor ran under the registry lock")
	}
}

func TestRegistryUpdatePreservesInflightSnapshot(t *testing.T) {
	d := NewDispatcher()
	entered, release := make(chan struct{}), make(chan struct{})
	releaseCall := sync.OnceFunc(func() { close(release) })
	defer releaseCall()
	var oldObserved, newObserved atomic.Int32
	d.SetObserver(func(string, int, time.Duration) { oldObserved.Add(1) })
	d.Register("work", func(context.Context, json.RawMessage) (any, error) { close(entered); <-release; return "old", nil })
	done := make(chan error, 1)
	go func() { _, err := d.Invoke(t.Context(), "work", nil); done <- err }()
	<-entered
	denied := errors.New("denied")
	d.UseFor("work", func(HandlerFunc) HandlerFunc {
		return func(context.Context, json.RawMessage) (any, error) { return nil, denied }
	})
	d.SetObserver(func(string, int, time.Duration) { newObserved.Add(1) })
	if _, err := d.Invoke(t.Context(), "work", nil); !errors.Is(err, denied) {
		t.Fatalf("new call = %v", err)
	}
	releaseCall()
	if err := <-done; err != nil {
		t.Fatalf("in-flight call = %v", err)
	}
	if oldObserved.Load() != 1 || newObserved.Load() != 1 {
		t.Fatalf("observers=%d/%d", oldObserved.Load(), newObserved.Load())
	}
}

func TestUnrelatedRegistrationDoesNotRebuildMiddleware(t *testing.T) {
	var builds atomic.Int32
	d := NewDispatcher()
	d.Use(func(next HandlerFunc) HandlerFunc { builds.Add(1); return next })
	base := func(context.Context, json.RawMessage) (any, error) { return nil, nil }
	d.Register("one", base)
	d.Register("two", base)
	invoke := func(name string) {
		t.Helper()
		if _, err := d.Invoke(t.Context(), name, nil); err != nil {
			t.Fatal(err)
		}
	}
	invoke("one")
	invoke("two")
	d.Register("three", base)
	invoke("one")
	invoke("two")
	if builds.Load() != 2 {
		t.Fatalf("unrelated method rebuilt chains: %d", builds.Load())
	}
	d.UseFor("one", func(next HandlerFunc) HandlerFunc { return next })
	invoke("one")
	invoke("two")
	if builds.Load() != 3 {
		t.Fatalf("per-method update rebuilt other chains: %d", builds.Load())
	}
}

func TestConcurrentRegistryReadsAndWrites(t *testing.T) {
	var d Dispatcher // zero value and new registrations use the same path
	d.Register("hot", func(context.Context, json.RawMessage) (any, error) { return 1, nil })
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 1000 {
				if got, err := d.Invoke(t.Context(), "hot", nil); err != nil || got != 1 {
					t.Errorf("Invoke=%v, %v", got, err)
				}
			}
		})
	}
	for i := range 100 {
		d.Register(fmt.Sprint(i), func(context.Context, json.RawMessage) (any, error) { return nil, nil })
		d.UseFor("hot", func(next HandlerFunc) HandlerFunc { return next })
		d.SetObserver(func(string, int, time.Duration) {})
		d.SetMaxBatch(i + 1)
	}
	wg.Wait()
}

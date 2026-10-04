package health

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestReadyFlag(t *testing.T) {
	var ready Ready

	if ready.IsReady() {
		t.Fatal("zero value must be not ready")
	}
	ready.Set(true)
	if !ready.IsReady() {
		t.Fatal("Set(true) must report ready")
	}
}

func TestCheckAggregatesDependencies(t *testing.T) {
	ready := &Ready{}
	ready.Set(true)
	ready.AddCheck("ok", func(context.Context) error { return nil })
	ready.AddCheck("broken", func(context.Context) error { return errors.New("connection refused") })

	isReady, details := ready.Check(context.Background())
	if isReady {
		t.Fatal("a failing check must make the service not ready")
	}
	if details["ok"] != "ok" || details["broken"] != "connection refused" {
		t.Fatalf("details = %+v", details)
	}
}

func TestCheckWithoutChecks(t *testing.T) {
	ready := &Ready{}
	ready.Set(true)

	isReady, details := ready.Check(context.Background())
	if !isReady || details != nil {
		t.Fatalf("isReady = %v, details = %+v", isReady, details)
	}
}

func TestShutdownDuringDependencyCheckCannotReportReady(t *testing.T) {
	ready := &Ready{}
	ready.Set(true)
	ready.AddCheck("dependency", func(ctx context.Context) error {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("probe has no deadline")
		}
		ready.Set(false)
		return nil
	})
	if ok, _ := ready.Check(t.Context()); ok {
		t.Fatal("dependency result overrode shutdown readiness")
	}
}

func TestHealthyAndDiagnosticsAgree(t *testing.T) {
	r := &Ready{}
	r.Set(true)
	for _, result := range []error{nil, ErrSkipped, errors.New("offline")} {
		r.AddCheck("db", func(context.Context) error { return result })
		ok, details := r.Check(t.Context())
		if r.Healthy(t.Context()) != ok {
			t.Fatal("status and diagnostic paths disagree")
		}
		details["db"] = "caller mutation"
		_, again := r.Check(t.Context())
		if again["db"] == "caller mutation" {
			t.Fatal("diagnostics expose shared state")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if r.Healthy(ctx) {
		t.Fatal("cancelled probe reported healthy")
	}
}

func TestProbeCanRegisterAndConcurrentChecksKeepSnapshots(t *testing.T) {
	r := &Ready{}
	r.Set(true)
	r.AddCheck("first", func(context.Context) error {
		r.AddCheck("second", func(context.Context) error { return nil })
		return nil
	})
	if ok, details := r.Check(t.Context()); !ok || len(details) != 1 {
		t.Fatalf("current snapshot changed: %v", details)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				if !r.Healthy(t.Context()) {
					t.Error("healthy snapshot failed")
				}
			}
		})
	}
	wg.Wait()
	if ok, details := r.Check(t.Context()); !ok || len(details) != 2 {
		t.Fatalf("new probe missing: %v", details)
	}
}

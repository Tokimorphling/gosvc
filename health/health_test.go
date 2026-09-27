package health

import (
	"context"
	"errors"
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

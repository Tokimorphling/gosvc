package ratelimit

import (
	"context"
	"testing"
)

func TestDisabledAllowsEverything(t *testing.T) {
	limiter := New(0, 0)
	for range 100 {
		if !limiter.Allow("client") {
			t.Fatal("disabled limiter must allow every request")
		}
	}
}

func TestEnabledLimits(t *testing.T) {
	limiter := New(1, 1)

	if !limiter.Allow("client") {
		t.Fatal("first request must pass")
	}
	if limiter.Allow("client") {
		t.Fatal("second immediate request must be rejected")
	}
	// Other keys have their own bucket.
	if !limiter.Allow("other") {
		t.Fatal("a different key must have its own bucket")
	}
}

func TestSetRateTogglesAtRuntime(t *testing.T) {
	limiter := New(0, 0)
	if !limiter.Allow("client") {
		t.Fatal("disabled limiter must allow")
	}

	limiter.SetRate(1, 1)
	if !limiter.Allow("client") {
		t.Fatal("first request after enabling must pass")
	}
	if limiter.Allow("client") {
		t.Fatal("second request after enabling must be rejected")
	}

	limiter.SetRate(0, 0)
	if !limiter.Allow("client") {
		t.Fatal("disabling must allow again")
	}
}

func TestCleanupStopsWithContext(t *testing.T) {
	limiter := New(10, 10)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		limiter.Cleanup(ctx)
		close(done)
	}()

	cancel()
	<-done
}

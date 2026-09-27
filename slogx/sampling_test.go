package slogx

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func newSamplingLogger(buf *bytes.Buffer, opts SamplingOptions) (*slog.Logger, *SamplingHandler) {
	inner := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	sampler := NewSamplingHandler(inner, opts)
	return slog.New(sampler), sampler
}

func TestSamplingHandlerInitialAndThereafter(t *testing.T) {
	var buf bytes.Buffer
	logger, sampler := newSamplingLogger(&buf, SamplingOptions{
		Initial:    2,
		Thereafter: 3,
		Tick:       time.Hour,
	})

	// n=1,2 are emitted by Initial; n=5 and n=8 satisfy (n-2)%3 == 0.
	for i := 0; i < 10; i++ {
		logger.Info("repeated")
	}

	stats := sampler.Stats()
	if stats.Emitted != 4 {
		t.Fatalf("emitted = %d, want 4", stats.Emitted)
	}
	if stats.Dropped != 6 {
		t.Fatalf("dropped = %d, want 6", stats.Dropped)
	}
	if stats.ByLevel["INFO"] != 6 {
		t.Fatalf("dropped by level = %+v", stats.ByLevel)
	}
}

func TestSamplingHandlerExemptsWarnings(t *testing.T) {
	var buf bytes.Buffer
	logger, sampler := newSamplingLogger(&buf, SamplingOptions{
		Initial:    1,
		Thereafter: 1000,
		Tick:       time.Hour,
	})

	for i := 0; i < 5; i++ {
		logger.Warn("repeated warning")
	}

	stats := sampler.Stats()
	if stats.Dropped != 0 {
		t.Fatalf("warnings must never be sampled, dropped = %d", stats.Dropped)
	}
	if strings.Count(buf.String(), "repeated warning") != 5 {
		t.Fatalf("expected 5 warnings in the output, got %q", buf.String())
	}
}

func TestSamplingHandlerDerivedLoggersShareCounters(t *testing.T) {
	var buf bytes.Buffer
	logger, sampler := newSamplingLogger(&buf, SamplingOptions{
		Initial:    1,
		Thereafter: 1000,
		Tick:       time.Hour,
	})

	logger.Info("shared")                // emitted (n=1)
	logger.With("k", "v").Info("shared") // dropped (n=2)
	logger.WithGroup("g").Info("shared") // dropped (n=3)

	stats := sampler.Stats()
	if stats.Emitted != 1 || stats.Dropped != 2 {
		t.Fatalf("stats = %+v, want emitted=1 dropped=2", stats)
	}
}

func TestSamplingHandlerWindowReset(t *testing.T) {
	// synctest runs the test in a bubble with a virtual clock, so the window
	// boundary is exercised deterministically instead of with a real sleep.
	synctest.Test(t, func(t *testing.T) {
		var buf bytes.Buffer
		logger, sampler := newSamplingLogger(&buf, SamplingOptions{
			Initial:    1,
			Thereafter: 1000,
			Tick:       time.Second,
		})

		logger.Info("x") // emitted
		logger.Info("x") // dropped
		time.Sleep(2 * time.Second)
		logger.Info("x") // new window, emitted again

		stats := sampler.Stats()
		if stats.Emitted != 2 || stats.Dropped != 1 {
			t.Fatalf("stats = %+v, want emitted=2 dropped=1", stats)
		}
	})
}

package redis

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"example.com/gosvc/config"
)

func newTestStore(t *testing.T) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)

	store, err := New(context.Background(), config.RedisConfig{
		Enabled:   true,
		Addr:      mr.Addr(),
		Prefix:    "test",
		BucketTTL: config.Duration(time.Hour),
		QueueSize: 64,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, mr
}

func TestStoreIncrAndRange(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := store.Incr(ctx, "http.requests", 1); err != nil {
			t.Fatalf("Incr: %v", err)
		}
	}
	if err := store.Incr(ctx, "http.requests", 2.5); err != nil {
		t.Fatalf("Incr: %v", err)
	}

	buckets, err := store.Range(ctx, "http.requests", time.Now().Add(-time.Hour), time.Now())
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(buckets) != 1 {
		t.Fatalf("got %d buckets, want 1", len(buckets))
	}
	if buckets[0].Value != 5.5 {
		t.Fatalf("value = %v, want 5.5", buckets[0].Value)
	}
}

func TestUniversalOptionsModes(t *testing.T) {
	single := universalOptions(config.RedisConfig{Addr: "127.0.0.1:6379"})
	if len(single.Addrs) != 1 || single.Addrs[0] != "127.0.0.1:6379" || single.MasterName != "" {
		t.Fatalf("single options = %+v", single)
	}

	cluster := universalOptions(config.RedisConfig{Mode: "cluster", Addrs: []string{"a:6379", "b:6379"}})
	if len(cluster.Addrs) != 2 || cluster.MasterName != "" {
		t.Fatalf("cluster options = %+v", cluster)
	}

	sentinel := universalOptions(config.RedisConfig{
		Mode:       "sentinel",
		Addrs:      []string{"s1:26379", "s2:26379"},
		MasterName: "mymaster",
	})
	if len(sentinel.Addrs) != 2 || sentinel.MasterName != "mymaster" {
		t.Fatalf("sentinel options = %+v", sentinel)
	}
}

func TestRecorderAggregatesAndFlushes(t *testing.T) {
	store, _ := newTestStore(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	recorder := NewRecorder(store, 64, logger)

	ctx, cancel := context.WithCancel(context.Background())
	go recorder.Run(ctx)

	for i := 0; i < 7; i++ {
		if err := recorder.Incr(ctx, "grpc.requests", 1); err != nil {
			t.Fatalf("Incr: %v", err)
		}
	}

	cancel()
	recorder.Wait()

	buckets, err := store.Range(context.Background(), "grpc.requests", time.Now().Add(-time.Hour), time.Now())
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	total := 0.0
	for _, bucket := range buckets {
		total += bucket.Value
	}
	if total != 7 {
		t.Fatalf("total = %v, want 7", total)
	}
}

func TestRecorderDropsWhenFull(t *testing.T) {
	store, _ := newTestStore(t)
	recorder := NewRecorder(store, 1, nil)

	if err := recorder.Incr(context.Background(), "m", 1); err != nil {
		t.Fatalf("first Incr: %v", err)
	}
	if err := recorder.Incr(context.Background(), "m", 1); err == nil {
		t.Fatal("expected a drop error when the queue is full")
	}
}

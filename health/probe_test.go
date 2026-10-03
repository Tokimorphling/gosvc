package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestProbeAccepts200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := Probe(ctx, server.URL); err != nil {
		t.Fatalf("Probe: %v", err)
	}
}

func TestProbeRejectsNon200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := Probe(ctx, server.URL); err == nil {
		t.Fatal("Probe must report a non-200 answer as an error")
	}
}

func TestProbeReportsUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := Probe(ctx, "http://127.0.0.1:1/readyz"); err == nil {
		t.Fatal("Probe must report an unreachable endpoint as an error")
	}
}

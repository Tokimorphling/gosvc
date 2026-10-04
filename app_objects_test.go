package gosvc

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tokimorphling/gosvc/config"
	"github.com/Tokimorphling/gosvc/store/object"
)

func objectEndpoint(t *testing.T, body string, fail *atomic.Bool) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail != nil && fail.Load() {
			w.WriteHeader(403)
			return
		}
		if r.Method == "HEAD" && r.URL.Path == "/bucket" {
			return
		}
		w.Header().Set("Content-Length", "3")
		if r.Method == "GET" {
			_, _ = io.WriteString(w, body)
		}
	}))
	t.Cleanup(s.Close)
	return s
}

func objectConfig(endpoint string) config.S3Config {
	c := config.Default().Storage.S3
	c.Enabled, c.Bucket, c.Endpoint, c.UsePathStyle = true, "bucket", endpoint, true
	c.AccessKeyID, c.SecretAccessKey = "test-key", "test-secret"
	c.MaxAttempts = 1
	c.RequestTimeout = config.Duration(2 * time.Second)
	return c
}

func TestObjectStoreFollowsReloadAndKeepsDownloadLease(t *testing.T) {
	first, second := objectEndpoint(t, "old", nil), objectEndpoint(t, "new", nil)
	cfg := testConfig()
	cfg.Storage.S3 = objectConfig(first.URL)
	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	stable := a.Objects()
	oldClient := a.currentStorage().s3
	download, err := stable.Get(t.Context(), "file", object.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer download.Body.Close()
	next := a.Config().Storage
	next.S3.Endpoint = second.URL
	done := make(chan error, 1)
	go func() { done <- a.reloadStorage(next) }()
	deadline := time.After(time.Second)
	for a.currentStorage().s3 == oldClient {
		select {
		case <-deadline:
			t.Fatal("storage did not switch")
		case <-time.After(time.Millisecond):
		}
	}
	select {
	case err := <-done:
		t.Fatalf("retired a leased download: %v", err)
	default:
	}
	if err := oldClient.Ping(t.Context()); err != nil {
		t.Fatalf("old client closed before body: %v", err)
	}
	fresh, err := stable.Get(t.Context(), "file", object.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(fresh.Body)
	_ = fresh.Body.Close()
	if err != nil || string(data) != "new" {
		t.Fatalf("stable facade=%q %v", data, err)
	}
	data, err = io.ReadAll(download.Body)
	_ = download.Body.Close()
	if err != nil || string(data) != "old" {
		t.Fatalf("old stream=%q %v", data, err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("completed stream kept old lease")
	}
	if err := oldClient.Ping(t.Context()); !errors.Is(err, object.ErrClosed) {
		t.Fatalf("old client not closed: %v", err)
	}
	if err := a.reloadStorage(config.Default().Storage); err != nil {
		t.Fatal(err)
	}
	if _, err := stable.Head(t.Context(), "file"); !errors.Is(err, object.ErrDisabled) {
		t.Fatalf("disabled facade=%v", err)
	}
}

func TestS3ReadinessAndFailedReloadRetainEffectiveConfig(t *testing.T) {
	var fail atomic.Bool
	first := objectEndpoint(t, "old", &fail)
	var alwaysFail atomic.Bool
	alwaysFail.Store(true)
	bad := objectEndpoint(t, "bad", &alwaysFail)
	cfg := testConfig()
	cfg.Storage.S3 = objectConfig(first.URL)
	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	a.Health().Set(true)
	if !a.Health().Healthy(t.Context()) {
		t.Fatal("healthy S3 not ready")
	}
	fail.Store(true)
	if ready, checks := a.Health().Check(t.Context()); ready || checks["s3"] == "ok" {
		t.Fatalf("S3 failure ignored: %v", checks)
	}
	fail.Store(false)
	next := a.Config()
	next.Storage.S3.Endpoint = bad.URL
	if err := a.applyConfig(next); err == nil {
		t.Fatal("failed bucket check accepted")
	}
	if a.Config().Storage.S3.Endpoint != first.URL {
		t.Fatal("failed reload replaced effective config")
	}
	if _, err := a.Objects().Head(t.Context(), "file"); err != nil {
		t.Fatalf("old generation unavailable: %v", err)
	}
	redacted := a.CurrentConfig().Storage.S3
	if redacted.AccessKeyID != "***" || redacted.SecretAccessKey != "***" {
		t.Fatal("credentials not redacted")
	}
	if got := metricValue(t, a.Metrics(), "gosvc_object_requests_total", map[string]string{"operation": "head", "outcome": "ok"}); got != 1 {
		t.Fatalf("object metric=%v", got)
	}
}

func TestAbandonedDownloadDeadlineReleasesShutdownLease(t *testing.T) {
	endpoint := objectEndpoint(t, "old", nil)
	cfg := testConfig()
	cfg.Storage.S3 = objectConfig(endpoint.URL)
	cfg.Storage.S3.RequestTimeout = config.Duration(80 * time.Millisecond)
	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	download, err := a.Objects().Get(t.Context(), "file", object.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer download.Body.Close()
	done := make(chan error, 1)
	go func() { done <- a.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("abandoned download held lease forever")
	}
	if got := metricValue(t, a.Metrics(), "gosvc_object_requests_total", map[string]string{"operation": "get", "outcome": "unavailable"}); got != 1 {
		t.Fatalf("abandoned stream outcome=%v", got)
	}
}

func TestObjectFacadeDisabledAndCanceled(t *testing.T) {
	a, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if a.Objects() == nil {
		t.Fatal("facade should be stable while disabled")
	}
	if _, err := a.Objects().Put(context.Background(), "file", strings.NewReader("x"), object.PutOptions{}); !errors.Is(err, object.ErrDisabled) {
		t.Fatalf("disabled Put=%v", err)
	}
}

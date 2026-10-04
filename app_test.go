package gosvc

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/cloudwego/hertz/pkg/app/server"
	ggrpc "google.golang.org/grpc"

	"github.com/Tokimorphling/gosvc/config"
	"github.com/Tokimorphling/gosvc/jsonrpc"
	"github.com/Tokimorphling/gosvc/observability"
	"github.com/Tokimorphling/gosvc/store/redis"
)

func testConfig() *config.Config {
	cfg := config.Default()
	cfg.HTTP.Host, cfg.HTTP.Port = "127.0.0.1", 0
	cfg.GRPC.Host, cfg.GRPC.Port = "127.0.0.1", 0
	cfg.Admin.Host, cfg.Admin.Port = "127.0.0.1", 0
	cfg.Log.Level = "error"
	return cfg
}

func TestNewValidatesAndReleasesBoundListenerOnFailure(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("New(nil) must return an error")
	}
	invalid := testConfig()
	invalid.HTTP.MaxBodyBytes = 0
	if _, err := New(invalid); err == nil {
		t.Fatal("invalid config must be rejected")
	}

	available, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := available.Addr().String()
	_ = available.Close()
	cfg := testConfig()
	cfg.HTTP.Port = available.Addr().(*net.TCPAddr).Port
	cfg.GRPC.Port = cfg.HTTP.Port // HTTP succeeds; gRPC fails on this port.
	if app, err := New(cfg); err == nil {
		_ = app.Close()
		t.Fatal("expected gRPC bind failure")
	}
	rebound, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("failed New left HTTP listener bound: %v", err)
	}
	_ = rebound.Close()
}

func TestCloseBeforeRunIsIdempotentAndConfigIsCopied(t *testing.T) {
	cfg := testConfig()
	cfg.HTTP.CORS.AllowOrigins = []string{"original"}
	app, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	addrs := []string{app.HTTPAddr(), app.GRPCAddr(), app.AdminAddr()}
	cfg.HTTP.CORS.AllowOrigins[0] = "mutated"
	if got := app.Config().HTTP.CORS.AllowOrigins[0]; got != "original" {
		t.Fatalf("input config mutated internal state: %q", got)
	}
	snapshot := app.Config()
	snapshot.HTTP.CORS.AllowOrigins[0] = "modified snapshot"
	if got := app.Config().HTTP.CORS.AllowOrigins[0]; got != "original" {
		t.Fatalf("Config exposed internal state: %q", got)
	}
	if err := app.Close(); err != nil {
		t.Fatal(err)
	}
	if err := app.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	for name, err := range map[string]error{
		"HTTP":     app.RegisterHTTP(func(*server.Hertz) {}),
		"gRPC":     app.RegisterGRPC(func(*ggrpc.Server) {}),
		"JSON-RPC": app.RegisterJSONRPC(func(*jsonrpc.Dispatcher) {}),
		"admin":    app.RegisterAdmin(func(*http.ServeMux) {}),
	} {
		if !errors.Is(err, ErrClosed) {
			t.Errorf("Register%s after Close = %v", name, err)
		}
	}
	for _, addr := range addrs {
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatalf("Close left listener %s bound: %v", addr, err)
		}
		_ = listener.Close()
	}
	if err := app.Run(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Run after Close = %v, want ErrClosed", err)
	}
}

func TestCloseFromRegistrationCallbackDoesNotDeadlock(t *testing.T) {
	app, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	if err := app.RegisterHTTP(func(_ *server.Hertz) {
		if err := app.Close(); err != nil {
			t.Errorf("Close in callback: %v", err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- app.Run(context.Background()) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close in callback blocked Run")
	}
}

func TestConcurrentCloseAndRunStartup(t *testing.T) {
	for range 10 {
		app, err := New(testConfig())
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- app.Run(context.Background()) }()
		if err := app.Close(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, ErrClosed) {
				t.Fatalf("Run during Close = %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Run hung when Close raced with startup")
		}
	}
}

func TestHTTPAuthEnableTakesEffectWithoutBypass(t *testing.T) {
	cfg := testConfig()
	cfg.HTTP.ShutdownTimeout = config.Duration(time.Second)
	app, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- app.Run(context.Background()) }()
	t.Cleanup(func() {
		_ = app.Close()
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("Run did not stop")
		}
	})
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	post := func(key string) (int, error) {
		req, err := http.NewRequest(http.MethodPost, "http://"+app.HTTPAddr()+"/rpc", strings.NewReader(`{"jsonrpc":"2.0","method":"system.methods","id":1}`))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		if key != "" {
			req.Header.Set("X-API-Key", key)
		}
		response, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		defer response.Body.Close()
		return response.StatusCode, nil
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if status, err := post(""); err == nil {
			if status != http.StatusOK {
				t.Fatalf("disabled auth status = %d", status)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("HTTP server did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	next := app.Config()
	next.Auth.Enabled = true
	next.Auth.APIKeys = []string{"key"}
	if err := app.applyConfig(next); err != nil {
		t.Fatal(err)
	}
	if status, err := post(""); err != nil || status != http.StatusUnauthorized {
		t.Fatalf("enabled auth missing key: status=%d err=%v", status, err)
	}
	if status, err := post("key"); err != nil || status != http.StatusOK {
		t.Fatalf("enabled auth valid key: status=%d err=%v", status, err)
	}
}

func TestReloadKeepsOnlyAppliedSections(t *testing.T) {
	app, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	requested := app.Config()
	requested.Service.Name = "restart-required"
	requested.Log.Level = "debug"
	requested.Log.Access.Enabled = true
	requested.Auth.Enabled = true // invalid without credentials
	requested.Limiter.RPS, requested.Limiter.Burst = 10, 10
	requested.Storage.Postgres.Enabled = true
	requested.Storage.Postgres.DSN = "postgres://user:pass@127.0.0.1:1/app"
	requested.Storage.Postgres.PingTimeout = config.Duration(100 * time.Millisecond)
	if err := app.applyConfig(requested); err == nil {
		t.Fatal("partial reload must report failed and restart-only sections")
	}
	effective := app.Config()
	if effective.Service.Name != "gosvc" || effective.Log.Access.Enabled || effective.Auth.Enabled || effective.Storage.Postgres.Enabled {
		t.Fatalf("unapplied settings entered effective config: %+v", effective)
	}
	if effective.Log.Level != "debug" || effective.Limiter.RPS != 10 {
		t.Fatalf("successful sections were not published: %+v", effective)
	}
	// A later retry must still see failed and restart-only differences.
	if err := app.applyConfig(requested); err == nil {
		t.Fatal("unapplied sections must remain retryable")
	}
}

func TestReloadRequiresExplicitAuthDisable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	write := func(raw string) {
		t.Helper()
		raw = "[http]\nhost = \"127.0.0.1\"\nport = 0\n[grpc]\nhost = \"127.0.0.1\"\nport = 0\n[admin]\nhost = \"127.0.0.1\"\nport = 0\n" + raw
		if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("[auth]\nenabled = true\napiKeys = [\"key\"]\n[greeting]\nprefix = \"hi\"\n")
	cfg, err := (config.Source{Path: path}).Load[config.Config]()
	if err != nil {
		t.Fatal(err)
	}
	cfg.HTTP.Host, cfg.HTTP.Port = "127.0.0.1", 0
	cfg.GRPC.Host, cfg.GRPC.Port = "127.0.0.1", 0
	cfg.Admin.Host, cfg.Admin.Port = "127.0.0.1", 0
	app, err := New(cfg, WithHotReload(path, "GOSVC"))
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	write("[auth]\nenabld = false\n[greeting]\nprefix = \"hi\"\n")
	if err := app.Reload(); err == nil || !strings.Contains(err.Error(), "auth.enabld") {
		t.Fatalf("nested auth typo = %v", err)
	}
	write("[auht]\nenabled = false\n[greeting]\nprefix = \"hi\"\n")
	if err := app.Reload(); err == nil || !strings.Contains(err.Error(), "explicit auth.enabled") {
		t.Fatalf("misspelled auth section = %v", err)
	}
	if !app.Config().Auth.Enabled {
		t.Fatal("typo disabled live auth")
	}
	write("[auth]\nenabled = false\n[greeting]\nprefix = \"hi\"\n")
	if err := app.Reload(); err != nil {
		t.Fatalf("explicit disable: %v", err)
	}
	if app.Config().Auth.Enabled {
		t.Fatal("explicit disable did not apply")
	}
}

func TestStorageLeaseAndAdminQueryFollowReload(t *testing.T) {
	mini := miniredis.RunT(t)
	cfg := testConfig()
	cfg.Storage.Redis.Enabled = true
	cfg.Storage.Redis.Addr = mini.Addr()
	app, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	started := make(chan struct{})
	nestedNow := make(chan struct{})
	nestedDone := make(chan error, 1)
	release := make(chan struct{})
	defer func() {
		select {
		case <-nestedNow:
		default:
			close(nestedNow)
		}
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	leaseDone := make(chan error, 1)
	go func() {
		leaseDone <- app.WithStore(func(s *redis.Store) error {
			close(started)
			<-nestedNow
			nestedDone <- app.WithStore(func(*redis.Store) error { return nil })
			<-release
			return s.Incr(t.Context(), "lease", 1)
		})
	}()
	<-started
	reloadDone := make(chan error, 1)
	go func() { reloadDone <- app.reloadStorage(config.Default().Storage) }()
	deadline := time.After(2 * time.Second)
	for app.Store() != nil {
		select {
		case <-deadline:
			t.Fatal("reload could not swap while a callback held the old generation")
		case <-time.After(time.Millisecond):
		}
	}
	close(nestedNow)
	select {
	case err := <-nestedDone:
		if !errors.Is(err, ErrStorageDisabled) {
			t.Fatalf("nested lease after swap = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("nested storage lease blocked behind retiring generation")
	}
	select {
	case err := <-reloadDone:
		t.Fatalf("reload closed a leased store early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-leaseDone; err != nil {
		t.Fatalf("leased operation: %v", err)
	}
	if err := <-reloadDone; err != nil {
		t.Fatalf("reload: %v", err)
	}
	if err := app.WithStore(func(*redis.Store) error { return nil }); !errors.Is(err, ErrStorageDisabled) {
		t.Fatalf("disabled storage lease = %v", err)
	}
	if app.Recorder() != nil || app.StableRecorder() == nil {
		t.Fatal("recorder snapshot or stable holder semantics changed")
	}
	request := httptest.NewRequest(http.MethodGet, "/debug/ts?metric=lease", nil)
	response := httptest.NewRecorder()
	app.admin.Mux().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("admin query after disable = %d: %s", response.Code, response.Body.String())
	}
}

// metricValue returns the value of a metric matching every given label pair.
func metricValue(t *testing.T, m *observability.Metrics, family string, labels map[string]string) float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, candidate := range families {
		if candidate.GetName() != family {
			continue
		}
		for _, metric := range candidate.GetMetric() {
			matched := true
			for key, want := range labels {
				found := false
				for _, label := range metric.GetLabel() {
					if label.GetName() == key && label.GetValue() == want {
						found = true
						break
					}
				}
				if !found {
					matched = false
					break
				}
			}
			if matched {
				if metric.GetCounter() != nil {
					return metric.GetCounter().GetValue()
				}
				if metric.GetGauge() != nil {
					return metric.GetGauge().GetValue()
				}
				t.Fatalf("metric %s has neither counter nor gauge", family)
			}
		}
	}
	return 0
}

// TestTCPDispatcherGetsDefaultObserver verifies that a dedicated TCP method
// table receives the runtime's JSON-RPC metrics observer, while an observer
// the application installed before gosvc.New is preserved.
func TestTCPDispatcherGetsDefaultObserver(t *testing.T) {
	newTCPConfig := func() *config.Config {
		cfg := testConfig()
		cfg.TCP.Enabled = true
		cfg.TCP.Host, cfg.TCP.Port = "127.0.0.1", 0
		return cfg
	}

	defaulted := jsonrpc.NewDispatcher()
	defaulted.Register("mining.hello", func(context.Context, json.RawMessage) (any, error) { return "ok", nil })
	app, err := New(newTCPConfig(), WithTCPDispatcher(defaulted))
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	if _, err := defaulted.Invoke(t.Context(), "mining.hello", nil); err != nil {
		t.Fatal(err)
	}
	if got := metricValue(t, app.Metrics(), "gosvc_jsonrpc_requests_total", map[string]string{"method": "mining.hello", "code": "0"}); got != 1 {
		t.Fatalf("dedicated table JSON-RPC requests = %g, want 1", got)
	}

	var customSeen atomic.Bool
	custom := jsonrpc.NewDispatcher()
	custom.Register("mining.hello", func(context.Context, json.RawMessage) (any, error) { return "ok", nil })
	custom.SetObserver(func(string, int, time.Duration) { customSeen.Store(true) })
	app, err = New(newTCPConfig(), WithTCPDispatcher(custom))
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	if _, err := custom.Invoke(t.Context(), "mining.hello", nil); err != nil {
		t.Fatal(err)
	}
	if !customSeen.Load() {
		t.Fatal("the runtime overrode a deliberately installed observer")
	}
	if got := metricValue(t, app.Metrics(), "gosvc_jsonrpc_requests_total", map[string]string{"method": "mining.hello", "code": "0"}); got != 0 {
		t.Fatalf("custom observer path recorded default metrics: %g", got)
	}
}

// TestOnShutdownHooksDrainWhileTransportsServe verifies that WithOnShutdown
// hooks run when serving has been asked to stop, but while the transports
// still serve — the phase where draining a push broker still reaches live
// clients.
func TestOnShutdownHooksDrainWhileTransportsServe(t *testing.T) {
	cfg := testConfig()
	inHook := make(chan struct{})
	release := make(chan struct{})
	var hooks int
	app, err := New(cfg, WithOnShutdown(func() { hooks++ }, func() {
		hooks++
		close(inHook)
		<-release
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- app.Run(runCtx) }()

	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	ready := func() bool {
		resp, err := client.Get("http://" + app.HTTPAddr() + "/healthz")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}
	deadline := time.Now().Add(3 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("service did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancelRun()
	select {
	case <-inHook:
	case <-time.After(3 * time.Second):
		t.Fatal("the shutdown hooks did not run")
	}
	if hooks != 2 {
		t.Fatalf("hook calls = %d, want 2", hooks)
	}

	// The hook is still parked: the transports must still be serving, or a
	// broker drain could not flush to its clients.
	if !ready() {
		t.Fatal("transports stopped before the shutdown hooks finished")
	}

	close(release)
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the hooks finished")
	}
	// closeResources already ran inside Run; the deferred Close stays a no-op.
	_ = app.Close()
}

func TestSystemHealthReportsReadiness(t *testing.T) {
	app, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	call := func() map[string]any {
		t.Helper()
		resp := app.JSONRPCDispatcher().Handle(t.Context(), &jsonrpc.Request{
			JSONRPC: "2.0",
			ID:      json.RawMessage("1"),
			Method:  "system.health",
		})
		if resp == nil || resp.Error != nil {
			t.Fatalf("system.health response: %+v", resp)
		}
		result, ok := resp.Result.(map[string]any)
		if !ok {
			t.Fatalf("system.health result type = %T", resp.Result)
		}
		return result
	}

	// Before Run the readiness flag is unset and checks report detail.
	if ready := call()["ready"]; ready != false {
		t.Fatalf("pre-run ready = %v", ready)
	}

	// A failing dependency check keeps the service not ready and surfaces
	// its detail, mirroring GET /readyz. The auto-registered postgres probe
	// reports "skipped" because it is disabled in the test config.
	app.Health().AddCheck("flaky", func(context.Context) error {
		return errors.New("boom")
	})
	result := call()
	if result["ready"] != false {
		t.Fatalf("with failing check ready = %v", result["ready"])
	}
	checks, ok := result["checks"].(map[string]string)
	if !ok || checks["flaky"] != "boom" {
		t.Fatalf("checks = %#v", result["checks"])
	}

	// A ready flag plus a passing check flips the answer.
	app.Health().Set(true)
	app.Health().AddCheck("flaky", func(context.Context) error { return nil })
	if ready := call()["ready"]; ready != true {
		t.Fatalf("ready with passing checks = %v", ready)
	}
}

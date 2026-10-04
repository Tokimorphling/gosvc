package deploydemo

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Tokimorphling/gosvc"
	"github.com/Tokimorphling/gosvc/jsonrpc"
)

func TestAuthenticatedAPIAndPublicAssets(t *testing.T) {
	cfg := &Config{}
	cfg.SetDefaults()
	cfg.Tasks = testTaskConfig(t)
	cfg.HTTP.Port, cfg.GRPC.Port, cfg.Admin.Port = 0, 0, 0
	cfg.Auth.Enabled, cfg.Auth.APIKeys = true, []string{"test-key"}
	cfg.Log.Level = "error"
	application, err := Build(cfg, "test-version")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- application.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("demo failed to shut down")
		}
	})
	base := "http://" + application.HTTPAddr()
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	deadline := time.Now().Add(3 * time.Second)
	for {
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("demo failed to start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	request := func(method, path, body string, authenticated bool, want int) []byte {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), method, base+path, bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if authenticated {
			req.Header.Set("X-API-Key", "test-key")
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != want {
			t.Fatalf("%s %s: status=%d body=%s", method, path, resp.StatusCode, raw)
		}
		return raw
	}
	for _, path := range []string{"/", "/app.js", "/style.css", "/healthz", "/readyz"} {
		if len(request("GET", path, "", false, 200)) == 0 {
			t.Fatalf("empty public asset: %s", path)
		}
	}
	for _, path := range []string{"/api/v1/tasks", "/api/v1/status", "/web/index.html"} {
		request("GET", path, "", false, 401)
	}
	request("GET", "/web/index.html", "", true, 404)
	var status Status
	if err := json.Unmarshal(request("GET", "/api/v1/status", "", true, 200), &status); err != nil || status.Version != "test-version" || status.Storage != "JSON file" {
		t.Fatalf("status=%+v, %v", status, err)
	}
	if got := string(request("GET", "/api/v1/tasks", "", true, 200)); got != "[]" {
		t.Fatalf("empty task list: %s", got)
	}
	for _, body := range []string{`{`, `{"title":""}`, `{"title":"valid","unknown":true}`, `{"title":"valid"}{}`} {
		request("POST", "/api/v1/tasks", body, true, 400)
	}
	request("POST", "/api/v1/tasks", `{"title":"`+strings.Repeat("x", 20000)+`"}`, true, 413)
	var created Task
	if err := json.Unmarshal(request("POST", "/api/v1/tasks", `{"title":"persist me"}`, true, 201), &created); err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/tasks/" + strconv.FormatInt(created.ID, 10)
	rpc := jsonrpc.NewHTTPClient(base, jsonrpc.WithHTTPClient(client), jsonrpc.WithHeader("X-API-Key", "test-key"))
	defer rpc.Close()
	completed, err := rpc.Call[IDRequest, Task](t.Context(), "tasks.complete", IDRequest{ID: created.ID})
	if err != nil || !completed.Done {
		t.Fatalf("complete via JSON-RPC=%+v, %v", completed, err)
	}
	var viaREST Task
	if err := json.Unmarshal(request("GET", path, "", true, 200), &viaREST); err != nil || viaREST != completed {
		t.Fatalf("REST=%+v RPC=%+v, %v", viaREST, completed, err)
	}
	reloaded, err := NewService(cfg.Tasks)
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := reloaded.Get(t.Context(), IDRequest{ID: created.ID})
	if err != nil || *persisted != completed {
		t.Fatalf("persisted=%+v RPC=%+v, %v", persisted, completed, err)
	}
	request("GET", "/api/v1/tasks/invalid", "", true, 400)
	request("DELETE", path, "", true, 200)
	request("GET", path, "", true, 404)
}

func TestConfigLoadsAPIKeyFromEnvironment(t *testing.T) {
	t.Setenv("DEMO_AUTH_ENABLED", "true")
	t.Setenv("DEMO_AUTH_API_KEYS", "first-key,second-key")
	cfg, err := (gosvc.Source{Path: "config.toml", EnvPrefix: "DEMO", Strict: true}).Load[Config]()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Auth.Enabled || len(cfg.Auth.APIKeys) != 2 {
		t.Fatal("environment auth configuration not applied")
	}
}

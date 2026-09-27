package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
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
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"example.com/gosvc"
	"example.com/gosvc/config"
	greeterv1 "example.com/gosvc/examples/app/api/greeter/v1"
	"example.com/gosvc/examples/app/greeter"
	"example.com/gosvc/jsonrpc"
	"example.com/gosvc/logging"
)

// TestEndToEnd boots the whole application on ephemeral ports and exercises
// every transport, including the typed JSON-RPC client and the shared error
// mapping.
func TestEndToEnd(t *testing.T) {
	cfg := baseConfig()
	cfg.TCP.Enabled = true
	cfg.TCP.Host, cfg.TCP.Port = "127.0.0.1", 0
	cfg.TCP.Workers = 2
	cfg.TCP.QueueSize = 16
	cfg.TCP.ReadTimeout = config.Duration(5 * time.Second)
	cfg.TCP.ShutdownTimeout = config.Duration(2 * time.Second)

	application, stop := startApp(t, cfg, "")
	defer stop()

	httpBase := "http://" + application.HTTPAddr()
	waitReady(t, httpBase+"/readyz")

	t.Run("rest hello", func(t *testing.T) {
		resp, err := http.Get(httpBase + "/api/v1/hello?name=e2e")
		if err != nil {
			t.Fatalf("GET hello: %v", err)
		}
		defer resp.Body.Close()
		body := readBody(t, resp)

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
		}
		if !strings.Contains(string(body), "hello, e2e") {
			t.Fatalf("body = %s", body)
		}
	})

	t.Run("rest validation error", func(t *testing.T) {
		resp, err := http.Get(httpBase + "/api/v1/hello")
		if err != nil {
			t.Fatalf("GET hello: %v", err)
		}
		defer resp.Body.Close()
		body := readBody(t, resp)

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
		}
		if !strings.Contains(string(body), "invalid_argument") {
			t.Fatalf("body = %s", body)
		}
	})

	t.Run("rest not found", func(t *testing.T) {
		resp, err := http.Get(httpBase + "/api/v1/greetings/999")
		if err != nil {
			t.Fatalf("GET greeting: %v", err)
		}
		defer resp.Body.Close()
		body := readBody(t, resp)

		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
		}
		if !strings.Contains(string(body), "not_found") {
			t.Fatalf("body = %s", body)
		}
	})

	t.Run("jsonrpc raw", func(t *testing.T) {
		raw := postJSON(t, httpBase+"/rpc",
			`{"jsonrpc":"2.0","id":1,"method":"greeter.sayHello","params":{"name":"rpc"}}`)

		var resp struct {
			Result struct {
				Message string `json:"message"`
			} `json:"result"`
			Error *struct {
				Code int `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(raw, &resp); err != nil {
			t.Fatalf("unmarshal %s: %v", raw, err)
		}
		if resp.Error != nil {
			t.Fatalf("unexpected error: %+v", resp.Error)
		}
		if resp.Result.Message != "hello, rpc" {
			t.Fatalf("message = %q", resp.Result.Message)
		}
	})

	t.Run("jsonrpc typed client over http", func(t *testing.T) {
		client := jsonrpc.NewHTTPClient(httpBase)
		defer client.Close()

		resp, err := client.Call[greeter.HelloRequest, *greeter.HelloResponse](
			context.Background(), "greeter.sayHello", greeter.HelloRequest{Name: "typed"})
		if err != nil {
			t.Fatalf("Call: %v", err)
		}
		if resp.Message != "hello, typed" || resp.Protocol != "jsonrpc" {
			t.Fatalf("resp = %+v", resp)
		}

		_, err = client.Call[greeter.GetGreetingRequest, *greeter.Greeting](
			context.Background(), "greeter.getGreeting", greeter.GetGreetingRequest{ID: 999})
		var rpcErr *jsonrpc.Error
		if !errors.As(err, &rpcErr) || rpcErr.Code != jsonrpc.CodeNotFound {
			t.Fatalf("err = %v, want not-found JSON-RPC error", err)
		}
	})

	t.Run("jsonrpc batch with notification", func(t *testing.T) {
		raw := postJSON(t, httpBase+"/rpc", `[
			{"jsonrpc":"2.0","id":2,"method":"greeter.getGreeting","params":{"id":999}},
			{"jsonrpc":"2.0","method":"greeter.sayHello","params":{"name":"notify"}}
		]`)

		var responses []struct {
			Error *struct {
				Code int `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(raw, &responses); err != nil {
			t.Fatalf("unmarshal %s: %v", raw, err)
		}
		if len(responses) != 1 {
			t.Fatalf("got %d responses, want 1 (notification must be skipped)", len(responses))
		}
		if responses[0].Error == nil || responses[0].Error.Code != -32001 {
			t.Fatalf("error = %+v, want not-found code", responses[0].Error)
		}
	})

	t.Run("tcp jsonrpc via typed client", func(t *testing.T) {
		client := jsonrpc.NewTCPClient(application.TCPAddr(), jsonrpc.WithTimeout(3*time.Second))
		defer client.Close()

		resp, err := client.Call[greeter.HelloRequest, *greeter.HelloResponse](
			context.Background(), "greeter.sayHello", greeter.HelloRequest{Name: "tcp"})
		if err != nil {
			t.Fatalf("Call: %v", err)
		}
		if resp.Message != "hello, tcp" {
			t.Fatalf("resp = %+v", resp)
		}
	})

	t.Run("tcp raw frame", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", application.TCPAddr(), 2*time.Second)
		if err != nil {
			t.Fatalf("dial tcp: %v", err)
		}
		defer conn.Close()

		request := `{"jsonrpc":"2.0","id":1,"method":"greeter.sayHello","params":{"name":"frame"}}` + "\n"
		if _, err := conn.Write([]byte(request)); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		line, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if !strings.Contains(line, "hello, frame") {
			t.Fatalf("line = %s", line)
		}
	})

	t.Run("grpc", func(t *testing.T) {
		conn, err := ggrpc.NewClient(application.GRPCAddr(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial grpc: %v", err)
		}
		defer conn.Close()
		client := greeterv1.NewGreeterClient(conn)

		callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer callCancel()

		resp, err := client.SayHello(callCtx, &greeterv1.SayHelloRequest{Name: "grpc"})
		if err != nil {
			t.Fatalf("SayHello: %v", err)
		}
		if resp.GetProtocol() != "grpc" || resp.GetMessage() != "hello, grpc" {
			t.Fatalf("resp = %+v", resp)
		}

		_, err = client.GetGreeting(callCtx, &greeterv1.GetGreetingRequest{Id: 999})
		if status.Code(err) != codes.NotFound {
			t.Fatalf("code = %v, want NotFound (err = %v)", status.Code(err), err)
		}
	})

	t.Run("metrics", func(t *testing.T) {
		body := getBody(t, "http://"+application.AdminAddr()+"/metrics")
		if !strings.Contains(string(body), "gosvc_http_requests_total") {
			t.Fatal("metrics output does not contain the HTTP counter")
		}
		if !strings.Contains(string(body), "gosvc_grpc_requests_total") {
			t.Fatal("metrics output does not contain the gRPC counter")
		}
	})

	t.Run("loglevel runtime control", func(t *testing.T) {
		body := getBody(t, "http://"+application.AdminAddr()+"/debug/loglevel")
		if !strings.Contains(string(body), "error") {
			t.Fatalf("initial level = %s", body)
		}

		req, err := http.NewRequest(http.MethodPut,
			"http://"+application.AdminAddr()+"/debug/loglevel",
			strings.NewReader(`{"level":"warn"}`))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("PUT loglevel: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if body := readBody(t, resp); !strings.Contains(string(body), "warn") {
			t.Fatalf("body = %s", body)
		}
	})

	t.Run("timeseries disabled", func(t *testing.T) {
		resp, err := http.Get("http://" + application.AdminAddr() + "/debug/ts?metric=http.requests")
		if err != nil {
			t.Fatalf("GET ts: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503 when Redis is disabled", resp.StatusCode)
		}
	})
}

// TestAuthEnforced verifies API key authentication across HTTP and gRPC while
// health endpoints stay public.
func TestAuthEnforced(t *testing.T) {
	cfg := baseConfig()
	cfg.Auth.Enabled = true
	cfg.Auth.APIKeys = []string{"test-key"}

	application, stop := startApp(t, cfg, "")
	defer stop()

	httpBase := "http://" + application.HTTPAddr()
	waitReady(t, httpBase+"/readyz")

	t.Run("health stays public", func(t *testing.T) {
		resp, err := http.Get(httpBase + "/healthz")
		if err != nil {
			t.Fatalf("GET healthz: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d", resp.StatusCode)
		}
	})

	t.Run("rest rejects missing key", func(t *testing.T) {
		resp, err := http.Get(httpBase + "/api/v1/hello?name=x")
		if err != nil {
			t.Fatalf("GET hello: %v", err)
		}
		defer resp.Body.Close()
		body := readBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
		}
		if !strings.Contains(string(body), "unauthenticated") {
			t.Fatalf("body = %s", body)
		}
	})

	t.Run("rest accepts api key", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, httpBase+"/api/v1/hello?name=x", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-API-Key", "test-key")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET hello: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, body = %s", resp.StatusCode, readBody(t, resp))
		}
	})

	t.Run("jsonrpc typed client sends api key", func(t *testing.T) {
		client := jsonrpc.NewHTTPClient(httpBase, jsonrpc.WithHeader("X-API-Key", "test-key"))
		defer client.Close()

		resp, err := client.Call[greeter.EmptyRequest, *greeter.Info](context.Background(), "greeter.info", greeter.EmptyRequest{})
		if err != nil {
			t.Fatalf("Call: %v", err)
		}
		if resp.Name == "" {
			t.Fatalf("resp = %+v", resp)
		}
	})

	t.Run("grpc auth", func(t *testing.T) {
		conn, err := ggrpc.NewClient(application.GRPCAddr(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial grpc: %v", err)
		}
		defer conn.Close()
		client := greeterv1.NewGreeterClient(conn)

		callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer callCancel()

		if _, err := client.SayHello(callCtx, &greeterv1.SayHelloRequest{Name: "x"}); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("code = %v, want Unauthenticated", status.Code(err))
		}

		authedCtx := metadata.AppendToOutgoingContext(callCtx, "x-api-key", "test-key")
		if _, err := client.SayHello(authedCtx, &greeterv1.SayHelloRequest{Name: "x"}); err != nil {
			t.Fatalf("authenticated SayHello: %v", err)
		}
	})
}

// TestLogSampling verifies that repeated identical records are sampled and that
// the counters are exposed on the admin endpoint.
func TestLogSampling(t *testing.T) {
	cfg := baseConfig()
	cfg.Log.Level = "info"
	cfg.Log.Sampling.Enabled = true
	cfg.Log.Sampling.Initial = 1
	cfg.Log.Sampling.Thereafter = 1000
	cfg.Log.Sampling.Tick = config.Duration(time.Hour)

	application, stop := startApp(t, cfg, "")
	defer stop()

	httpBase := "http://" + application.HTTPAddr()
	waitReady(t, httpBase+"/readyz")

	for i := 0; i < 5; i++ {
		getBody(t, httpBase+"/api/v1/hello?name=sample")
	}

	body := getBody(t, "http://"+application.AdminAddr()+"/debug/logstats")
	var stats struct {
		Enabled bool   `json:"enabled"`
		Emitted uint64 `json:"emitted"`
		Dropped uint64 `json:"dropped"`
	}
	if err := json.Unmarshal(body, &stats); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	if !stats.Enabled {
		t.Fatalf("sampling must be enabled: %s", body)
	}
	if stats.Emitted == 0 || stats.Dropped == 0 {
		t.Fatalf("expected emitted and dropped records: %s", body)
	}
}

// TestConfigHotReload rewrites the config file and verifies that auth and log
// level are applied without restarting, that secrets are redacted and that the
// manual reload endpoint works.
func TestConfigHotReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")

	const secret = "0123456789abcdef"
	writeConfig := func(apiKey, level string) {
		t.Helper()
		content := `
[service]
name = "reload-test"
env = "dev"

[http]
host = "127.0.0.1"
port = 0

[grpc]
host = "127.0.0.1"
port = 0

[admin]
host = "127.0.0.1"
port = 0

[log]
level = "` + level + `"
format = "json"

[auth]
enabled = true
apiKeys = ["` + apiKey + `"]

[auth.jwt]
secret = "` + secret + `"

[greeting]
prefix = "hi"
maxNameLen = 32
`
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	writeConfig("key-1", "error")
	cfg, err := Load(path, "GOSVC")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Greeting.Prefix != "hi" || cfg.Greeting.MaxNameLen != 32 {
		t.Fatalf("application section not loaded: %+v", cfg.Greeting)
	}

	application, stop := startApp(t, cfg, path)
	defer stop()

	httpBase := "http://" + application.HTTPAddr()
	adminBase := "http://" + application.AdminAddr()
	waitReady(t, httpBase+"/readyz")

	authed := func(key string) int {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, httpBase+"/api/v1/info", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-API-Key", key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	if code := authed("key-1"); code != http.StatusOK {
		t.Fatalf("key-1 before reload: status = %d", code)
	}
	if code := authed("key-2"); code != http.StatusUnauthorized {
		t.Fatalf("key-2 before reload: status = %d", code)
	}

	writeConfig("key-2", "info")

	waitFor(t, 8*time.Second, func() bool { return authed("key-2") == http.StatusOK })
	if code := authed("key-1"); code != http.StatusUnauthorized {
		t.Fatalf("key-1 after reload: status = %d", code)
	}

	waitFor(t, 5*time.Second, func() bool {
		return strings.Contains(string(getBody(t, adminBase+"/debug/loglevel")), "info")
	})

	body := getBody(t, adminBase+"/debug/config")
	if strings.Contains(string(body), secret) {
		t.Fatalf("secret leaked through /debug/config: %s", body)
	}
	if !strings.Contains(string(body), "***") {
		t.Fatalf("expected redacted secrets: %s", body)
	}

	resp, err := http.Post(adminBase+"/debug/reload", "application/json", nil)
	if err != nil {
		t.Fatalf("manual reload: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("manual reload status = %d, body = %s", resp.StatusCode, readBody(t, resp))
	}
}

// TestTraceContextInLogs enables tracing against a fake OTLP collector and
// verifies that request logs carry trace_id/span_id and that spans are
// exported.
func TestTraceContextInLogs(t *testing.T) {
	var exported atomic.Int64
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		exported.Add(1)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	defer collector.Close()

	logPath := filepath.Join(t.TempDir(), "app.log")

	cfg := baseConfig()
	cfg.Log.Level = "info"
	cfg.Log.Output = "file"
	cfg.Log.File.Path = logPath
	cfg.Telemetry.Enabled = true
	cfg.Telemetry.OTLPEndpoint = strings.TrimPrefix(collector.URL, "http://")
	cfg.Telemetry.Insecure = true
	cfg.Telemetry.BatchTimeout = config.Duration(200 * time.Millisecond)

	application, stop := startApp(t, cfg, "")
	defer stop()

	httpBase := "http://" + application.HTTPAddr()
	waitReady(t, httpBase+"/readyz")
	getBody(t, httpBase+"/api/v1/hello?name=trace")

	conn, err := ggrpc.NewClient(application.GRPCAddr(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	defer conn.Close()
	client := greeterv1.NewGreeterClient(conn)
	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()
	if _, err := client.SayHello(callCtx, &greeterv1.SayHelloRequest{Name: "trace"}); err != nil {
		t.Fatalf("SayHello: %v", err)
	}

	waitFor(t, 5*time.Second, func() bool {
		return logRecordHas(t, logPath, "http request", "trace_id")
	})
	waitFor(t, 5*time.Second, func() bool {
		return logRecordHas(t, logPath, "grpc request", "trace_id")
	})
	waitFor(t, 5*time.Second, func() bool { return exported.Load() > 0 })
}

// logRecordHas reports whether the JSON log file contains a record with the
// given msg and a non-empty string field.
func logRecordHas(t *testing.T, path, msg, field string) bool {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		if record["msg"] != msg {
			continue
		}
		if value, ok := record[field].(string); ok && value != "" {
			return true
		}
	}
	return false
}

// TestAccessLogSink verifies that request logs can be routed to a dedicated
// sink without leaking into the application log.
func TestAccessLogSink(t *testing.T) {
	dir := t.TempDir()
	appLog := filepath.Join(dir, "app.log")
	accessLog := filepath.Join(dir, "access.log")

	cfg := baseConfig()
	cfg.Log.Level = "info"
	cfg.Log.Output = "file"
	cfg.Log.File.Path = appLog
	cfg.Log.Access.Enabled = true
	cfg.Log.Access.Output = "file"
	cfg.Log.Access.File.Path = accessLog

	application, stop := startApp(t, cfg, "")
	defer stop()

	httpBase := "http://" + application.HTTPAddr()
	waitReady(t, httpBase+"/readyz")
	getBody(t, httpBase+"/api/v1/hello?name=access")

	waitFor(t, 5*time.Second, func() bool {
		return logRecordHas(t, accessLog, "http request", "log_type")
	})
	if raw, err := os.ReadFile(appLog); err == nil && strings.Contains(string(raw), "http request") {
		t.Fatalf("access lines must not appear in the application log: %s", raw)
	}
}

// TestStorageHotReload verifies that storage connections are rebuilt on reload,
// that disabling Redis clears the recorder, and that a failing rebuild keeps the
// current state and the service alive.
func TestStorageHotReload(t *testing.T) {
	mini := miniredis.RunT(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")

	writeConfig := func(redisAddr, postgresDSN string) {
		t.Helper()
		content := `
[service]
name = "storage-reload"
env = "dev"

[http]
host = "127.0.0.1"
port = 0

[grpc]
host = "127.0.0.1"
port = 0

[admin]
host = "127.0.0.1"
port = 0

[log]
level = "error"
format = "json"
`
		if redisAddr != "" {
			content += `
[storage.redis]
enabled = true
mode = "single"
addr = "` + redisAddr + `"
prefix = "reload-test"
bucketTtl = "1h"
queueSize = 64
`
		}
		if postgresDSN != "" {
			content += `
[storage.postgres]
enabled = true
dsn = "` + postgresDSN + `"
pingTimeout = "300ms"
`
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	writeConfig(mini.Addr(), "")
	cfg, err := Load(path, "GOSVC")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	application, stop := startApp(t, cfg, path)
	defer stop()

	httpBase := "http://" + application.HTTPAddr()
	waitReady(t, httpBase+"/readyz")

	if application.Store() == nil {
		t.Fatal("redis store must be enabled")
	}
	if application.Recorder() == nil {
		t.Fatal("recorder must be set")
	}

	// Disabling Redis closes the store and clears the recorder.
	writeConfig("", "")
	waitFor(t, 8*time.Second, func() bool {
		return application.Store() == nil && application.Recorder() == nil
	})

	// A failing rebuild (unreachable postgres) keeps the current state and the
	// service keeps serving.
	writeConfig("", "postgres://user:pass@127.0.0.1:1/app")
	waitFor(t, 8*time.Second, func() bool { return application.Config().Storage.Postgres.Enabled })
	if application.Postgres() != nil {
		t.Fatal("failed rebuild must not install a broken pool")
	}
	getBody(t, httpBase+"/healthz")
}

func baseConfig() *Config {
	cfg := &Config{}
	cfg.SetDefaults()

	cfg.HTTP.Host, cfg.HTTP.Port = "127.0.0.1", 0
	cfg.GRPC.Host, cfg.GRPC.Port = "127.0.0.1", 0
	cfg.Admin.Host, cfg.Admin.Port = "127.0.0.1", 0
	cfg.Log.Level = "error"
	return cfg
}

func startApp(t *testing.T, cfg *Config, configPath string) (*gosvc.App, func()) {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}

	logHandle, err := logging.New(cfg.Log, cfg.Service.Name, cfg.Service.Env, Version)
	if err != nil {
		t.Fatalf("build logger: %v", err)
	}
	slog.SetDefault(logHandle.Logger())

	application, err := Build(Options{
		Config:     cfg,
		Log:        logHandle,
		ConfigPath: configPath,
		EnvPrefix:  "GOSVC",
		Version:    Version,
	})
	if err != nil {
		t.Fatalf("build app: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- application.Run(ctx) }()

	stop := func() {
		t.Helper()
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("app.Run returned error: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("app did not shut down within 15s")
		}
	}
	return application, stop
}

func postJSON(t *testing.T, url, body string) []byte {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	raw := readBody(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST %s: status = %d, body = %s", url, resp.StatusCode, raw)
	}
	return raw
}

func getBody(t *testing.T, url string) []byte {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	return readBody(t, resp)
}

func readBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return raw
}

func waitReady(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("service not ready after 10s: %s", url)
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

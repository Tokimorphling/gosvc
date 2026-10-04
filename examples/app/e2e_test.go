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

	"github.com/Tokimorphling/gosvc"
	"github.com/Tokimorphling/gosvc/config"
	greeterv1 "github.com/Tokimorphling/gosvc/examples/app/api/greeter/v1"
	"github.com/Tokimorphling/gosvc/examples/app/greeter"
	"github.com/Tokimorphling/gosvc/jsonrpc"
	"github.com/Tokimorphling/gosvc/logging"
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
		if rpcErr, ok := errors.AsType[*jsonrpc.Error](err); !ok || rpcErr.Code != jsonrpc.CodeNotFound {
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

	t.Run("tcp push notifications", func(t *testing.T) {
		// Subscriber A: raw connection, subscribes and keeps reading.
		subscriber, err := net.DialTimeout("tcp", application.TCPAddr(), 2*time.Second)
		if err != nil {
			t.Fatalf("dial tcp: %v", err)
		}
		defer subscriber.Close()

		if _, err := subscriber.Write([]byte(`{"jsonrpc":"2.0","id":1,"method":"events.subscribe"}` + "\n")); err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		reader := bufio.NewReader(subscriber)
		if err := subscriber.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read subscribe response: %v", err)
		}
		if !strings.Contains(line, `"subscribers":1`) {
			t.Fatalf("subscribe response = %s", line)
		}

		// Trigger B: a second connection pokes the broker.
		trigger, err := net.DialTimeout("tcp", application.TCPAddr(), 2*time.Second)
		if err != nil {
			t.Fatalf("dial tcp: %v", err)
		}
		defer trigger.Close()
		if _, err := trigger.Write([]byte(`{"jsonrpc":"2.0","id":1,"method":"events.ping"}` + "\n")); err != nil {
			t.Fatalf("ping: %v", err)
		}

		// A receives the pushed notification.
		if err := subscriber.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		line, err = reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read pushed notification: %v", err)
		}
		if !strings.Contains(line, `"method":"events.pong"`) || strings.Contains(line, `"id":1`) {
			t.Fatalf("pushed frame must be an id-less notification, got %s", line)
		}

		// The push is observable as a metric.
		if !strings.Contains(string(getBody(t, "http://"+application.AdminAddr()+"/metrics")), "gosvc_notify_sent_total") {
			t.Fatal("metrics output does not contain the notify counter")
		}
	})

	t.Run("tcp push subscribe requires the tcp transport", func(t *testing.T) {
		// events.subscribe through HTTP /rpc must fail with a typed error:
		// sessions only exist on TCP connections.
		raw := postJSON(t, httpBase+"/rpc", `{"jsonrpc":"2.0","id":2,"method":"events.subscribe"}`)
		if !strings.Contains(string(raw), "requires the TCP transport") {
			t.Fatalf("subscribe over HTTP must be rejected, got %s", raw)
		}
	})

	t.Run("grpc server streaming", func(t *testing.T) {
		conn, err := ggrpc.NewClient(application.GRPCAddr(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial grpc: %v", err)
		}
		defer conn.Close()
		client := greeterv1.NewGreeterClient(conn)

		streamCtx, streamCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer streamCancel()
		stream, err := client.WatchGreetings(streamCtx, &greeterv1.WatchGreetingsRequest{Id: 1})
		if err != nil {
			t.Fatalf("WatchGreetings: %v", err)
		}

		updates := 0
		for {
			update, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("Recv: %v", err)
			}
			if update.GetText() != "hello" || update.GetSequence() != int64(updates) {
				t.Fatalf("update = %+v, want sequence %d", update, updates)
			}
			updates++
			if updates == 2 {
				break // two updates prove the stream keeps producing
			}
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("CloseSend: %v", err)
		}
	})

	t.Run("sse event stream", func(t *testing.T) {
		resp, err := http.Get(httpBase + "/api/v1/events")
		if err != nil {
			t.Fatalf("GET events: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
			t.Fatalf("content-type = %q", got)
		}

		scanner := bufio.NewScanner(resp.Body)
		var data string
		for scanner.Scan() {
			if line := scanner.Text(); strings.HasPrefix(line, "data:") {
				data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
				break
			}
		}
		if err := scanner.Err(); err != nil {
			t.Fatalf("read event stream: %v", err)
		}
		if !strings.Contains(data, "hello from the event stream") {
			t.Fatalf("first event data = %q", data)
		}
		// Closing the response cancels the server-side handler through the
		// runtime's disconnect detection.
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

	t.Run("grpc stream auth", func(t *testing.T) {
		conn, err := ggrpc.NewClient(application.GRPCAddr(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial grpc: %v", err)
		}
		defer conn.Close()
		client := greeterv1.NewGreeterClient(conn)

		callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer callCancel()

		// Streaming RPCs go through the same auth interceptor chain. The
		// stream is established lazily, so the rejection surfaces on the
		// first Recv, not on the opening call.
		stream, err := client.WatchGreetings(callCtx, &greeterv1.WatchGreetingsRequest{Id: 1})
		if err == nil {
			_, err = stream.Recv()
		}
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("code = %v, want Unauthenticated on streams", status.Code(err))
		}
	})
}

// TestAuthPublicRoutePattern verifies that WithPublicPaths accepts route
// patterns: whitelisting "/api/v1/greetings/:id" unauthenticated every id,
// while other routes keep requiring credentials.
func TestAuthPublicRoutePattern(t *testing.T) {
	cfg := baseConfig()
	cfg.Auth.Enabled = true
	cfg.Auth.APIKeys = []string{"test-key"}

	application, stop := startApp(t, cfg, "", "/api/v1/greetings/:id")
	defer stop()

	httpBase := "http://" + application.HTTPAddr()
	waitReady(t, httpBase+"/readyz")

	resp, err := http.Get(httpBase + "/api/v1/greetings/3")
	if err != nil {
		t.Fatalf("GET greeting without credentials: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("whitelisted route pattern must stay public, status = %d", resp.StatusCode)
	}
	if !strings.Contains(string(readBody(t, resp)), "你好") {
		t.Fatalf("unexpected body: %s", readBody(t, resp))
	}

	unauthed, err := http.Get(httpBase + "/api/v1/hello?name=x")
	if err != nil {
		t.Fatalf("GET hello: %v", err)
	}
	defer unauthed.Body.Close()
	if unauthed.StatusCode != http.StatusUnauthorized {
		t.Fatalf("routes outside the whitelist must still require auth, status = %d", unauthed.StatusCode)
	}
}

// TestAdminToken verifies that a configured admin token gates every admin
// endpoint and stays redacted in the config dump.
func TestAdminToken(t *testing.T) {
	const token = "test-admin-token-1234"

	cfg := baseConfig()
	cfg.Admin.Token = token

	application, stop := startApp(t, cfg, "")
	defer stop()

	adminBase := "http://" + application.AdminAddr()
	waitReady(t, "http://"+application.HTTPAddr()+"/readyz")

	resp, err := http.Get(adminBase + "/metrics")
	if err != nil {
		t.Fatalf("GET metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("metrics without token must be rejected, status = %d", resp.StatusCode)
	}

	req, err := http.NewRequest(http.MethodGet, adminBase+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	authed, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET metrics with token: %v", err)
	}
	defer authed.Body.Close()
	if authed.StatusCode != http.StatusOK {
		t.Fatalf("metrics with token must pass, status = %d", authed.StatusCode)
	}

	cfgReq, err := http.NewRequest(http.MethodGet, adminBase+"/debug/config", nil)
	if err != nil {
		t.Fatal(err)
	}
	cfgReq.Header.Set("Authorization", "Bearer "+token)
	cfgResp, err := http.DefaultClient.Do(cfgReq)
	if err != nil {
		t.Fatalf("GET config with token: %v", err)
	}
	defer cfgResp.Body.Close()
	dump := string(readBody(t, cfgResp))
	if strings.Contains(dump, token) {
		t.Fatalf("admin token must be redacted in the config dump: %s", dump)
	}
	if !strings.Contains(dump, "***") {
		t.Fatalf("expected a redacted secret marker: %s", dump)
	}
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

	for range 5 {
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
	for line := range strings.SplitSeq(string(raw), "\n") {
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

	writeConfig := func(level, redisAddr, postgresDSN string) {
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
level = "` + level + `"
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

	writeConfig("error", mini.Addr(), "")
	cfg, err := Load(path, "GOSVC")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	application, stop := startApp(t, cfg, path)
	defer stop()

	httpBase := "http://" + application.HTTPAddr()
	adminBase := "http://" + application.AdminAddr()
	waitReady(t, httpBase+"/readyz")

	if application.Store() == nil {
		t.Fatal("redis store must be enabled")
	}
	if application.Recorder() == nil {
		t.Fatal("recorder must be set")
	}

	// Disabling Redis closes the store and clears the recorder.
	writeConfig("error", "", "")
	waitFor(t, 8*time.Second, func() bool {
		return application.Store() == nil && application.Recorder() == nil
	})

	// A failing rebuild (unreachable postgres) keeps the current state: no
	// broken pool is installed, the effective config keeps the previous
	// storage section, and the service keeps serving. The manual reload
	// endpoint is synchronous and reports a partial failure, while the log
	// level flipped by the same file proves other sections were still applied.
	writeConfig("warn", "", "postgres://user:pass@127.0.0.1:1/app")
	resp, err := http.Post(adminBase+"/debug/reload", "application/json", nil)
	if err != nil {
		t.Fatalf("manual reload: %v", err)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("manual reload status = %d, body = %s", resp.StatusCode, readBody(t, resp))
	}
	resp.Body.Close()

	if application.Postgres() != nil {
		t.Fatal("failed rebuild must not install a broken pool")
	}
	if application.Config().Storage.Postgres.Enabled {
		t.Fatal("failed rebuild must keep the previous storage section in the effective config")
	}
	if !strings.Contains(string(getBody(t, adminBase+"/debug/loglevel")), "warn") {
		t.Fatalf("the same reload must apply hot-reloadable sections: %s", getBody(t, adminBase+"/debug/loglevel"))
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

func startApp(t *testing.T, cfg *Config, configPath string, publicPaths ...string) (*gosvc.App, func()) {
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
		Config:      cfg,
		Log:         logHandle,
		ConfigPath:  configPath,
		EnvPrefix:   "GOSVC",
		Version:     Version,
		PublicPaths: publicPaths,
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
		// Close idle keep-alive connections held by the shared default
		// transport so the graceful drain does not wait for them until the
		// shutdown timeout. This mirrors a load balancer dropping its pooled
		// connections when the endpoint leaves rotation.
		if transport, ok := http.DefaultTransport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
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

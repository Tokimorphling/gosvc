package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	greeterv1 "example.com/gosvc/api/greeter/v1"
	"example.com/gosvc/internal/app"
	"example.com/gosvc/internal/config"
	"example.com/gosvc/internal/logging"
	"example.com/gosvc/internal/version"
)

// TestEndToEnd boots the whole application on ephemeral ports and exercises
// every transport, including the shared error mapping.
func TestEndToEnd(t *testing.T) {
	cfg := baseConfig()
	cfg.TCP.Enabled = true
	cfg.TCP.Host, cfg.TCP.Port = "127.0.0.1", 0
	cfg.TCP.Workers = 2
	cfg.TCP.QueueSize = 16
	cfg.TCP.ReadTimeout = config.Duration(5 * time.Second)
	cfg.TCP.ShutdownTimeout = config.Duration(2 * time.Second)

	application, stop := startApp(t, cfg)
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

	t.Run("jsonrpc single", func(t *testing.T) {
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

	t.Run("tcp jsonrpc", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", application.TCPAddr(), 2*time.Second)
		if err != nil {
			t.Fatalf("dial tcp: %v", err)
		}
		defer conn.Close()

		request := `{"jsonrpc":"2.0","id":1,"method":"greeter.sayHello","params":{"name":"tcp"}}` + "\n"
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

		var resp struct {
			Result struct {
				Message string `json:"message"`
			} `json:"result"`
			Error *struct {
				Code int `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal([]byte(line), &resp); err != nil {
			t.Fatalf("unmarshal %q: %v", line, err)
		}
		if resp.Error != nil {
			t.Fatalf("unexpected error: %+v", resp.Error)
		}
		if resp.Result.Message != "hello, tcp" {
			t.Fatalf("message = %q", resp.Result.Message)
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

	application, stop := startApp(t, cfg)
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

	t.Run("jsonrpc rejects missing key", func(t *testing.T) {
		resp, err := http.Post(httpBase+"/rpc", "application/json",
			strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"greeter.info"}`))
		if err != nil {
			t.Fatalf("POST rpc: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d", resp.StatusCode)
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

func baseConfig() *config.Config {
	cfg := config.Default()
	cfg.HTTP.Host, cfg.HTTP.Port = "127.0.0.1", 0
	cfg.GRPC.Host, cfg.GRPC.Port = "127.0.0.1", 0
	cfg.Admin.Host, cfg.Admin.Port = "127.0.0.1", 0
	cfg.Log.Level = "error"
	return cfg
}

func startApp(t *testing.T, cfg *config.Config) (*app.App, func()) {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}

	logger, level, err := logging.New(cfg.Log, cfg.Service.Name, cfg.Service.Env, version.Version)
	if err != nil {
		t.Fatalf("build logger: %v", err)
	}
	application, err := app.New(cfg, logger, level)
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

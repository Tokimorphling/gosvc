package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	greeterv1 "example.com/gosvc/api/greeter/v1"
	"example.com/gosvc/internal/app"
	"example.com/gosvc/internal/config"
	"example.com/gosvc/internal/logging"
)

// TestEndToEnd boots the whole application on ephemeral ports and exercises
// every transport, including the shared error mapping.
func TestEndToEnd(t *testing.T) {
	cfg := config.Default()
	cfg.HTTP.Host, cfg.HTTP.Port = "127.0.0.1", 0
	cfg.GRPC.Host, cfg.GRPC.Port = "127.0.0.1", 0
	cfg.Admin.Host, cfg.Admin.Port = "127.0.0.1", 0
	cfg.Log.Level = "error"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}

	logger, level, err := logging.New(cfg.Log)
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

	t.Run("grpc", func(t *testing.T) {
		conn, err := ggrpc.NewClient(application.GRPCAddr(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial grpc: %v", err)
		}
		defer conn.Close()
		client := greeterv1.NewGreeterClient(conn)

		callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
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
		resp, err := http.Get("http://" + application.AdminAddr() + "/metrics")
		if err != nil {
			t.Fatalf("GET metrics: %v", err)
		}
		defer resp.Body.Close()
		body := readBody(t, resp)

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if !strings.Contains(string(body), "gosvc_http_requests_total") {
			t.Fatalf("metrics output does not contain the HTTP counter")
		}
	})

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

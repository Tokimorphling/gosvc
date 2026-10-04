package jsonrpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/sonic"

	"github.com/Tokimorphling/gosvc/apierror"
)

type echoRequest struct {
	Name string `json:"name"`
}

type echoResponse struct {
	Message string `json:"message"`
}

func newEchoDispatcher() *Dispatcher {
	d := NewDispatcher()
	d.RegisterTyped("echo", func(_ context.Context, req echoRequest) (*echoResponse, error) {
		return &echoResponse{Message: "hello " + req.Name}, nil
	})
	d.Register("fail", func(context.Context, json.RawMessage) (any, error) {
		return nil, apierror.New(apierror.KindNotFound, "nope")
	})
	return d
}

func TestRegisterTyped(t *testing.T) {
	d := newEchoDispatcher()

	raw, ok := d.Serve(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"echo","params":{"name":"x"}}`))
	if !ok {
		t.Fatal("expected a response")
	}
	resp := decodeResponse(t, raw)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %+v", resp.Error)
	}
	var result echoResponse
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		t.Fatal(err)
	}
	if result.Message != "hello x" {
		t.Fatalf("result = %+v", result)
	}
}

func TestClientHTTP(t *testing.T) {
	d := newEchoDispatcher()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		resp, ok := d.Serve(r.Context(), body)
		if !ok {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(resp)
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL)
	defer client.Close()

	resp, err := client.Call[echoRequest, *echoResponse](context.Background(), "echo", echoRequest{Name: "http"})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if resp.Message != "hello http" {
		t.Fatalf("resp = %+v", resp)
	}

	if err := client.Notify(context.Background(), "echo", echoRequest{Name: "notify"}); err != nil {
		t.Fatalf("Notify: %v", err)
	}
}

func TestClientHTTPPropagatesRPCError(t *testing.T) {
	d := newEchoDispatcher()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		resp, ok := d.Serve(r.Context(), body)
		if !ok {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = w.Write(resp)
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL)
	defer client.Close()

	_, err := client.Call[struct{}, any](context.Background(), "fail", struct{}{})
	var rpcErr *Error
	if !errors.As(err, &rpcErr) {
		t.Fatalf("err = %v, want *jsonrpc.Error", err)
	}
	if rpcErr.Code != CodeNotFound {
		t.Fatalf("code = %d, want %d", rpcErr.Code, CodeNotFound)
	}
}

func TestHTTPNotificationsReportTransportFailures(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		kind   apierror.Kind
	}{
		{200, "", ""}, {202, "", ""}, {204, "", ""},
		{401, `{"error":{"code":"unauthenticated","message":"missing API key"}}`, apierror.KindUnauthenticated},
		{429, `{"error":{"code":"rate_limited","message":"busy"}}`, apierror.KindRateLimited},
		{500, `{"error":{"code":"internal","message":"internal server error"}}`, apierror.KindInternal},
		{503, "proxy unavailable", apierror.KindUnknown},
	} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			client := NewHTTPClient(server.URL)
			defer client.Close()
			err := client.Notify(t.Context(), "event", struct{}{})
			if tc.status < 300 {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || apierror.KindOf(err) != tc.kind {
				t.Fatalf("Notify = %v, want kind %q", err, tc.kind)
			}
		})
	}
}

func TestHTTPClientBoundsUnresponsiveServer(t *testing.T) {
	for _, tc := range []struct {
		name   string
		opts   []ClientOption
		budget time.Duration
	}{
		{"default", nil, defaultTimeout},
		{"configured", []ClientOption{WithTimeout(30 * time.Millisecond)}, 30 * time.Millisecond},
		{"custom-client", []ClientOption{WithHTTPClient(&http.Client{Timeout: 30 * time.Millisecond})}, 30 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				<-r.Context().Done()
			}))
			defer server.Close()
			client := NewHTTPClient(server.URL, tc.opts...)
			defer client.Close()
			ctx, cancel := context.WithTimeout(t.Context(), tc.budget+2*time.Second)
			defer cancel()
			_, err := client.Call[struct{}, any](ctx, "slow", struct{}{})
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Call = %v", err)
			}
			if ctx.Err() != nil {
				t.Fatal("client waited for caller deadline instead of its own budget")
			}
		})
	}
}

func TestClientTCP(t *testing.T) {
	d := newEchoDispatcher()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				for {
					line, err := reader.ReadBytes('\n')
					if err != nil {
						return
					}
					resp, ok := d.Serve(context.Background(), line)
					if !ok {
						continue
					}
					_, _ = conn.Write(append(resp, '\n'))
				}
			}(conn)
		}
	}()

	client := NewTCPClient(listener.Addr().String())
	defer client.Close()

	resp, err := client.Call[echoRequest, *echoResponse](context.Background(), "echo", echoRequest{Name: "tcp"})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if resp.Message != "hello tcp" {
		t.Fatalf("resp = %+v", resp)
	}

	// The connection is reused for the second call.
	resp, err = client.Call[echoRequest, *echoResponse](context.Background(), "echo", echoRequest{Name: "again"})
	if err != nil {
		t.Fatalf("second Call: %v", err)
	}
	if resp.Message != "hello again" {
		t.Fatalf("resp = %+v", resp)
	}
}

// TestClientTCPPipelinesConcurrentCalls verifies that the TCP transport keeps
// multiple requests in flight on one connection and matches responses by id:
// a slow response must not delay an unrelated fast one.
func TestClientTCPPipelinesConcurrentCalls(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	// slow echoes "slow" with a delay; every other name answers immediately.
	// Requests are handled concurrently per connection so a pipelined client
	// can overlap them.
	var writeMu sync.Mutex
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				for {
					line, err := reader.ReadBytes('\n')
					if err != nil {
						return
					}
					go func(line []byte) {
						var req struct {
							ID     json.RawMessage `json:"id"`
							Params struct {
								Name string `json:"name"`
							} `json:"params"`
						}
						if err := sonic.Unmarshal(line, &req); err != nil {
							return
						}
						if req.Params.Name == "slow" {
							time.Sleep(300 * time.Millisecond)
						}
						resp := fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":{"message":"hello %s"}}`, req.ID, req.Params.Name)
						writeMu.Lock()
						_, _ = conn.Write([]byte(resp + "\n"))
						writeMu.Unlock()
					}(append([]byte(nil), line...))
				}
			}(conn)
		}
	}()

	client := NewTCPClient(listener.Addr().String())
	defer client.Close()

	slowDone := make(chan error, 1)
	fastDone := make(chan error, 1)
	start := time.Now()

	go func() {
		_, err := client.Call[echoRequest, *echoResponse](context.Background(), "echo", echoRequest{Name: "slow"})
		slowDone <- err
	}()
	// Give the slow request a head start so ordering alone cannot pass the
	// test: the fast response must come back while the slow one is pending.
	time.Sleep(100 * time.Millisecond)
	go func() {
		_, err := client.Call[echoRequest, *echoResponse](context.Background(), "echo", echoRequest{Name: "fast"})
		fastDone <- err
	}()

	if err := <-fastDone; err != nil {
		t.Fatalf("fast call failed: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("fast call waited %s for the slow one; requests are not pipelined", elapsed)
	}
	if err := <-slowDone; err != nil {
		t.Fatalf("slow call failed: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
		t.Fatalf("slow call returned in %s, want at least the server delay", elapsed)
	}
}

// TestClientTCPReceivesNotifications verifies that the TCP transport routes
// server-originated notifications (frames without an id) to the handler while
// responses keep being matched to their calls.
func TestClientTCPReceivesNotifications(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	notifications := make(chan string, 4)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				return
			}
			var req struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
			}
			if sonic.Unmarshal(line, &req) != nil || req.ID == nil {
				continue
			}
			// Push a notification before the response: the client must route
			// it to the handler instead of confusing it with the reply.
			_, _ = conn.Write([]byte(`{"jsonrpc":"2.0","method":"events.tick","params":{"n":1}}` + "\n"))
			resp := fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":{"message":"hello"}}`, req.ID)
			_, _ = conn.Write([]byte(resp + "\n"))
		}
	}()

	client := NewTCPClient(listener.Addr().String(), WithNotificationHandler(func(method string, params json.RawMessage) {
		if method == "events.tick" {
			notifications <- string(params)
		}
	}))
	defer client.Close()

	resp, err := client.Call[echoRequest, *echoResponse](context.Background(), "echo", echoRequest{Name: "x"})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if resp.Message != "hello" {
		t.Fatalf("resp = %+v", resp)
	}

	select {
	case params := <-notifications:
		if !strings.Contains(params, `"n":1`) {
			t.Fatalf("params = %s", params)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("notification handler was not invoked")
	}
}

// TestClientTCPReconnectsAfterBreak verifies that a failed connection is
// re-dialled on the next call.
func TestClientTCPReconnectsAfterBreak(t *testing.T) {
	d := newEchoDispatcher()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serve := func(ln net.Listener) {
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				go func(conn net.Conn) {
					defer conn.Close()
					reader := bufio.NewReader(conn)
					for {
						line, err := reader.ReadBytes('\n')
						if err != nil {
							return
						}
						resp, ok := d.Serve(context.Background(), line)
						if !ok {
							continue
						}
						_, _ = conn.Write(append(resp, '\n'))
					}
				}(conn)
			}
		}()
	}
	serve(listener)

	client := NewTCPClient(listener.Addr().String())
	if _, err := client.Call[echoRequest, *echoResponse](context.Background(), "echo", echoRequest{Name: "one"}); err != nil {
		t.Fatalf("first call: %v", err)
	}

	// Killing the server breaks the connection; a new listener on the same
	// port makes the next call succeed again.
	_ = listener.Close()
	listener, err = net.Listen("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("re-listen: %v", err)
	}
	defer listener.Close()
	serve(listener)

	resp, err := client.Call[echoRequest, *echoResponse](context.Background(), "echo", echoRequest{Name: "two"})
	if err != nil {
		t.Fatalf("call after reconnect: %v", err)
	}
	if resp.Message != "hello two" {
		t.Fatalf("resp = %+v", resp)
	}
	_ = client.Close()
}

// TestClientHTTPMapsTransportErrors verifies that an error body produced by
// the runtime's error mapping (a non-200 status with a {"error":{"code":...}}
// body) surfaces as a typed apierror the caller can branch on.
func TestClientHTTPMapsTransportErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"code":"unauthenticated","message":"missing API key","requestId":"r1"}}`))
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL)
	defer client.Close()

	_, err := client.Call[struct{}, any](context.Background(), "echo", struct{}{})
	if err == nil {
		t.Fatal("expected an error")
	}
	if apierror.KindOf(err) != apierror.KindUnauthenticated {
		t.Fatalf("err = %v, want kind unauthenticated", err)
	}
	if msg := apierror.ClientMessage(err); msg != "missing API key" {
		t.Fatalf("message = %q", msg)
	}
}

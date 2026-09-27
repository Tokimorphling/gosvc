package jsonrpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"example.com/gosvc/apierror"
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

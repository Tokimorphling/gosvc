package tasks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/Tokimorphling/gosvc/apierror"
	"github.com/Tokimorphling/gosvc/jsonrpc"
)

func TestRESTAndJSONRPCShareTasksAndAuthentication(t *testing.T) {
	cfg := &Config{}
	cfg.SetDefaults()
	cfg.HTTP.Port, cfg.GRPC.Port, cfg.Admin.Port = 0, 0, 0
	cfg.Auth.Enabled, cfg.Auth.APIKeys = true, []string{"sample-key"}
	cfg.Log.Level = "error"
	application, err := Build(cfg)
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
			t.Error("sample failed to shut down")
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
			t.Fatal("sample failed to start")
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
			req.Header.Set("X-API-Key", "sample-key")
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
	request("GET", "/api/v1/tasks", "", false, 401)
	request("POST", "/api/v1/tasks", `{"title":""}`, true, 400)
	request("POST", "/api/v1/tasks", `{`, true, 400)
	var task Task
	if err := json.Unmarshal(request("POST", "/api/v1/tasks", `{"title":"ship"}`, true, 201), &task); err != nil {
		t.Fatal(err)
	}
	rpc := jsonrpc.NewHTTPClient(base, jsonrpc.WithHTTPClient(client), jsonrpc.WithHeader("X-API-Key", "sample-key"))
	defer rpc.Close()
	completed, err := rpc.Call[IDRequest, Task](t.Context(), "tasks.complete", IDRequest{ID: task.ID})
	if err != nil || !completed.Done {
		t.Fatalf("complete=%+v, %v", completed, err)
	}
	var viaREST Task
	path := "/api/v1/tasks/" + strconv.FormatInt(task.ID, 10)
	if err := json.Unmarshal(request("GET", path, "", true, 200), &viaREST); err != nil {
		t.Fatal(err)
	}
	if viaREST != completed {
		t.Fatalf("REST=%+v RPC=%+v", viaREST, completed)
	}
	items, err := rpc.Call[struct{}, []Task](t.Context(), "tasks.list", struct{}{})
	if err != nil || len(items) != 1 {
		t.Fatalf("list=%+v, %v", items, err)
	}
	request("GET", "/api/v1/tasks/invalid", "", true, 400)
	request("DELETE", path, "", true, 200)
	request("GET", path, "", true, 404)
	_, err = rpc.Call[IDRequest, Task](t.Context(), "tasks.get", IDRequest{ID: task.ID})
	if rpcErr, ok := errors.AsType[*jsonrpc.Error](err); !ok || rpcErr.Code != jsonrpc.CodeNotFound {
		t.Fatalf("get deleted via RPC=%v", err)
	}
	unauthenticated := jsonrpc.NewHTTPClient(base, jsonrpc.WithHTTPClient(client))
	defer unauthenticated.Close()
	_, err = unauthenticated.Call[struct{}, []Task](t.Context(), "tasks.list", struct{}{})
	if apierror.KindOf(err) != apierror.KindUnauthenticated {
		t.Fatalf("unauthenticated RPC=%v", err)
	}
	request("GET", "/readyz", "", false, 200)
}

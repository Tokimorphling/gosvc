package jsonrpc

import (
	"context"
	"encoding/json"
	"testing"

	"example.com/gosvc/apierror"
)

type testResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func newTestDispatcher() *Dispatcher {
	d := NewDispatcher()
	d.Register("echo", func(_ context.Context, params json.RawMessage) (any, error) {
		var req struct {
			Name string `json:"name"`
		}
		if err := DecodeParams(params, &req); err != nil {
			return nil, err
		}
		return map[string]string{"name": req.Name}, nil
	})
	d.Register("fail", func(_ context.Context, _ json.RawMessage) (any, error) {
		return nil, apierror.New(apierror.KindNotFound, "nope")
	})
	d.Register("boom", func(_ context.Context, _ json.RawMessage) (any, error) {
		return nil, apierror.New(apierror.KindInternal, "database exploded")
	})
	return d
}

func decodeResponse(t *testing.T, raw []byte) testResponse {
	t.Helper()
	var resp testResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("unmarshal response %s: %v", raw, err)
	}
	return resp
}

func TestServeSingleSuccess(t *testing.T) {
	d := newTestDispatcher()
	raw, ok := d.Serve(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"echo","params":{"name":"x"}}`))
	if !ok {
		t.Fatal("expected a response body")
	}

	resp := decodeResponse(t, raw)
	if resp.JSONRPC != "2.0" {
		t.Fatalf("jsonrpc = %q", resp.JSONRPC)
	}
	if string(resp.ID) != "1" {
		t.Fatalf("id = %s", resp.ID)
	}
	if resp.Error != nil {
		t.Fatalf("unexpected error: %+v", resp.Error)
	}
	var result struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		t.Fatal(err)
	}
	if result.Name != "x" {
		t.Fatalf("result = %+v", result)
	}
}

func TestServeParseError(t *testing.T) {
	d := newTestDispatcher()
	raw, ok := d.Serve(context.Background(), []byte(`{`))
	if !ok {
		t.Fatal("parse errors must produce a response")
	}
	if resp := decodeResponse(t, raw); resp.Error == nil || resp.Error.Code != CodeParseError {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestServeInvalidRequest(t *testing.T) {
	d := newTestDispatcher()

	for _, body := range []string{
		`{"jsonrpc":"1.0","id":1,"method":"echo"}`,
		`{"jsonrpc":"2.0","id":1}`,
	} {
		raw, ok := d.Serve(context.Background(), []byte(body))
		if !ok {
			t.Fatalf("%s: expected a response", body)
		}
		if resp := decodeResponse(t, raw); resp.Error == nil || resp.Error.Code != CodeInvalidRequest {
			t.Fatalf("%s: resp = %+v", body, resp)
		}
	}
}

func TestServeMethodNotFound(t *testing.T) {
	d := newTestDispatcher()
	raw, _ := d.Serve(context.Background(), []byte(`{"jsonrpc":"2.0","id":7,"method":"nope"}`))
	resp := decodeResponse(t, raw)
	if resp.Error == nil || resp.Error.Code != CodeMethodNotFound {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestServeNotificationProducesNoResponse(t *testing.T) {
	d := newTestDispatcher()
	raw, ok := d.Serve(context.Background(), []byte(`{"jsonrpc":"2.0","method":"echo","params":{"name":"x"}}`))
	if ok || raw != nil {
		t.Fatalf("notifications must not produce a response, got ok=%v raw=%s", ok, raw)
	}
}

func TestServeBatchSkipsNotifications(t *testing.T) {
	d := newTestDispatcher()
	body := `[
		{"jsonrpc":"2.0","id":2,"method":"fail"},
		{"jsonrpc":"2.0","method":"echo","params":{"name":"ignored"}}
	]`
	raw, ok := d.Serve(context.Background(), []byte(body))
	if !ok {
		t.Fatal("expected a batch response")
	}

	var responses []testResponse
	if err := json.Unmarshal(raw, &responses); err != nil {
		t.Fatal(err)
	}
	if len(responses) != 1 {
		t.Fatalf("got %d responses, want 1", len(responses))
	}
	if responses[0].Error == nil || responses[0].Error.Code != CodeNotFound {
		t.Fatalf("resp = %+v", responses[0])
	}
}

func TestServeEmptyBatch(t *testing.T) {
	d := newTestDispatcher()
	raw, _ := d.Serve(context.Background(), []byte(`[]`))
	if resp := decodeResponse(t, raw); resp.Error == nil || resp.Error.Code != CodeInvalidRequest {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestInternalErrorsAreNotLeaked(t *testing.T) {
	d := newTestDispatcher()
	raw, _ := d.Serve(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"boom"}`))
	resp := decodeResponse(t, raw)
	if resp.Error == nil || resp.Error.Code != CodeInternal {
		t.Fatalf("resp = %+v", resp)
	}
	if resp.Error.Message != "internal server error" {
		t.Fatalf("internal message leaked: %q", resp.Error.Message)
	}
}

func TestDecodeParamsRejectsPositional(t *testing.T) {
	d := newTestDispatcher()
	raw, _ := d.Serve(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"echo","params":["x"]}`))
	if resp := decodeResponse(t, raw); resp.Error == nil || resp.Error.Code != CodeInvalidParams {
		t.Fatalf("resp = %+v", resp)
	}
}

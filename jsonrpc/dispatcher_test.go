package jsonrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Tokimorphling/gosvc/apierror"
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

func TestServeInvalidObjectsReturnInvalidRequest(t *testing.T) {
	d := newTestDispatcher()
	for _, body := range []string{
		`{"foo":"boo"}`,
		`{"jsonrpc":"2.0"}`,
		`{"jsonrpc":"1.0","method":"echo"}`,
		`{"jsonrpc":"2.0","method":42}`,
		`{"jsonrpc":"2.0","method":"echo","params":null}`,
		`{"jsonrpc":"2.0","method":"echo","id":true}`,
		`{"jsonrpc":"2.0","method":"echo","id":{}}`,
		`{"jsonrpc":"2.0","method":"echo","id":[]}`,
		`null`,
		`42`,
	} {
		raw, ok := d.Serve(context.Background(), []byte(body))
		if !ok {
			t.Fatalf("%s: invalid request was treated as a notification", body)
		}
		resp := decodeResponse(t, raw)
		if resp.Error == nil || resp.Error.Code != CodeInvalidRequest || string(resp.ID) != "null" {
			t.Fatalf("%s: resp = %+v", body, resp)
		}
	}
}

func TestServeExplicitNullIDReceivesResponse(t *testing.T) {
	d := newTestDispatcher()
	for _, body := range []string{
		`{"jsonrpc":"2.0","method":"echo","id":null}`,
		`{"jsonrpc":"2.0","method":"missing","id":null}`,
	} {
		raw, ok := d.Serve(context.Background(), []byte(body))
		if !ok {
			t.Fatalf("%s: explicit null id was treated as a notification", body)
		}
		resp := decodeResponse(t, raw)
		if string(resp.ID) != "null" {
			t.Fatalf("%s: id = %s", body, resp.ID)
		}
	}
	if (&Request{ID: json.RawMessage("null")}).IsNotification() {
		t.Fatal("explicit null id must not be a notification")
	}
	raw, ok := d.Serve(context.Background(), []byte(`{"jsonrpc":"2.0","method":"echo","id":"request-1"}`))
	if !ok || string(decodeResponse(t, raw).ID) != `"request-1"` {
		t.Fatalf("string id response = %s, ok = %v", raw, ok)
	}
}

func TestServeBatchInvalidElementsAndNotifications(t *testing.T) {
	d := NewDispatcher()
	called := 0
	d.Register("record", func(context.Context, json.RawMessage) (any, error) {
		called++
		return "ok", nil
	})
	body := `[
		{"jsonrpc":"2.0","method":"record"},
		{"foo":"boo"},
		null,
		42,
		{"jsonrpc":"2.0","method":"record","id":true},
		{"jsonrpc":"2.0","method":"record","id":null},
		{"jsonrpc":"2.0","method":"record","id":2}
	]`
	raw, ok := d.Serve(context.Background(), []byte(body))
	if !ok {
		t.Fatal("invalid batch elements and requests with ids must receive responses")
	}
	var responses []testResponse
	if err := json.Unmarshal(raw, &responses); err != nil {
		t.Fatalf("unmarshal batch response %s: %v", raw, err)
	}
	if len(responses) != 6 {
		t.Fatalf("got %d responses, want 6: %s", len(responses), raw)
	}
	for i := range 4 {
		if responses[i].Error == nil || responses[i].Error.Code != CodeInvalidRequest || string(responses[i].ID) != "null" {
			t.Fatalf("response %d = %+v, want Invalid Request with null id", i, responses[i])
		}
	}
	if responses[4].Error != nil || string(responses[4].ID) != "null" {
		t.Fatalf("explicit null id response = %+v", responses[4])
	}
	if responses[5].Error != nil || string(responses[5].ID) != "2" {
		t.Fatalf("numeric id response = %+v", responses[5])
	}
	if called != 3 {
		t.Fatalf("handler called %d times, want notification and two valid requests", called)
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

func TestObserverBoundsUnknownMethodNames(t *testing.T) {
	d := newTestDispatcher()
	type observation struct {
		method string
		code   int
	}
	var got []observation
	d.SetObserver(func(method string, code int, _ time.Duration) {
		got = append(got, observation{method, code})
	})

	d.Serve(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"client.chosen.1"}`))
	d.Serve(context.Background(), []byte(`{"jsonrpc":"2.0","method":"client.chosen.2"}`))
	d.Serve(context.Background(), []byte(`{"jsonrpc":"2.0","method":"echo","id":true}`))
	d.Handle(context.Background(), &Request{JSONRPC: "2.0", Method: "client.chosen.direct", ID: json.RawMessage("1")})
	_, _ = d.Invoke(context.Background(), "client.chosen.3", nil)
	_, _ = d.Invoke(context.Background(), "echo", nil)

	want := []observation{
		{UnknownMethodLabel, CodeMethodNotFound},
		{UnknownMethodLabel, CodeMethodNotFound},
		{UnknownMethodLabel, CodeInvalidRequest},
		{UnknownMethodLabel, CodeMethodNotFound},
		{UnknownMethodLabel, CodeMethodNotFound},
		{"echo", 0},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("observations = %+v, want %+v", got, want)
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

func TestBatchIsCapped(t *testing.T) {
	d := newTestDispatcher()

	request := func(id int) string {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"echo","params":{"name":"x"}}`, id)
	}

	// The default cap rejects a batch that is one item larger.
	over := make([]string, 0, defaultMaxBatch+1)
	for i := range defaultMaxBatch + 1 {
		over = append(over, request(i))
	}
	raw, _ := d.Serve(context.Background(), []byte("["+strings.Join(over, ",")+"]"))
	if resp := decodeResponse(t, raw); resp.Error == nil || resp.Error.Code != CodeInvalidRequest {
		t.Fatalf("oversized batch must be rejected: %+v", resp)
	}

	// A batch at the cap still works.
	at := make([]string, 0, defaultMaxBatch)
	for i := range defaultMaxBatch {
		at = append(at, request(i))
	}
	raw, _ = d.Serve(context.Background(), []byte("["+strings.Join(at, ",")+"]"))
	var responses []testResponse
	if err := json.Unmarshal(raw, &responses); err != nil {
		t.Fatal(err)
	}
	if len(responses) != defaultMaxBatch {
		t.Fatalf("got %d responses, want %d", len(responses), defaultMaxBatch)
	}

	// The cap is adjustable.
	d.SetMaxBatch(2)
	raw, _ = d.Serve(context.Background(), []byte("["+request(1)+","+request(2)+","+request(3)+"]"))
	if resp := decodeResponse(t, raw); resp.Error == nil || resp.Error.Code != CodeInvalidRequest {
		t.Fatalf("custom cap must reject the batch: %+v", resp)
	}
	raw, _ = d.Serve(context.Background(), []byte("["+request(1)+","+request(2)+"]"))
	if err := json.Unmarshal(raw, &responses); err != nil {
		t.Fatal(err)
	}
	if len(responses) != 2 {
		t.Fatalf("got %d responses, want 2", len(responses))
	}
}

func TestTryRegisterReportsDuplicates(t *testing.T) {
	d := NewDispatcher()
	handler := func(context.Context, json.RawMessage) (any, error) { return nil, nil }

	if err := d.TryRegister("once", handler); err != nil {
		t.Fatalf("first registration must succeed: %v", err)
	}
	if err := d.TryRegister("once", handler); !errors.Is(err, ErrDuplicateMethod) {
		t.Fatalf("duplicate must be reported, got %v", err)
	}
	if len(d.Methods()) != 1 {
		t.Fatalf("the duplicate must not overwrite: %v", d.Methods())
	}
}

// TestMiddlewareShortCircuit verifies UseFor with an authorisation-shaped
// middleware: the wrapped method is rejected without invoking its handler,
// while other methods are unaffected.
func TestMiddlewareShortCircuit(t *testing.T) {
	d := NewDispatcher()
	invoked := map[string]bool{}

	d.Register("restricted", func(_ context.Context, _ json.RawMessage) (any, error) {
		invoked["restricted"] = true
		return "secret", nil
	})
	d.Register("open", func(_ context.Context, _ json.RawMessage) (any, error) {
		invoked["open"] = true
		return "fine", nil
	})
	d.UseFor("restricted", func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, params json.RawMessage) (any, error) {
			if v := ctx.Value(authedKey{}); v == nil {
				return nil, apierror.New(apierror.KindPermissionDenied, "authorize first")
			}
			return next(ctx, params)
		}
	})

	raw, _ := d.Serve(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"restricted"}`))
	if resp := decodeResponse(t, raw); resp.Error == nil || resp.Error.Code != CodePermissionDenied {
		t.Fatalf("restricted must be denied: %+v", resp)
	}
	if invoked["restricted"] {
		t.Fatal("the handler must not run when the middleware short-circuits")
	}

	raw, _ = d.Serve(context.WithValue(context.Background(), authedKey{}, true),
		[]byte(`{"jsonrpc":"2.0","id":2,"method":"restricted"}`))
	if resp := decodeResponse(t, raw); resp.Error != nil {
		t.Fatalf("authorized call must pass: %+v", resp)
	}
	if !invoked["restricted"] {
		t.Fatal("the handler must run after the middleware passes")
	}

	// Other methods do not run the method-specific middleware.
	raw, _ = d.Serve(context.Background(), []byte(`{"jsonrpc":"2.0","id":3,"method":"open"}`))
	if resp := decodeResponse(t, raw); resp.Error != nil || !invoked["open"] {
		t.Fatalf("UseFor must not affect other methods: %+v", resp)
	}
}

type authedKey struct{}

// TestMiddlewareOrderAndContext verifies that Use middlewares run in the
// order they were registered (first is outermost) and that ctx survives the
// whole chain.
func TestMiddlewareOrderAndContext(t *testing.T) {
	d := NewDispatcher()
	var order []string

	d.Register("echo", func(ctx context.Context, _ json.RawMessage) (any, error) {
		if ctx.Value(traceKey{}) == nil {
			return nil, errors.New("context lost in middleware chain")
		}
		order = append(order, "handler")
		return "ok", nil
	})
	for _, name := range []string{"first", "second", "third"} {
		d.Use(func(next HandlerFunc) HandlerFunc {
			return func(ctx context.Context, params json.RawMessage) (any, error) {
				order = append(order, name)
				return next(context.WithValue(ctx, traceKey{}, name), params)
			}
		})
	}

	raw, _ := d.Serve(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"echo"}`))
	if resp := decodeResponse(t, raw); resp.Error != nil {
		t.Fatalf("resp = %+v", resp)
	}
	want := []string{"first", "second", "third", "handler"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
}

type traceKey struct{}

// TestMiddlewareAppliesToInvoke verifies the codec entry point runs the same
// middleware chain as the envelope path: without the required context value
// the middleware rejects, with it the call passes and params arrive verbatim.
func TestMiddlewareAppliesToInvoke(t *testing.T) {
	d := NewDispatcher()
	seen := ""
	d.Register("probe", func(_ context.Context, params json.RawMessage) (any, error) {
		seen = string(params)
		return "done", nil
	})
	d.Use(func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, params json.RawMessage) (any, error) {
			if ctx.Value(traceKey{}) == nil {
				return nil, apierror.New(apierror.KindPermissionDenied, "ctx missing")
			}
			return next(ctx, params)
		}
	})

	// Without the required context value the middleware short-circuits Invoke.
	if _, err := d.Invoke(context.Background(), "probe", json.RawMessage(`"raw"`)); apierror.KindOf(err) != apierror.KindPermissionDenied {
		t.Fatalf("err = %v, want the middleware to run on Invoke", err)
	}

	// With it, params pass through verbatim (no params decoding on this path).
	if _, err := d.Invoke(context.WithValue(context.Background(), traceKey{}, "x"), "probe", json.RawMessage(`"raw"`)); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if seen != `"raw"` {
		t.Fatalf("params = %s, want verbatim pass-through", seen)
	}

	if _, err := d.Invoke(context.Background(), "missing", nil); !errors.Is(err, ErrMethodNotFound) {
		t.Fatalf("err = %v, want ErrMethodNotFound", err)
	}
}

// TestMiddlewareLateRegistration verifies that middleware registered after
// methods applies to the existing handlers too (chains are rebuilt).
func TestMiddlewareLateRegistration(t *testing.T) {
	d := NewDispatcher()
	d.Register("echo", func(context.Context, json.RawMessage) (any, error) { return "ok", nil })

	d.Use(func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, params json.RawMessage) (any, error) {
			return "blocked", nil
		}
	})

	raw, _ := d.Serve(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"echo"}`))
	var resp struct {
		Result string `json:"result"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Result != "blocked" {
		t.Fatalf("result = %q, want the middleware to take effect after registration", resp.Result)
	}
}

// TestSetObserverIfAbsent verifies that a default observer can be wired into
// an existing dispatcher without overriding an observer that was deliberately
// installed first.
func TestSetObserverIfAbsent(t *testing.T) {
	d := NewDispatcher()
	d.Register("echo", func(context.Context, json.RawMessage) (any, error) { return "ok", nil })

	var customSeen bool
	d.SetObserver(func(string, int, time.Duration) { customSeen = true })
	if d.SetObserverIfAbsent(func(string, int, time.Duration) { t.Fatal("the absent-install overrode a set observer") }) {
		t.Fatal("SetObserverIfAbsent reported installation over an existing observer")
	}
	_, ok := d.Serve(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"echo"}`))
	if !ok {
		t.Fatal("expected a response body")
	}
	if !customSeen {
		t.Fatal("the custom observer was not called")
	}

	empty := NewDispatcher()
	if !empty.SetObserverIfAbsent(func(string, int, time.Duration) {}) {
		t.Fatal("SetObserverIfAbsent must install when no observer is set")
	}
}

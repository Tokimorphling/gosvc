package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/bytedance/sonic"

	"example.com/gosvc/internal/apierror"
)

// Observer is called once per handled method for metrics and tracing.
type Observer func(method string, code int, d time.Duration)

// Dispatcher routes JSON-RPC methods to handlers.
type Dispatcher struct {
	mu       sync.RWMutex
	methods  map[string]HandlerFunc
	observer Observer
}

// NewDispatcher creates an empty dispatcher.
func NewDispatcher() *Dispatcher {
	return &Dispatcher{methods: make(map[string]HandlerFunc)}
}

// Register binds a method name to a handler. It panics on duplicates to catch
// wiring mistakes at startup.
func (d *Dispatcher) Register(method string, handler HandlerFunc) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.methods[method]; exists {
		panic("jsonrpc: duplicate method " + method)
	}
	d.methods[method] = handler
}

// SetObserver installs a metrics observer. Call it before serving traffic.
func (d *Dispatcher) SetObserver(observer Observer) { d.observer = observer }

// Methods returns the registered method names in sorted order.
func (d *Dispatcher) Methods() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]string, 0, len(d.methods))
	for method := range d.methods {
		out = append(out, method)
	}
	sort.Strings(out)
	return out
}

// Handle processes a single request and returns its response.
func (d *Dispatcher) Handle(ctx context.Context, req *Request) *Response {
	start := time.Now()
	resp := d.handle(ctx, req)
	if d.observer != nil {
		method := ""
		if req != nil {
			method = req.Method
		}
		code := 0
		if resp != nil && resp.Error != nil {
			code = resp.Error.Code
		}
		d.observer(method, code, time.Since(start))
	}
	return resp
}

func (d *Dispatcher) handle(ctx context.Context, req *Request) *Response {
	if req == nil {
		return errorResponse(nil, CodeInvalidRequest, "invalid request")
	}
	if req.JSONRPC != "2.0" {
		return errorResponse(req.ID, CodeInvalidRequest, `jsonrpc must be "2.0"`)
	}
	if req.Method == "" {
		return errorResponse(req.ID, CodeInvalidRequest, "method must not be empty")
	}

	d.mu.RLock()
	handler := d.methods[req.Method]
	d.mu.RUnlock()

	if handler == nil {
		return errorResponse(req.ID, CodeMethodNotFound, "method not found: "+req.Method)
	}

	result, err := handler(ctx, req.Params)
	if err != nil {
		code, message := codeOf(err)
		return errorResponse(req.ID, code, message)
	}
	return &Response{JSONRPC: "2.0", ID: normalizeID(req.ID), Result: result}
}

// Serve handles one request body, which may be a single request or a batch.
// ok reports whether a response body must be sent: it is false when the body
// contained only notifications (HTTP 204).
func (d *Dispatcher) Serve(ctx context.Context, body []byte) (resp []byte, ok bool) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return d.marshal(errorResponse(nil, CodeParseError, "empty request body")), true
	}

	if body[0] == '[' {
		return d.serveBatch(ctx, body)
	}

	var req Request
	if err := sonic.Unmarshal(body, &req); err != nil {
		return d.marshal(errorResponse(nil, CodeParseError, "parse error")), true
	}
	if req.IsNotification() {
		d.Handle(ctx, &req)
		return nil, false
	}
	return d.marshal(d.Handle(ctx, &req)), true
}

func (d *Dispatcher) serveBatch(ctx context.Context, body []byte) ([]byte, bool) {
	var reqs []*Request
	if err := sonic.Unmarshal(body, &reqs); err != nil {
		return d.marshal(errorResponse(nil, CodeParseError, "parse error")), true
	}
	if len(reqs) == 0 {
		return d.marshal(errorResponse(nil, CodeInvalidRequest, "empty batch")), true
	}

	responses := make([]*Response, 0, len(reqs))
	for _, req := range reqs {
		if req == nil {
			responses = append(responses, errorResponse(nil, CodeInvalidRequest, "invalid request"))
			continue
		}
		if req.IsNotification() {
			d.Handle(ctx, req)
			continue
		}
		responses = append(responses, d.Handle(ctx, req))
	}
	if len(responses) == 0 {
		return nil, false
	}
	return d.marshal(responses), true
}

func (d *Dispatcher) marshal(v any) []byte {
	raw, err := sonic.Marshal(v)
	if err != nil {
		raw, _ = sonic.Marshal(errorResponse(nil, CodeInternal, "failed to encode response"))
	}
	return raw
}

func errorResponse(id json.RawMessage, code int, message string) *Response {
	return &Response{
		JSONRPC: "2.0",
		ID:      normalizeID(id),
		Error:   &Error{Code: code, Message: message},
	}
}

// DecodeParams decodes named (object) params into v. Positional params are
// rejected with an invalid-params error; absent params leave v untouched.
func DecodeParams(params json.RawMessage, v any) error {
	if len(params) == 0 || string(params) == "null" {
		return nil
	}
	if params[0] == '[' {
		return apierror.New(apierror.KindInvalidArgument, "positional params are not supported; use named params")
	}
	if err := sonic.Unmarshal(params, v); err != nil {
		return apierror.Wrap(err, apierror.KindInvalidArgument, "invalid params")
	}
	return nil
}

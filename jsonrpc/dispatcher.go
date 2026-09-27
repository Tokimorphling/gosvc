package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/bytedance/sonic"

	"github.com/Tokimorphling/gosvc/apierror"
)

// defaultMaxBatch caps how many requests one batch payload may carry. JSON-RPC
// allows servers to reject oversized batches, and without a cap a single
// request body could occupy a worker for an unbounded number of invocations.
const defaultMaxBatch = 128

// ErrDuplicateMethod is returned by TryRegister when the method name is
// already bound.
var ErrDuplicateMethod = errors.New("jsonrpc: duplicate method")

// Observer is called once per handled method for metrics and tracing.
type Observer func(method string, code int, d time.Duration)

// Dispatcher routes JSON-RPC methods to handlers.
type Dispatcher struct {
	mu       sync.RWMutex
	methods  map[string]HandlerFunc
	observer Observer
	maxBatch int
}

// NewDispatcher creates an empty dispatcher.
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		methods:  make(map[string]HandlerFunc),
		maxBatch: defaultMaxBatch,
	}
}

// Register binds a method name to a handler. It panics on duplicates to catch
// wiring mistakes at startup. Use TryRegister when registering at runtime,
// where a duplicate must not take the process down.
func (d *Dispatcher) Register(method string, handler HandlerFunc) {
	if err := d.TryRegister(method, handler); err != nil {
		panic(err)
	}
}

// TryRegister binds a method name to a handler and reports whether the name
// was still free, making it safe to call at runtime.
func (d *Dispatcher) TryRegister(method string, handler HandlerFunc) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.methods[method]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateMethod, method)
	}
	d.methods[method] = handler
	return nil
}

// RegisterTyped binds a typed handler: Req is decoded from the JSON-RPC params
// and Resp becomes the result. The type parameters are inferred from handler,
// so business methods stay typed end to end:
//
//	d.RegisterTyped("greeter.sayHello", svc.SayHello)
//	// func(context.Context, greeter.HelloRequest) (*greeter.HelloResponse, error)
func (d *Dispatcher) RegisterTyped[Req, Resp any](method string, handler func(ctx context.Context, req Req) (Resp, error)) {
	d.Register(method, func(ctx context.Context, params json.RawMessage) (any, error) {
		var req Req
		if err := DecodeParams(params, &req); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	})
}

// SetObserver installs a metrics observer. It is safe to call while requests
// are being served.
func (d *Dispatcher) SetObserver(observer Observer) {
	d.mu.Lock()
	d.observer = observer
	d.mu.Unlock()
}

// SetMaxBatch caps the number of requests accepted in one batch payload.
// Values <= 0 restore the default. The cap rejects the whole batch with an
// InvalidRequest error, as the JSON-RPC specification permits.
func (d *Dispatcher) SetMaxBatch(n int) {
	if n <= 0 {
		n = defaultMaxBatch
	}
	d.mu.Lock()
	d.maxBatch = n
	d.mu.Unlock()
}

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
	resp, observer := d.handle(ctx, req)
	if observer != nil {
		method := ""
		if req != nil {
			method = req.Method
		}
		code := 0
		if resp != nil && resp.Error != nil {
			code = resp.Error.Code
		}
		observer(method, code, time.Since(start))
	}
	return resp
}

func (d *Dispatcher) handle(ctx context.Context, req *Request) (*Response, Observer) {
	// Read the observer and handler under one lock so SetObserver can be
	// called at any time without racing Handle.
	d.mu.RLock()
	observer := d.observer
	var handler HandlerFunc
	if req != nil {
		handler = d.methods[req.Method]
	}
	d.mu.RUnlock()

	if req == nil {
		return errorResponse(nil, CodeInvalidRequest, "invalid request"), observer
	}
	if req.JSONRPC != "2.0" {
		return errorResponse(req.ID, CodeInvalidRequest, `jsonrpc must be "2.0"`), observer
	}
	if req.Method == "" {
		return errorResponse(req.ID, CodeInvalidRequest, "method must not be empty"), observer
	}

	if handler == nil {
		return errorResponse(req.ID, CodeMethodNotFound, "method not found: "+req.Method), observer
	}

	result, err := handler(ctx, req.Params)
	if err != nil {
		code, message := codeOf(err)
		return errorResponse(req.ID, code, message), observer
	}
	return &Response{JSONRPC: "2.0", ID: normalizeID(req.ID), Result: result}, observer
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

	d.mu.RLock()
	maxBatch := d.maxBatch
	d.mu.RUnlock()
	if len(reqs) > maxBatch {
		return d.marshal(errorResponse(nil, CodeInvalidRequest,
			fmt.Sprintf("batch too large: %d requests exceed the limit of %d", len(reqs), maxBatch))), true
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

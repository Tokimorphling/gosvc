package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bytedance/sonic"

	"github.com/Tokimorphling/gosvc/apierror"
)

// defaultMaxBatch caps how many requests one batch payload may carry. JSON-RPC
// allows servers to reject oversized batches, and without a cap a single
// request body could occupy a worker for an unbounded number of invocations.
const defaultMaxBatch = 128

// UnknownMethodLabel is passed to observers for invalid requests and methods
// that are not registered. Client supplied method names must not create an
// unbounded number of metric label values.
const UnknownMethodLabel = "_unknown"

// ErrDuplicateMethod is returned by TryRegister when the method name is
// already bound.
var ErrDuplicateMethod = errors.New("jsonrpc: duplicate method")

// ErrMethodNotFound is returned by Invoke when the method is not registered.
// Envelope-level entry points (Handle/Serve) map it onto the JSON-RPC
// not-found code themselves; a codec renders its own error shape.
var ErrMethodNotFound = errors.New("jsonrpc: method not found")

// Observer is called once per handled method for metrics and tracing. Its
// method is UnknownMethodLabel for invalid or unregistered requests.
// It must be concurrency safe; calls retain the observer from their table snapshot.
type Observer func(method string, code int, d time.Duration)

// Middleware wraps the invocation of one method. It may short-circuit (return
// without calling next), rewrite params, or replace the result, and it runs
// inside the transport's recover and after ctx enrichment (request id, trace),
// so panics become internal errors and log fields are available.
//
// Middleware applies to every entry point that dispatches a method — Handle,
// Serve (HTTP /rpc and TCP default path) and Invoke (custom codecs) — so one
// hook covers all transports that share the Dispatcher. params is passed
// through verbatim: it is a JSON-RPC params object on the envelope paths and
// whatever the codec produced on the Invoke path, so middlewares must not
// assume named parameters.
type Middleware func(next HandlerFunc) HandlerFunc

// Handle processes a single request and returns its response.
func (d *Dispatcher) Handle(ctx context.Context, req *Request) *Response {
	table := d.load()
	var start time.Time
	if table.observer != nil {
		start = time.Now()
	}
	resp, metricMethod := table.handle(ctx, req)
	observer := table.observer
	if observer != nil {
		code := 0
		if resp != nil && resp.Error != nil {
			code = resp.Error.Code
		}
		observer(metricMethod, code, time.Since(start))
	}
	return resp
}

func (t *dispatchTable) handle(ctx context.Context, req *Request) (*Response, string) {
	if !validRequest(req) {
		return errorResponse(nil, CodeInvalidRequest, "invalid request"), UnknownMethodLabel
	}

	handler := t.methods[req.Method].handler()
	if handler == nil {
		return errorResponse(req.ID, CodeMethodNotFound, "method not found: "+req.Method), UnknownMethodLabel
	}

	result, err := handler(ctx, req.Params)
	if err != nil {
		code, message := codeOf(err)
		return errorResponse(req.ID, code, message), req.Method
	}
	return &Response{JSONRPC: "2.0", ID: normalizeID(req.ID), Result: result}, req.Method
}

// Invoke dispatches a method without the JSON-RPC 2.0 envelope: no
// "jsonrpc"=="2.0" validation, no params decoding and no batching. It is the
// entry point for custom transport codecs (see transport/tcp Codec), which own
// the wire shape and use Invoke to reach the same method table, middleware and
// observer.
//
// When the method is not registered, Invoke returns a wrapped ErrMethodNotFound
// that the codec maps onto its own error shape; the observer is called with the
// JSON-RPC not-found code so metrics stay comparable across entry points.
func (d *Dispatcher) Invoke(ctx context.Context, method string, params json.RawMessage) (any, error) {
	table := d.load()
	observer := table.observer
	var start time.Time
	if observer != nil {
		start = time.Now()
	}
	handler := table.methods[method].handler()
	if handler == nil {
		if observer != nil {
			observer(UnknownMethodLabel, CodeMethodNotFound, time.Since(start))
		}
		return nil, fmt.Errorf("%w: %s", ErrMethodNotFound, method)
	}
	result, err := handler(ctx, params)
	if observer != nil {
		code := 0
		if err != nil {
			code, _ = codeOf(err)
		}
		observer(method, code, time.Since(start))
	}
	return result, err
}

// Serve handles one request body, which may be a single request or a batch.
// ok reports whether a response body must be sent: it is false when the body
// contained only notifications (HTTP 204).
func (d *Dispatcher) Serve(ctx context.Context, body []byte) (resp []byte, ok bool) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return d.marshal(errorResponse(nil, CodeParseError, "empty request body")), true
	}
	if !json.Valid(body) {
		return d.marshal(errorResponse(nil, CodeParseError, "parse error")), true
	}

	if body[0] == '[' {
		return d.serveBatch(ctx, body)
	}

	req, valid := parseRequest(body)
	if !valid {
		return d.marshal(d.Handle(ctx, nil)), true
	}
	response := d.Handle(ctx, req)
	if req.IsNotification() {
		return nil, false
	}
	return d.marshal(response), true
}

func (d *Dispatcher) serveBatch(ctx context.Context, body []byte) ([]byte, bool) {
	var reqs []json.RawMessage
	if err := sonic.Unmarshal(body, &reqs); err != nil {
		return d.marshal(errorResponse(nil, CodeParseError, "parse error")), true
	}
	if len(reqs) == 0 {
		return d.marshal(errorResponse(nil, CodeInvalidRequest, "empty batch")), true
	}

	maxBatch := d.load().maxBatch
	if len(reqs) > maxBatch {
		return d.marshal(errorResponse(nil, CodeInvalidRequest,
			fmt.Sprintf("batch too large: %d requests exceed the limit of %d", len(reqs), maxBatch))), true
	}

	responses := make([]*Response, 0, len(reqs))
	for _, raw := range reqs {
		req, valid := parseRequest(raw)
		if !valid {
			responses = append(responses, d.Handle(ctx, nil))
			continue
		}
		response := d.Handle(ctx, req)
		if req.IsNotification() {
			continue
		}
		responses = append(responses, response)
	}
	if len(responses) == 0 {
		return nil, false
	}
	return d.marshal(responses), true
}

// parseRequest validates the envelope before deciding whether its absent id
// makes it a notification. A syntactically valid non-object is an Invalid
// Request, including when it is an element of a batch.
func parseRequest(raw []byte) (*Request, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' {
		return nil, false
	}
	var fields map[string]json.RawMessage
	if err := sonic.Unmarshal(raw, &fields); err != nil {
		return nil, false
	}
	var req Request
	if err := sonic.Unmarshal(fields["jsonrpc"], &req.JSONRPC); err != nil {
		return nil, false
	}
	if err := sonic.Unmarshal(fields["method"], &req.Method); err != nil {
		return nil, false
	}
	req.ID = fields["id"]
	req.Params = fields["params"]
	return &req, validRequest(&req)
}

func validRequest(req *Request) bool {
	if req == nil || req.JSONRPC != "2.0" || req.Method == "" {
		return false
	}
	if id := bytes.TrimSpace(req.ID); len(req.ID) != 0 {
		if !json.Valid(id) {
			return false
		}
		switch id[0] {
		case '"':
		case 'n':
			if !bytes.Equal(id, []byte("null")) {
				return false
			}
		default:
			if id[0] != '-' && (id[0] < '0' || id[0] > '9') {
				return false
			}
		}
	}
	if params := bytes.TrimSpace(req.Params); len(req.Params) != 0 {
		if !json.Valid(params) || (params[0] != '{' && params[0] != '[') {
			return false
		}
	}
	return true
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

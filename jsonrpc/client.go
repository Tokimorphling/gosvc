package jsonrpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"

	"github.com/Tokimorphling/gosvc/apierror"
)

const defaultTimeout = 5 * time.Second

// Transport sends JSON-RPC request bodies to a server. RoundTrip expects a
// response; Send is fire-and-forget (notifications).
type Transport interface {
	RoundTrip(ctx context.Context, request []byte) ([]byte, error)
	Send(ctx context.Context, request []byte) error
	Close() error
}

// Client is a JSON-RPC 2.0 client. Call is generic over the request and
// response types, so both sides of an internal API stay typed:
//
//	client := jsonrpc.NewTCPClient(addr)
//	resp, err := client.Call[HelloRequest, *HelloResponse](ctx, "greeter.sayHello", HelloRequest{Name: "x"})
type Client struct {
	transport Transport
	nextID    atomic.Int64
}

// NewClient wraps a transport.
func NewClient(transport Transport) *Client { return &Client{transport: transport} }

// Call invokes method and decodes the result into Resp.
func (c *Client) Call[Req, Resp any](ctx context.Context, method string, req Req) (Resp, error) {
	var zero Resp
	if c == nil || c.transport == nil {
		return zero, fmt.Errorf("jsonrpc: client has no transport")
	}

	id := c.nextID.Add(1)
	body, err := c.encodeRequest(id, method, req, false)
	if err != nil {
		return zero, err
	}

	raw, err := c.transport.RoundTrip(ctx, body)
	if err != nil {
		return zero, err
	}

	var wire struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   *Error          `json:"error"`
	}
	if err := sonic.Unmarshal(raw, &wire); err != nil {
		return zero, fmt.Errorf("jsonrpc: decode response: %w", err)
	}
	if wire.Error != nil {
		return zero, wire.Error
	}
	if len(wire.ID) > 0 && string(wire.ID) != strconv.FormatInt(id, 10) {
		return zero, fmt.Errorf("jsonrpc: response id %s does not match request id %d", wire.ID, id)
	}
	if len(wire.Result) == 0 || string(wire.Result) == "null" {
		return zero, nil
	}
	if err := sonic.Unmarshal(wire.Result, &zero); err != nil {
		return zero, fmt.Errorf("jsonrpc: decode result: %w", err)
	}
	return zero, nil
}

// Notify sends a notification; the server must not reply.
func (c *Client) Notify(ctx context.Context, method string, params any) error {
	if c == nil || c.transport == nil {
		return fmt.Errorf("jsonrpc: client has no transport")
	}
	body, err := EncodeNotification(method, params)
	if err != nil {
		return err
	}
	return c.transport.Send(ctx, body)
}

// Close releases the underlying transport.
func (c *Client) Close() error {
	if c == nil || c.transport == nil {
		return nil
	}
	return c.transport.Close()
}

func (c *Client) encodeRequest(id int64, method string, params any, notification bool) ([]byte, error) {
	encodedParams, err := encodeParamsOf(params)
	if err != nil {
		return nil, err
	}

	request := struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id,omitempty"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params,omitempty"`
	}{
		JSONRPC: "2.0",
		Method:  method,
		Params:  encodedParams,
	}
	if !notification {
		request.ID = json.RawMessage(strconv.FormatInt(id, 10))
	}

	body, err := sonic.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("jsonrpc: encode request: %w", err)
	}
	return body, nil
}

// ClientOption customises the convenience clients.
type ClientOption func(*clientOptions)

type clientOptions struct {
	httpClient     *http.Client
	headers        http.Header
	timeout        time.Duration
	onNotification NotificationHandler
}

// NotificationHandler receives server-originated notifications pushed over a
// transport that supports them (the TCP transport). method is the JSON-RPC
// method name; params is the raw params value, or nil when the frame carried
// none. Handlers run on the transport's read loop and must not block.
type NotificationHandler func(method string, params json.RawMessage)

// WithHTTPClient supplies the HTTP client used by NewHTTPClient.
func WithHTTPClient(client *http.Client) ClientOption {
	return func(o *clientOptions) { o.httpClient = client }
}

// WithHeader adds a header (for example X-API-Key) to every HTTP request.
func WithHeader(key, value string) ClientOption {
	return func(o *clientOptions) {
		if o.headers == nil {
			o.headers = make(http.Header)
		}
		o.headers.Add(key, value)
	}
}

// WithTimeout sets the per-request timeout.
func WithTimeout(d time.Duration) ClientOption {
	return func(o *clientOptions) { o.timeout = d }
}

// WithNotificationHandler installs a callback for server-originated
// notifications. It only has an effect on transports that support them
// (NewTCPClient); HTTP is strictly request/response and never invokes it.
func WithNotificationHandler(fn NotificationHandler) ClientOption {
	return func(o *clientOptions) { o.onNotification = fn }
}

// NewHTTPClient posts to baseURL + "/rpc", the endpoint served by
// transport/http.
func NewHTTPClient(baseURL string, opts ...ClientOption) *Client {
	o := clientOptions{timeout: defaultTimeout}
	for _, opt := range opts {
		opt(&o)
	}

	client := o.httpClient
	if client == nil {
		client = &http.Client{Timeout: o.timeout}
	}

	return NewClient(&HTTPTransport{
		URL:     strings.TrimSuffix(baseURL, "/") + "/rpc",
		Client:  client,
		Headers: o.headers,
	})
}

// NewTCPClient speaks the line-delimited JSON-RPC protocol served by
// transport/tcp, including server-pushed notifications when
// WithNotificationHandler is supplied.
func NewTCPClient(addr string, opts ...ClientOption) *Client {
	o := clientOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	return NewClient(&TCPTransport{Addr: addr, Timeout: o.timeout, OnNotification: o.onNotification})
}

// HTTPTransport posts JSON-RPC bodies to an HTTP endpoint.
type HTTPTransport struct {
	URL     string
	Client  *http.Client
	Headers http.Header
}

// RoundTrip implements Transport.
func (t *HTTPTransport) RoundTrip(ctx context.Context, request []byte) ([]byte, error) {
	resp, err := t.do(ctx, request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, fmt.Errorf("jsonrpc: read response: %w", err)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return body, nil
	case http.StatusNoContent:
		return nil, fmt.Errorf("jsonrpc: server returned no content for a request that expects a response")
	default:
		return nil, httpResponseError(resp.StatusCode, body)
	}
}

// httpResponseError preserves the same mapped errors for calls and notifications.
func httpResponseError(status int, body []byte) error {
	if kind, message := decodeErrorBody(body); kind != "" {
		return &apierror.Error{Kind: apierror.Kind(kind), Message: message, Op: fmt.Sprintf("http %d", status)}
	}
	return fmt.Errorf("jsonrpc: http %d: %s", status, strings.TrimSpace(string(body)))
}

// decodeErrorBody extracts the transport error mapping from an HTTP error
// body. It returns "" when the body does not look like a mapped error.
func decodeErrorBody(body []byte) (kind, message string) {
	var mapped struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := sonic.Unmarshal(body, &mapped); err != nil {
		return "", ""
	}
	if mapped.Error.Code == "" || mapped.Error.Message == "" {
		return "", ""
	}
	return mapped.Error.Code, mapped.Error.Message
}

// Send implements Transport.
func (t *HTTPTransport) Send(ctx context.Context, request []byte) error {
	resp, err := t.do(ctx, request)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("jsonrpc: read notification response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return httpResponseError(resp.StatusCode, body)
	}
	return nil
}

// Close implements Transport.
func (t *HTTPTransport) Close() error { return nil }

func (t *HTTPTransport) do(ctx context.Context, request []byte) (*http.Response, error) {
	client := t.Client
	if client == nil {
		client = http.DefaultClient
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.URL, bytes.NewReader(request))
	if err != nil {
		return nil, fmt.Errorf("jsonrpc: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for key, values := range t.Headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	return client.Do(req)
}

// TCPTransport speaks the line-delimited protocol over one persistent
// connection with pipelining: multiple calls may be in flight concurrently and
// responses are matched to requests by their JSON-RPC id, so a slow request
// does not serialize the ones behind it.
type TCPTransport struct {
	Addr    string
	Timeout time.Duration
	// OnNotification, when set, receives server-originated notification
	// frames (no id). It runs on the read loop and must not block.
	OnNotification NotificationHandler

	mu      sync.Mutex
	conn    net.Conn
	pending map[string]chan tcpResult
	closed  bool
}

type tcpResult struct {
	body []byte
	err  error
}

// errTransportClosed is returned by calls made after Close.
var errTransportClosed = errors.New("jsonrpc: transport closed")

// RoundTrip implements Transport. The request body must carry a JSON-RPC id;
// it is used to route the response back to this call.
func (t *TCPTransport) RoundTrip(ctx context.Context, request []byte) ([]byte, error) {
	var head struct {
		ID json.RawMessage `json:"id"`
	}
	if err := sonic.Unmarshal(request, &head); err != nil || len(head.ID) == 0 {
		return nil, fmt.Errorf("jsonrpc: tcp transport requires a request id")
	}

	// Bound the call when the context carries no deadline, matching the
	// deadline-based behaviour of the previous serial transport.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.timeoutOrDefault())
		defer cancel()
	}

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, errTransportClosed
	}
	if err := t.ensureLocked(ctx); err != nil {
		t.mu.Unlock()
		return nil, err
	}
	key := string(head.ID)
	result := make(chan tcpResult, 1)
	t.pending[key] = result
	err := t.writeLocked(ctx, request)
	t.mu.Unlock()

	if err != nil {
		t.mu.Lock()
		delete(t.pending, key)
		t.mu.Unlock()
		return nil, err
	}

	select {
	case res := <-result:
		return res.body, res.err
	case <-ctx.Done():
		// The response may still arrive later; dropping the pending entry
		// makes the read loop discard it instead of leaking a channel.
		t.mu.Lock()
		delete(t.pending, key)
		t.mu.Unlock()
		return nil, ctx.Err()
	}
}

// Send implements Transport.
func (t *TCPTransport) Send(ctx context.Context, request []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return errTransportClosed
	}
	if err := t.ensureLocked(ctx); err != nil {
		return err
	}
	return t.writeLocked(ctx, request)
}

// Close implements Transport. Pending calls fail with errTransportClosed.
func (t *TCPTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	err := t.closeLocked()
	t.failAllLocked(errTransportClosed)
	return err
}

// ensureLocked dials when no connection is held and starts the read loop. It
// requires t.mu.
func (t *TCPTransport) ensureLocked(ctx context.Context) error {
	if t.conn != nil {
		return nil
	}
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", t.Addr)
	if err != nil {
		return fmt.Errorf("jsonrpc: tcp dial %s: %w", t.Addr, err)
	}
	t.conn = conn
	if t.pending == nil {
		t.pending = make(map[string]chan tcpResult)
	}
	go t.readLoop(conn, bufio.NewReader(conn))
	return nil
}

// writeLocked writes one frame. It requires t.mu, which serialises concurrent
// writes. A write error tears the connection down and fails every pending
// call; the next RoundTrip dials again.
func (t *TCPTransport) writeLocked(ctx context.Context, request []byte) error {
	deadline := time.Now().Add(t.timeoutOrDefault())
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = t.conn.SetWriteDeadline(deadline)
	if err := writeFrame(t.conn, request); err != nil {
		t.resetLocked()
		t.failAllLocked(fmt.Errorf("jsonrpc: tcp write: %w", err))
		return fmt.Errorf("jsonrpc: tcp write: %w", err)
	}
	return nil
}

// readLoop consumes responses on one connection until it breaks, matching
// each response to its pending call by id. reader is bound to conn and never
// touched through the transport, so tearing the connection down concurrently
// is safe.
func (t *TCPTransport) readLoop(conn net.Conn, reader *bufio.Reader) {
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			t.mu.Lock()
			// Only tear down when this is still the live connection: the
			// transport may already have reset and dialled again.
			if t.conn == conn {
				t.resetLocked()
				t.failAllLocked(fmt.Errorf("jsonrpc: tcp read: %w", err))
			}
			t.mu.Unlock()
			return
		}
		var head struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if sonic.Unmarshal(line, &head) != nil {
			continue
		}
		if len(head.ID) == 0 {
			// No id: either unsolicited traffic or a server notification.
			if head.Method != "" && t.OnNotification != nil {
				t.OnNotification(head.Method, head.Params)
			}
			continue
		}
		key := string(head.ID)
		t.mu.Lock()
		result, ok := t.pending[key]
		if ok {
			delete(t.pending, key)
		}
		t.mu.Unlock()
		if ok {
			result <- tcpResult{body: bytes.TrimSpace(line)}
		}
	}
}

// failAllLocked delivers err to every pending call. It requires t.mu; the
// result channels are buffered so the send never blocks.
func (t *TCPTransport) failAllLocked(err error) {
	for key, result := range t.pending {
		result <- tcpResult{err: err}
		delete(t.pending, key)
	}
}

func (t *TCPTransport) timeoutOrDefault() time.Duration {
	if t.Timeout > 0 {
		return t.Timeout
	}
	return defaultTimeout
}

// resetLocked drops the connection without failing pending calls. It
// requires t.mu.
func (t *TCPTransport) resetLocked() {
	if t.conn != nil {
		_ = t.conn.Close()
	}
	t.conn = nil
}

func (t *TCPTransport) closeLocked() error {
	if t.conn == nil {
		return nil
	}
	err := t.conn.Close()
	t.conn = nil
	return err
}

func writeFrame(conn net.Conn, request []byte) error {
	frame := make([]byte, 0, len(request)+1)
	frame = append(frame, request...)
	frame = append(frame, '\n')
	_, err := conn.Write(frame)
	return err
}

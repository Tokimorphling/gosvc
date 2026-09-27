package jsonrpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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
	body, err := c.encodeRequest(0, method, params, true)
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
	encodedParams, err := encodeParams(params)
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

// encodeParams drops empty params so methods without arguments stay clean.
func encodeParams[Req any](req Req) (json.RawMessage, error) {
	raw, err := sonic.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("jsonrpc: encode params: %w", err)
	}
	switch string(raw) {
	case "null", "{}":
		return nil, nil
	}
	return raw, nil
}

// ClientOption customises the convenience clients.
type ClientOption func(*clientOptions)

type clientOptions struct {
	httpClient *http.Client
	headers    http.Header
	timeout    time.Duration
}

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

// NewHTTPClient posts to baseURL + "/rpc", the endpoint served by
// transport/http.
func NewHTTPClient(baseURL string, opts ...ClientOption) *Client {
	o := clientOptions{}
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
// transport/tcp.
func NewTCPClient(addr string, opts ...ClientOption) *Client {
	o := clientOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	return NewClient(&TCPTransport{Addr: addr, Timeout: o.timeout})
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
		return nil, fmt.Errorf("jsonrpc: http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
}

// Send implements Transport.
func (t *HTTPTransport) Send(ctx context.Context, request []byte) error {
	resp, err := t.do(ctx, request)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.Body.Close()
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

// TCPTransport keeps one persistent connection and serializes requests on it.
type TCPTransport struct {
	Addr    string
	Timeout time.Duration

	mu     sync.Mutex
	conn   net.Conn
	reader *bufio.Reader
}

// RoundTrip implements Transport.
func (t *TCPTransport) RoundTrip(ctx context.Context, request []byte) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.ensureLocked(ctx); err != nil {
		return nil, err
	}
	t.setDeadlineLocked(ctx)

	if err := writeFrame(t.conn, request); err != nil {
		t.resetLocked()
		return nil, fmt.Errorf("jsonrpc: tcp write: %w", err)
	}
	line, err := t.reader.ReadBytes('\n')
	if err != nil {
		t.resetLocked()
		return nil, fmt.Errorf("jsonrpc: tcp read: %w", err)
	}
	return bytes.TrimSpace(line), nil
}

// Send implements Transport.
func (t *TCPTransport) Send(ctx context.Context, request []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.ensureLocked(ctx); err != nil {
		return err
	}
	t.setDeadlineLocked(ctx)

	if err := writeFrame(t.conn, request); err != nil {
		t.resetLocked()
		return fmt.Errorf("jsonrpc: tcp write: %w", err)
	}
	return nil
}

// Close implements Transport.
func (t *TCPTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closeLocked()
}

func (t *TCPTransport) ensureLocked(ctx context.Context) error {
	if t.conn != nil {
		return nil
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", t.Addr)
	if err != nil {
		return fmt.Errorf("jsonrpc: tcp dial %s: %w", t.Addr, err)
	}
	t.conn = conn
	t.reader = bufio.NewReader(conn)
	return nil
}

func (t *TCPTransport) setDeadlineLocked(ctx context.Context) {
	timeout := t.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = t.conn.SetDeadline(deadline)
}

func (t *TCPTransport) resetLocked() {
	if t.conn != nil {
		_ = t.conn.Close()
	}
	t.conn = nil
	t.reader = nil
}

func (t *TCPTransport) closeLocked() error {
	if t.conn == nil {
		return nil
	}
	err := t.conn.Close()
	t.conn = nil
	t.reader = nil
	return err
}

func writeFrame(conn net.Conn, request []byte) error {
	frame := make([]byte, 0, len(request)+1)
	frame = append(frame, request...)
	frame = append(frame, '\n')
	_, err := conn.Write(frame)
	return err
}

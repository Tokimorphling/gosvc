package tcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tokimorphling/gosvc/apierror"
	"github.com/Tokimorphling/gosvc/config"
	"github.com/Tokimorphling/gosvc/health"
	"github.com/Tokimorphling/gosvc/jsonrpc"
	"github.com/Tokimorphling/gosvc/push"
)

// textCodec is a deliberately non-JSON-RPC dialect used to exercise the codec
// seam end to end:
//
//	request      CALL <id> <method> [<params-json>]
//	request      NOTIFY <method> [<params-json>]
//	response     RESULT <id> <result-json>
//	protocol err ERROR <sentinel|kind> <message>
//	push         PUSH <method> <params-json>
//
// It maps jsonrpc.ErrMethodNotFound onto its own "not_found" shape to prove
// the sentinel contract described by the Codec documentation.
type textCodec struct{}

func (textCodec) Name() string { return "text" }

func (textCodec) Decode(body []byte) (Call, error) {
	parts := strings.SplitN(strings.TrimSpace(string(body)), " ", 4)
	switch parts[0] {
	case "CALL":
		if len(parts) < 3 {
			return Call{}, errors.New("text: malformed CALL frame")
		}
		call := Call{ID: json.RawMessage(parts[1]), Method: parts[2]}
		if len(parts) == 4 {
			call.Params = json.RawMessage(parts[3])
		}
		return call, nil
	case "NOTIFY":
		if len(parts) < 2 {
			return Call{}, errors.New("text: malformed NOTIFY frame")
		}
		call := Call{Method: parts[1], Notification: true}
		if len(parts) > 2 {
			call.Params = json.RawMessage(parts[2])
		}
		return call, nil
	default:
		return Call{}, errors.New("text: unknown frame type")
	}
}

func (textCodec) Encode(call Call, result any, callErr error) ([]byte, error) {
	if callErr != nil {
		if errors.Is(callErr, jsonrpc.ErrMethodNotFound) {
			return []byte("ERROR not_found method not found"), nil
		}
		kind := apierror.KindOf(callErr)
		if kind == apierror.KindUnknown {
			kind = "unknown"
		}
		return []byte(fmt.Sprintf("ERROR %s %s", kind, apierror.ClientMessage(callErr))), nil
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf("RESULT %s %s", call.ID, raw)), nil
}

func (textCodec) EncodeNotification(method string, params any) ([]byte, error) {
	raw, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf("PUSH %s %s", method, raw)), nil
}

// serialTextCodec is the text dialect with per-connection serial dispatch
// requested, the way an order-dependent protocol like stratum would.
type serialTextCodec struct{ textCodec }

func (serialTextCodec) SerialPerConn() bool { return true }

// TestCodecSerialPerConn verifies that a serial codec's frames are executed
// strictly in arrival order even when the client pipelines them: the slow
// first call completes before the fast second one starts.
func TestCodecSerialPerConn(t *testing.T) {
	var mu sync.Mutex
	var order []string

	dispatcher := jsonrpc.NewDispatcher()
	slow := func(_ context.Context, _ json.RawMessage) (any, error) {
		time.Sleep(150 * time.Millisecond)
		mu.Lock()
		order = append(order, "slow")
		mu.Unlock()
		return "slow", nil
	}
	fast := func(_ context.Context, _ json.RawMessage) (any, error) {
		mu.Lock()
		order = append(order, "fast")
		mu.Unlock()
		return "fast", nil
	}
	dispatcher.Register("slow", slow)
	dispatcher.Register("fast", fast)

	h := startHarness(t, func(o *Options) {
		o.Codec = serialTextCodec{}
		o.Dispatcher = dispatcher
	})
	defer h.stop()

	conn, reader := dial(t, h.addr)
	defer conn.Close()

	// Pipeline both calls back to back.
	if _, err := conn.Write([]byte("CALL 1 slow\nCALL 2 fast\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	first, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read first: %v", err)
	}
	second, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read second: %v", err)
	}

	if first != "RESULT 1 \"slow\"\n" || second != "RESULT 2 \"fast\"\n" {
		t.Fatalf("first = %q, second = %q", first, second)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "slow" || order[1] != "fast" {
		t.Fatalf("execution order = %v, want [slow fast]", order)
	}
}

// TestSinkInjectedIntoDispatchContext verifies that TCP requests carry a
// push.Sink resolvable through push.SinkFromContext, usable by generic
// subscribe handlers.
func TestSinkInjectedIntoDispatchContext(t *testing.T) {
	var sinkSeen atomic.Bool
	dispatcher := jsonrpc.NewDispatcher()
	dispatcher.Register("probe", func(ctx context.Context, _ json.RawMessage) (any, error) {
		if _, ok := push.SinkFromContext(ctx); ok {
			sinkSeen.Store(true)
		}
		if jsonrpc.TransportFromContext(ctx) != "tcp" {
			return nil, errors.New("transport identity not injected")
		}
		return "ok", nil
	})

	h := startHarness(t, func(o *Options) {
		o.Dispatcher = dispatcher
	})
	defer h.stop()

	conn, reader := dial(t, h.addr)
	defer conn.Close()
	send(t, conn, `{"jsonrpc":"2.0","id":1,"method":"probe"}`)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(line, `"result":"ok"`) {
		t.Fatalf("line = %q", line)
	}
	if !sinkSeen.Load() {
		t.Fatal("the dispatch context must carry a push sink")
	}
}

// harness runs a Server on an ephemeral port with the given options and
// returns its address plus a stop function that waits for the drain.
type harness struct {
	addr string
	stop func()
}

func startHarness(t *testing.T, mutate func(*Options)) *harness {
	t.Helper()

	cfg := &config.Config{}
	cfg.SetDefaults()
	cfg.TCP.Enabled = true
	cfg.TCP.Host, cfg.TCP.Port = "127.0.0.1", 0
	cfg.TCP.Workers = 2
	cfg.TCP.QueueSize = 64
	cfg.TCP.HandlerTimeout = config.Duration(3 * time.Second)
	cfg.TCP.ShutdownTimeout = config.Duration(2 * time.Second)
	cfg.TCP.NotifyQueueSize = 16
	cfg.TCP.NotifyPolicy = "drop"
	cfg.TCP.ReadTimeout = config.Duration(5 * time.Second)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	dispatcher := jsonrpc.NewDispatcher()
	dispatcher.Register("echo", func(ctx context.Context, params json.RawMessage) (any, error) {
		if len(params) == 0 {
			return "empty", nil
		}
		var value any
		if err := json.Unmarshal(params, &value); err != nil {
			return nil, err
		}
		return value, nil
	})

	ready := &health.Ready{}
	ready.Set(true)

	options := Options{
		Config:     cfg,
		Logger:     slog.Default(),
		Dispatcher: dispatcher,
		Ready:      ready,
	}
	if mutate != nil {
		mutate(&options)
	}

	server, err := New(options)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(ctx) }()

	return &harness{
		addr: server.Addr(),
		stop: func() {
			t.Helper()
			cancel()
			select {
			case err := <-serveErr:
				if err != nil {
					t.Fatalf("serve: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("server did not shut down within 10s")
			}
		},
	}
}

func dial(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return conn, bufio.NewReader(conn)
}

func send(t *testing.T, conn net.Conn, line string) {
	t.Helper()
	if _, err := conn.Write([]byte(line + "\n")); err != nil {
		t.Fatalf("write %q: %v", line, err)
	}
}

func TestCodecRoundtripAndErrors(t *testing.T) {
	h := startHarness(t, func(o *Options) { o.Codec = textCodec{} })
	defer h.stop()

	conn, reader := dial(t, h.addr)
	defer conn.Close()

	// Regular call, params passed through verbatim.
	send(t, conn, `CALL 7 echo {"a":1}`)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if line != "RESULT 7 {\"a\":1}\n" {
		t.Fatalf("line = %q", line)
	}

	// Notifications are dispatched, never answered.
	send(t, conn, `NOTIFY echo {"a":2}`)
	if err := conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadString('\n'); err == nil {
		t.Fatal("notifications must not produce a response")
	}
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}

	// Unknown method maps onto the codec's own error shape.
	send(t, conn, `CALL 8 nope`)
	line, err = reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if line != "ERROR not_found method not found\n" {
		t.Fatalf("line = %q", line)
	}

	// Malformed frames surface as protocol-level errors in the dialect.
	send(t, conn, `GARBAGE`)
	line, err = reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.HasPrefix(line, "ERROR ") {
		t.Fatalf("line = %q", line)
	}
}

func TestCodecRawHandlerErrorIsInternal(t *testing.T) {
	cfg := &config.Config{}
	cfg.SetDefaults()
	cfg.TCP.Enabled = true
	cfg.TCP.Host, cfg.TCP.Port = "127.0.0.1", 0
	cfg.TCP.HandlerTimeout = config.Duration(3 * time.Second)
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	dispatcher := jsonrpc.NewDispatcher()
	dispatcher.Register("boom", func(context.Context, json.RawMessage) (any, error) {
		return nil, fmt.Errorf("password=sekrit exploded") // raw error: must not leak
	})
	ready := &health.Ready{}
	ready.Set(true)
	server, err := New(Options{
		Config: cfg, Logger: slog.Default(),
		Dispatcher: dispatcher, Ready: ready, Codec: textCodec{},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(ctx) }()

	conn, reader := dial(t, server.Addr())
	defer conn.Close()
	send(t, conn, `CALL 1 boom`)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if line != "ERROR internal internal server error\n" {
		t.Fatalf("line = %q, want the raw error redacted", line)
	}

	cancel()
	select {
	case <-serveErr:
	case <-time.After(10 * time.Second):
		t.Fatal("server did not shut down")
	}
}

// lifecycleRecorder captures the callback sequence for assertions.
type lifecycleRecorder struct {
	mu         sync.Mutex
	connected  []*Conn
	disconnect []struct {
		conn   *Conn
		reason error
	}
	onConnectPanic bool
}

func (r *lifecycleRecorder) OnConnect(ctx context.Context, conn *Conn) context.Context {
	r.mu.Lock()
	r.connected = append(r.connected, conn)
	r.mu.Unlock()
	if r.onConnectPanic {
		panic("OnConnect requested to panic")
	}
	return context.WithValue(ctx, lifecycleKey{}, conn.RemoteAddr)
}

func (r *lifecycleRecorder) OnDisconnect(ctx context.Context, conn *Conn, reason error) {
	r.mu.Lock()
	r.disconnect = append(r.disconnect, struct {
		conn   *Conn
		reason error
	}{conn, reason})
	r.mu.Unlock()
}

type lifecycleKey struct{}

func TestCallbacksLifecycle(t *testing.T) {
	rec := &lifecycleRecorder{}

	// The dedicated handler proves the context returned by OnConnect parents
	// every request on the connection.
	dispatcher := jsonrpc.NewDispatcher()
	dispatcher.Register("ctx", func(ctx context.Context, _ json.RawMessage) (any, error) {
		if value, ok := ctx.Value(lifecycleKey{}).(string); ok {
			return value, nil
		}
		return nil, errors.New("context from OnConnect is missing")
	})

	h := startHarness(t, func(o *Options) {
		o.Callbacks = rec
		o.Dispatcher = dispatcher
	})
	defer h.stop()

	conn, reader := dial(t, h.addr)

	send(t, conn, `{"jsonrpc":"2.0","id":1,"method":"ctx"}`)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(line, `"result":"127.0.0.1:`) {
		t.Fatalf("line = %q, want the OnConnect context value in the result", line)
	}

	rec.mu.Lock()
	if len(rec.connected) != 1 {
		rec.mu.Unlock()
		t.Fatalf("OnConnect calls = %d, want 1", len(rec.connected))
	}
	if rec.connected[0].Session == nil {
		rec.mu.Unlock()
		t.Fatal("callbacks must eagerly create the session")
	}
	rec.mu.Unlock()

	// Peer close: OnDisconnect fires once with the same *Conn and no reason.
	_ = conn.Close()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		rec.mu.Lock()
		got := len(rec.disconnect)
		rec.mu.Unlock()
		if got == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.disconnect) != 1 {
		t.Fatalf("OnDisconnect calls = %d, want 1", len(rec.disconnect))
	}
	if rec.disconnect[0].conn != rec.connected[0] {
		t.Fatal("OnDisconnect must receive the same *Conn as OnConnect")
	}
	if rec.disconnect[0].reason != nil {
		t.Fatalf("reason = %v, want nil for a peer close", rec.disconnect[0].reason)
	}
}

func TestCallbacksShutdownReason(t *testing.T) {
	rec := &lifecycleRecorder{}
	h := startHarness(t, func(o *Options) { o.Callbacks = rec })

	conn, _ := dial(t, h.addr)
	defer conn.Close()

	h.stop() // drains; the connection ends server-side

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rec.mu.Lock()
		got := len(rec.disconnect)
		rec.mu.Unlock()
		if got == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.disconnect) != 1 {
		t.Fatalf("OnDisconnect calls = %d, want 1", len(rec.disconnect))
	}
	if !errors.Is(rec.disconnect[0].reason, ErrServerShutdown) {
		t.Fatalf("reason = %v, want ErrServerShutdown", rec.disconnect[0].reason)
	}
}

func TestCodecPushUsesDialect(t *testing.T) {
	rec := &lifecycleRecorder{}
	h := startHarness(t, func(o *Options) {
		o.Codec = textCodec{}
		o.Callbacks = rec
	})
	defer h.stop()

	conn, reader := dial(t, h.addr)
	defer conn.Close()

	send(t, conn, `CALL 1 echo "x"`)
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatalf("read: %v", err)
	}

	// Push through the eager session created by the callbacks: the frame
	// must be in the text dialect, not JSON-RPC.
	rec.mu.Lock()
	session := rec.connected[0].Session
	rec.mu.Unlock()
	if err := session.Notify("events.tick", map[string]int{"n": 1}); err != nil {
		t.Fatalf("notify: %v", err)
	}
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read push: %v", err)
	}
	if line != "PUSH events.tick {\"n\":1}\n" {
		t.Fatalf("line = %q", line)
	}
}

func TestOnConnectPanicClosesConnection(t *testing.T) {
	rec := &lifecycleRecorder{onConnectPanic: true}
	h := startHarness(t, func(o *Options) { o.Callbacks = rec })
	defer h.stop()

	conn, _ := dial(t, h.addr)
	defer conn.Close()

	// A panicking OnConnect must close the connection, not the server: the
	// pending read fails once the connection dies.
	send(t, conn, `CALL 1 echo "x"`)
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected the connection to be closed after the OnConnect panic")
	}
}

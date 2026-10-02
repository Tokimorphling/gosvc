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
	"github.com/Tokimorphling/gosvc/internal/workerpool"
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

func TestCodecSerialPerConnDoesNotBlockOtherConnections(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	releaseSlow := sync.OnceFunc(func() { close(release) })

	dispatcher := jsonrpc.NewDispatcher()
	dispatcher.Register("slow", func(context.Context, json.RawMessage) (any, error) {
		close(started)
		<-release
		return "slow", nil
	})
	dispatcher.Register("fast", func(context.Context, json.RawMessage) (any, error) {
		return "fast", nil
	})

	h := startHarness(t, func(o *Options) {
		o.Config.TCP.Workers = 2
		o.Codec = serialTextCodec{}
		o.Dispatcher = dispatcher
	})
	defer h.stop()
	defer releaseSlow()

	first, firstReader := dial(t, h.addr)
	defer first.Close()
	send(t, first, "CALL 1 slow")
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("slow handler did not start")
	}
	if _, err := first.Write([]byte(strings.Repeat("CALL 2 fast\n", 8))); err != nil {
		t.Fatal(err)
	}
	// Allow netpoll to hand the pipelined successors to the pool. In the old
	// implementation its second worker would be occupied waiting for slow.
	time.Sleep(100 * time.Millisecond)

	other, otherReader := dial(t, h.addr)
	defer other.Close()
	send(t, other, "CALL 3 fast")
	if err := other.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	line, err := otherReader.ReadString('\n')
	if err != nil || line != "RESULT 3 \"fast\"\n" {
		t.Fatalf("other connection response = %q, %v", line, err)
	}

	releaseSlow()
	line, err = firstReader.ReadString('\n')
	if err != nil || line != "RESULT 1 \"slow\"\n" {
		t.Fatalf("first connection response = %q, %v", line, err)
	}
}

func TestMaxFrameBytesAllowsPipelinedFrames(t *testing.T) {
	h := startHarness(t, func(o *Options) { o.Config.TCP.MaxFrameBytes = 64 })
	defer h.stop()

	conn, reader := dial(t, h.addr)
	defer conn.Close()
	const count = 8
	var pipeline strings.Builder
	for id := 1; id <= count; id++ {
		fmt.Fprintf(&pipeline, `{"jsonrpc":"2.0","id":%d,"method":"echo"}`+"\n", id)
	}
	if _, err := conn.Write([]byte(pipeline.String())); err != nil {
		t.Fatal(err)
	}
	seen := make(map[int]bool)
	for range count {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		var response struct {
			ID int `json:"id"`
		}
		if err := json.Unmarshal([]byte(line), &response); err != nil {
			t.Fatalf("response = %q: %v", line, err)
		}
		if response.ID < 1 || response.ID > count || seen[response.ID] {
			t.Fatalf("unexpected response id %d", response.ID)
		}
		seen[response.ID] = true
	}
}

func TestMaxFrameBytesAcrossReads(t *testing.T) {
	h := startHarness(t, func(o *Options) {
		o.Config.TCP.MaxFrameBytes = 64
		o.Codec = textCodec{}
	})
	defer h.stop()

	conn, reader := dial(t, h.addr)
	defer conn.Close()
	frame := "CALL 1 echo" + strings.Repeat(" ", 64-len("CALL 1 echo")-1) + "\n"
	if len(frame) != 64 {
		t.Fatalf("test frame length = %d", len(frame))
	}
	if _, err := conn.Write([]byte(frame[:len(frame)-1])); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if _, err := conn.Write([]byte(frame[len(frame)-1:])); err != nil {
		t.Fatal(err)
	}
	line, err := reader.ReadString('\n')
	if err != nil || line != "RESULT 1 \"empty\"\n" {
		t.Fatalf("max-sized frame response = %q, %v", line, err)
	}
	send(t, conn, "CALL 2 echo")
	line, err = reader.ReadString('\n')
	if err != nil || line != "RESULT 2 \"empty\"\n" {
		t.Fatalf("following frame response = %q, %v", line, err)
	}
}

func TestMaxFrameBytesRejectsUnterminatedFrame(t *testing.T) {
	h := startHarness(t, func(o *Options) { o.Config.TCP.MaxFrameBytes = 64 })
	defer h.stop()

	conn, reader := dial(t, h.addr)
	defer conn.Close()
	for _, chunk := range []string{strings.Repeat("x", 32), strings.Repeat("x", 32), "x"} {
		if _, err := conn.Write([]byte(chunk)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(30 * time.Millisecond)
	}
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read oversize response: %v", err)
	}
	if !strings.Contains(line, "frame too large") {
		t.Fatalf("response = %q, want frame too large", line)
	}
	if _, err := reader.ReadByte(); err == nil {
		t.Fatal("oversized connection was not closed")
	}
}

func TestIncompleteFrameReadTimeout(t *testing.T) {
	h := startHarness(t, func(o *Options) {
		o.Config.TCP.ReadTimeout = config.Duration(100 * time.Millisecond)
	})
	defer h.stop()

	conn, reader := dial(t, h.addr)
	defer conn.Close()
	if _, err := conn.Write([]byte(`{"jsonrpc":"2.0"`)); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadByte(); err == nil {
		t.Fatal("incomplete frame remained connected after read timeout")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatalf("connection did not close on incomplete-frame timeout: %v", err)
	}
}

func TestIncompleteFramePeerDisconnect(t *testing.T) {
	rec := &lifecycleRecorder{}
	h := startHarness(t, func(o *Options) {
		o.Config.TCP.ReadTimeout = config.Duration(300 * time.Millisecond)
		o.Callbacks = rec
	})
	defer h.stop()

	conn, _ := dial(t, h.addr)
	if _, err := conn.Write([]byte(`{"jsonrpc":"2.0"`)); err != nil {
		t.Fatal(err)
	}
	// Let OnRequest arm its partial-frame timer, then end the connection.
	time.Sleep(30 * time.Millisecond)
	_ = conn.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rec.mu.Lock()
		count := len(rec.disconnect)
		rec.mu.Unlock()
		if count == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("disconnect callback was not called for a partial frame")
}

func TestCloseUnusedServer(t *testing.T) {
	cfg := &config.Config{}
	cfg.SetDefaults()
	cfg.TCP.Host, cfg.TCP.Port = "127.0.0.1", 0
	server, err := New(Options{Config: cfg, Logger: slog.Default()})
	if err != nil {
		t.Fatal(err)
	}
	addr := server.Addr()
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := server.pool.Submit(func() {}); !errors.Is(err, workerpool.ErrClosed) {
		t.Fatalf("pool Submit after Close = %v", err)
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen after Close: %v", err)
	}
	_ = listener.Close()
}

func TestServeWithPreCanceledContext(t *testing.T) {
	server := newStartupTestServer(t)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assertServeStops(t, server, ctx)
}

func TestServeCancellationDuringStartup(t *testing.T) {
	for i := range 12 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			server := newStartupTestServer(t)
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			started := make(chan struct{})
			result := make(chan error, 1)
			go func() {
				close(started)
				result <- server.Serve(ctx)
			}()
			<-started
			if i%2 == 0 {
				go cancel()
			} else {
				cancel()
			}
			awaitServeStop(t, server, result)
		})
	}
}

func newStartupTestServer(t *testing.T) *Server {
	t.Helper()
	cfg := &config.Config{}
	cfg.SetDefaults()
	cfg.TCP.Host, cfg.TCP.Port = "127.0.0.1", 0
	cfg.TCP.ShutdownTimeout = config.Duration(time.Second)
	server, err := New(Options{Config: cfg, Logger: slog.Default()})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func assertServeStops(t *testing.T, server *Server, ctx context.Context) {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- server.Serve(ctx) }()
	awaitServeStop(t, server, result)
}

func awaitServeStop(t *testing.T, server *Server, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Serve with cancelled context: %v", err)
		}
	case <-time.After(2 * time.Second):
		// Unblock an older netpoll loop as well, so a failure does not leave a
		// hung goroutine behind for the rest of the test process.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = server.eventLoop.Shutdown(shutdownCtx)
		cancel()
		t.Fatal("Serve did not stop after context cancellation")
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
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
	mu             sync.Mutex
	connected      []*Conn
	disconnect     []disconnectRecord
	onConnectPanic bool
}

// disconnectRecord pairs the ended connection with the reported reason.
type disconnectRecord struct {
	conn   *Conn
	reason error
}

// ownConnected returns the *Conn recorded for the test's own client socket.
// Local machines may probe freshly bound loopback listeners (port scanners,
// EDR), which shows up as extra connections; assertions filter by remote.
func (r *lifecycleRecorder) ownConnected(remote string) *Conn {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.connected {
		if c.RemoteAddr == remote {
			return c
		}
	}
	return nil
}

// ownDisconnects returns the OnDisconnect records of the given connection.
func (r *lifecycleRecorder) ownDisconnects(remote string) []disconnectRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []disconnectRecord
	for _, d := range r.disconnect {
		if d.conn.RemoteAddr == remote {
			out = append(out, d)
		}
	}
	return out
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
	r.disconnect = append(r.disconnect, disconnectRecord{conn: conn, reason: reason})
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

	own := conn.LocalAddr().String()
	ownConn := rec.ownConnected(own)
	if ownConn == nil {
		t.Fatal("OnConnect did not run for the test connection")
	}
	if ownConn.Session == nil {
		t.Fatal("callbacks must eagerly create the session")
	}

	// Peer close: OnDisconnect fires once with the same *Conn and no reason.
	_ = conn.Close()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := len(rec.ownDisconnects(own)); got == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := rec.ownDisconnects(own); len(got) != 1 {
		t.Fatalf("OnDisconnect calls = %d, want 1", len(got))
	}
	disconnect := rec.ownDisconnects(own)[0]
	if disconnect.conn != rec.ownConnected(own) {
		t.Fatal("OnDisconnect must receive the same *Conn as OnConnect")
	}
	if disconnect.reason != nil {
		t.Fatalf("reason = %v, want nil for a peer close", disconnect.reason)
	}
}

func TestCallbacksShutdownReason(t *testing.T) {
	rec := &lifecycleRecorder{}
	h := startHarness(t, func(o *Options) { o.Callbacks = rec })

	conn, _ := dial(t, h.addr)
	defer conn.Close()

	h.stop() // drains; the connection ends server-side

	own := conn.LocalAddr().String()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := len(rec.ownDisconnects(own)); got == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	disconnects := rec.ownDisconnects(own)
	if len(disconnects) != 1 {
		t.Fatalf("OnDisconnect calls = %d, want 1", len(disconnects))
	}
	if !errors.Is(disconnects[0].reason, ErrServerShutdown) {
		t.Fatalf("reason = %v, want ErrServerShutdown", disconnects[0].reason)
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
	own := conn.LocalAddr().String()
	ownConn := rec.ownConnected(own)
	if ownConn == nil {
		t.Fatal("OnConnect did not run for the test connection")
	}
	if err := ownConn.Session.Notify("events.tick", map[string]int{"n": 1}); err != nil {
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

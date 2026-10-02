// Package tcp serves line-delimited JSON-RPC 2.0 over netpoll.
//
// Design notes:
//   - requests are read on the netpoll event loop (one OnRequest at a time per
//     connection) and handed to a bounded worker pool, so slow handlers never
//     block the event loop and the queue provides explicit backpressure;
//   - netpoll requires serialized writes per connection, so every connection
//     carries a write mutex in its context;
//   - frames are length-limited while assembling each line: partial frames
//     are consumed from netpoll's read buffer and retained only up to the
//     configured limit, without waiting for an unbounded Until call;
//   - connections that arrive while the service is not ready, or exceed the
//     rate limit, are answered once and closed: netpoll is level-triggered
//     and re-fires OnRequest while input is pending, so leaving unread data
//     behind would spin the event loop and amplify writes;
//   - TLS is intentionally not handled here: terminate it at a gateway and use
//     this listener for internal traffic. The TCP transport also does not
//     authenticate callers (unlike HTTP and gRPC); treat it as a trusted
//     internal hop or front it with an authenticating proxy.
package tcp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/netpoll"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Tokimorphling/gosvc/apierror"
	"github.com/Tokimorphling/gosvc/config"
	"github.com/Tokimorphling/gosvc/health"
	"github.com/Tokimorphling/gosvc/internal/workerpool"
	"github.com/Tokimorphling/gosvc/jsonrpc"
	"github.com/Tokimorphling/gosvc/logging"
	"github.com/Tokimorphling/gosvc/observability"
	"github.com/Tokimorphling/gosvc/push"
	"github.com/Tokimorphling/gosvc/ratelimit"
	"github.com/Tokimorphling/gosvc/store"
)

var (
	busyFrame     = []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32004,"message":"server busy"}}`)
	notReadyFrame = []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32004,"message":"service not ready"}}`)
	tooLargeFrame = []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"frame too large"}}`)
	internalFrame = []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"internal error"}}`)
)

// Options wires the TCP server.
type Options struct {
	Config   *config.Config
	Logger   *slog.Logger
	Metrics  *observability.Metrics
	Limiter  *ratelimit.Limiter
	Recorder store.Recorder
	Ready    *health.Ready
	// Dispatcher is shared with the HTTP /rpc endpoint so methods are
	// registered once.
	Dispatcher *jsonrpc.Dispatcher
	// Tracer records one span per request. Optional.
	Tracer trace.Tracer
	// Codec owns the frame dialect. Nil keeps the strict JSON-RPC 2.0
	// behaviour (dispatcher.Serve: envelope validation, batches, params
	// decoding) byte for byte.
	Codec Codec
	// Callbacks observes connection lifecycle events. When set, every
	// connection gets its push Session eagerly and OnConnect/OnDisconnect
	// fire; nil keeps the lazy, callback-free behaviour.
	Callbacks Callbacks
}

// Server is the netpoll based JSON-RPC server.
type Server struct {
	eventLoop  netpoll.EventLoop
	listener   net.Listener
	pool       *workerpool.Pool
	dispatcher *jsonrpc.Dispatcher
	cfg        config.TCPConfig
	logger     *slog.Logger
	metrics    *observability.Metrics
	limiter    *ratelimit.Limiter
	recorder   store.Recorder
	ready      *health.Ready
	tracer     trace.Tracer

	codec     Codec
	callbacks Callbacks
	// serialPerConn is resolved once at construction from the codec's
	// optional Serial capability.
	serialPerConn bool

	// shutdownFrame is the going-away frame rendered in the active dialect.
	shutdownFrame []byte
	// draining marks the shutdown phase, so OnDisconnect reports
	// ErrServerShutdown for connections closed during the drain.
	draining atomic.Bool

	// sessions tracks live push sessions for the shutdown broadcast.
	sessions  sync.Map
	closeOnce sync.Once
	closeErr  error
}

// connState is created per connection on the event loop and carried in the
// connection context. It owns the per-connection write lock and, lazily, the
// push session.
type connState struct {
	server *Server
	conn   netpoll.Connection
	remote string

	writeMu   sync.Mutex
	sessionMu sync.Mutex
	session   *Session

	// disconnectFired keeps OnDisconnect exactly-once: netpoll re-runs the
	// whole close-callback chain when a second Close path (poller detach,
	// the shutdown session close) follows the first one.
	disconnectFired atomic.Bool

	// connInfo is the lifecycle-callback view of the connection; it is only
	// set when callbacks are installed, so OnDisconnect can distinguish
	// prepared connections.
	connInfo *Conn
	// closeReason is set (before the close) for server-initiated
	// disconnects, so OnDisconnect can report the cause. Peer-initiated
	// closes leave it nil.
	closeReason atomic.Pointer[error]

	// partial is the current unfinished frame. netpoll serialises OnRequest
	// per connection, so only the connection reader goroutine touches it.
	partial    []byte
	frameTimer *time.Timer
}

type connStateKey struct{}

// New binds the listener and builds the event loop.
func New(opts Options) (*Server, error) {
	listener, err := net.Listen("tcp", opts.Config.TCP.Addr())
	if err != nil {
		return nil, fmt.Errorf("listen tcp: %w", err)
	}

	dispatcher := opts.Dispatcher
	if dispatcher == nil {
		dispatcher = jsonrpc.NewDispatcher()
	}

	s := &Server{
		listener:   listener,
		pool:       workerpool.New(opts.Config.TCP.Workers, opts.Config.TCP.QueueSize),
		dispatcher: dispatcher,
		cfg:        opts.Config.TCP,
		logger:     opts.Logger,
		metrics:    opts.Metrics,
		limiter:    opts.Limiter,
		recorder:   opts.Recorder,
		ready:      opts.Ready,
		tracer:     opts.Tracer,
		codec:      opts.Codec,
		callbacks:  opts.Callbacks,
	}
	if s.metrics == nil {
		s.metrics = observability.New("gosvc")
	}
	s.shutdownFrame = shuttingDownFrame
	if opts.Codec != nil {
		if frame, err := opts.Codec.EncodeNotification(MethodShutdown, nil); err == nil {
			s.shutdownFrame = frame
		}
		if serial, ok := opts.Codec.(Serial); ok {
			s.serialPerConn = serial.SerialPerConn()
		}
	}
	s.pool.SetPanicHandler(func(recovered any) {
		opts.Logger.Error("tcp worker panic", "panic", recovered)
	})

	loopOptions := []netpoll.Option{
		netpoll.WithOnPrepare(s.onPrepare),
		netpoll.WithReadTimeout(opts.Config.TCP.ReadTimeout.D()),
	}
	eventLoop, err := netpoll.NewEventLoop(s.handleRequest, loopOptions...)
	if err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("netpoll event loop: %w", err)
	}
	s.eventLoop = eventLoop
	return s, nil
}

// Dispatcher exposes the shared JSON-RPC dispatcher.
func (s *Server) Dispatcher() *jsonrpc.Dispatcher { return s.dispatcher }

// Addr returns the effective listen address.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Close releases a server that was constructed but never served. It is safe
// to call more than once. A running server is stopped through Serve's context.
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.listener.Close()
		if errors.Is(s.closeErr, net.ErrClosed) {
			s.closeErr = nil
		}
		s.pool.Stop()
	})
	return s.closeErr
}

// Serve blocks until ctx is cancelled or the event loop fails. When ctx is
// cancelled it broadcasts a going-away notification to live push sessions,
// waits for the event loop to stop and for queued requests to drain before
// returning, so callers (gosvc.App.Run) do not exit the process while
// in-flight requests are still running.
func (s *Server) Serve(ctx context.Context) error {
	shutdownSignal, cancelShutdown := context.WithCancel(ctx)
	defer cancelShutdown()
	done := make(chan struct{})
	serveDone := make(chan struct{})
	go func() {
		defer close(done)
		<-shutdownSignal.Done()
		s.draining.Store(true)
		s.notifyShutdown()
		if ctx.Err() != nil {
			// Give live sessions a chance to flush going-away frames. When
			// Serve already failed, there is no reason to wait out the grace.
			select {
			case <-time.After(shutdownGrace):
			case <-serveDone:
			}
		}
		// Closing the original listener makes a cancellation that beats
		// ConvertListener fail promptly. Conversion duplicates the fd, so
		// Shutdown is still needed for a loop that has already started.
		_ = s.listener.Close()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout.D())
			err := s.eventLoop.Shutdown(shutdownCtx)
			cancel()
			if err != nil {
				s.logger.Warn("tcp shutdown returned error", "error", err)
			}
			select {
			case <-serveDone:
				// Shutdown before Serve installs its internal server is a no-op
				// in netpoll v0.7.5. Retry until Serve actually exits.
				s.pool.Stop()
				s.closeSessions()
				return
			case <-ticker.C:
			}
		}
	}()

	err := s.eventLoop.Serve(s.listener)
	close(serveDone)
	// A failed (or otherwise early) loop exit must join the shutdown goroutine
	// before App can release resources owned by in-flight handlers.
	cancelShutdown()
	<-done
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("tcp serve: %w", err)
	}
	return nil
}

// notifyShutdown enqueues the going-away notification on every live session,
// best-effort: sessions with a full queue simply miss it.
func (s *Server) notifyShutdown() {
	s.sessions.Range(func(key, value any) bool {
		if session, ok := value.(*Session); ok {
			_ = session.enqueue(s.shutdownFrame)
		}
		return true
	})
}

// closeSessions tears down every live session (used on shutdown).
func (s *Server) closeSessions() {
	s.sessions.Range(func(key, value any) bool {
		if session, ok := value.(*Session); ok {
			session.close()
		}
		return true
	})
}

// addSession registers a live session for the shutdown broadcast.
func (s *Server) addSession(session *Session) { s.sessions.Store(session, struct{}{}) }

// removeSession drops a session from the registry.
func (s *Server) removeSession(session *Session) { s.sessions.Delete(session) }

// handleRequest runs on netpoll's per-connection reader goroutine: it must
// only read and dispatch, not run business handlers.
func (s *Server) handleRequest(ctx context.Context, connection netpoll.Connection) error {
	if s.ready != nil && !s.ready.IsReady() {
		// Answer once, then close: leaving the input unread would spin the
		// event loop (netpoll is level-triggered) and flood the client with
		// busy frames. The client reconnects when the service is ready.
		s.reject(ctx, connection, notReadyError(), notReadyFrame)
		_ = connection.Close()
		return nil
	}
	if s.limiter != nil && !s.limiter.Allow(clientKey(connection)) {
		s.reject(ctx, connection, busyError(), busyFrame)
		_ = connection.Close()
		return nil
	}

	state, _ := ctx.Value(connStateKey{}).(*connState)
	if state == nil {
		_ = connection.Close()
		return nil
	}

	// Inspect only the current frame. A read buffer may contain many complete
	// short frames, whose aggregate length can exceed MaxFrameBytes.
	body, tooLarge, err := s.readFrame(connection, state)
	if tooLarge {
		s.reject(ctx, connection, tooLargeError(), tooLargeFrame)
		_ = connection.Close()
		return nil
	}
	if err != nil {
		// The connection died while reading; netpoll will close it.
		return nil
	}
	if body == nil {
		// The currently buffered bytes were consumed into the bounded partial
		// frame. A future read will resume when more bytes arrive.
		state.armFrameTimeout(s.cfg.ReadTimeout.D())
		return nil
	}
	state.stopFrameTimeout()

	if err := s.dispatch(connection, ctx, body); err != nil {
		s.reject(ctx, connection, busyError(), busyFrame)
	}
	return nil
}

func (state *connState) armFrameTimeout(timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	if state.frameTimer == nil {
		state.frameTimer = time.AfterFunc(timeout, func() { _ = state.conn.Close() })
		return
	}
	state.frameTimer.Reset(timeout)
}

func (state *connState) stopFrameTimeout() {
	if state.frameTimer != nil {
		state.frameTimer.Stop()
	}
}

// readFrame consumes only bytes from the current line. The returned body is
// detached from netpoll's buffer and includes the newline. A nil body means
// that the line is incomplete. At most MaxFrameBytes of a partial frame are
// retained between calls; a following byte without a newline is rejected.
func (s *Server) readFrame(connection netpoll.Connection, state *connState) (body []byte, tooLarge bool, err error) {
	reader := connection.Reader()
	available := reader.Len()
	if available == 0 {
		return nil, false, nil
	}
	remaining := s.cfg.MaxFrameBytes - len(state.partial)
	if remaining <= 0 {
		return nil, true, nil
	}
	inspect := min(available, remaining)
	chunk, err := reader.Peek(inspect)
	if err != nil {
		return nil, false, err
	}
	if newline := bytes.IndexByte(chunk, '\n'); newline >= 0 {
		frameBytes := newline + 1
		body = make([]byte, len(state.partial)+frameBytes)
		copy(body, state.partial)
		copy(body[len(state.partial):], chunk[:frameBytes])
		if err = reader.Skip(frameBytes); err != nil {
			return nil, false, err
		}
		if err = reader.Release(); err != nil {
			return nil, false, err
		}
		state.partial = nil
		return body, false, nil
	}
	if available > remaining {
		return nil, true, nil
	}
	state.partial = append(state.partial, chunk...)
	if err = reader.Skip(available); err != nil {
		return nil, false, err
	}
	return nil, false, reader.Release()
}

// dispatch hands one frame to the worker pool. A serial codec queues later
// frames for the same connection without occupying workers while they wait.
func (s *Server) dispatch(connection netpoll.Connection, ctx context.Context, body []byte) error {
	task := func() { s.process(connection, ctx, body) }
	if !s.serialPerConn {
		return s.pool.Submit(task)
	}

	state, _ := ctx.Value(connStateKey{}).(*connState)
	if state == nil {
		return s.pool.Submit(task)
	}

	return s.pool.SubmitSerial(state, task)
}

// reject writes a protocol-level error frame: through the codec when one is
// installed, else the JSON-RPC constant. It also records the cause so
// OnDisconnect can report it. It runs on the event loop for the server-level
// frames; codec.Encode must be fast and non-blocking.
func (s *Server) reject(ctx context.Context, connection netpoll.Connection, cause error, fallback []byte) {
	if state, _ := ctx.Value(connStateKey{}).(*connState); state != nil {
		state.closeReason.Store(&cause)
	}
	if s.codec != nil {
		frame, err := s.codec.Encode(Call{}, nil, cause)
		if err != nil {
			s.logger.Warn("tcp codec failed to encode protocol error", "error", err, "remote", connection.RemoteAddr().String())
			return
		}
		s.write(ctx, connection, frame)
		return
	}
	s.write(ctx, connection, fallback)
}

// clientKey returns the remote address without the ephemeral port so the rate
// limiter buckets per client, not per connection.
func clientKey(connection netpoll.Connection) string {
	if host, _, err := net.SplitHostPort(connection.RemoteAddr().String()); err == nil {
		return host
	}
	return connection.RemoteAddr().String()
}

// process runs on a worker goroutine and may block on business logic.
func (s *Server) process(connection netpoll.Connection, parent context.Context, body []byte) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), s.cfg.HandlerTimeout.D())
	defer cancel()
	ctx = logging.WithRequestID(ctx, logging.NewRequestID())
	// Transport identity (for middleware branching) and the push sink (for
	// subscribe-style handlers) are part of every TCP request context.
	ctx = jsonrpc.WithTransport(ctx, "tcp")
	if state, ok := parent.Value(connStateKey{}).(*connState); ok {
		ctx = push.WithSink(ctx, connSink{state: state})
	}

	var span trace.Span
	if s.tracer != nil {
		ctx, span = s.tracer.Start(ctx, "tcp.request",
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("network.transport", "tcp"),
				attribute.String("rpc.system", "jsonrpc"),
				attribute.String("server.address", connection.RemoteAddr().String()),
			))
		ctx = logging.WithTrace(ctx)
	}
	if span != nil {
		// Registered before the recover below so it runs after it: the span
		// status is set before the span ends.
		defer span.End()
	}

	defer func() {
		if r := recover(); r != nil {
			// A panicking handler must not leave the client waiting for a
			// response until its own timeout; answer with an internal error
			// frame (the request id is lost at this point) and keep the span
			// and logs informative. The worker pool survives either way.
			s.logger.Error("tcp handler panic",
				"panic", r,
				"remote", connection.RemoteAddr().String(),
			)
			if span != nil {
				span.SetStatus(codes.Error, "panic")
			}
			s.reject(ctx, connection, internalError(), internalFrame)
		}
	}()

	if s.codec != nil {
		s.processCoded(ctx, connection, body)
		return
	}

	response, ok := s.dispatcher.Serve(ctx, body)
	if s.recorder != nil {
		_ = s.recorder.Incr(ctx, "tcp.requests", 1)
	}
	if !ok {
		return
	}
	s.write(ctx, connection, response)
}

// processCoded is the custom-codec path: decode the frame, dispatch without
// the JSON-RPC envelope, render the reply in the dialect. Custom codecs are
// single frame, single call; batching stays on the default path.
func (s *Server) processCoded(ctx context.Context, connection netpoll.Connection, body []byte) {
	if s.recorder != nil {
		_ = s.recorder.Incr(ctx, "tcp.requests", 1)
	}

	call, err := s.codec.Decode(body)
	if err != nil {
		// A decode failure is protocol-level: the codec authored the error,
		// so it also renders it.
		s.writeCodedError(ctx, connection, Call{}, err)
		return
	}

	result, invokeErr := s.dispatcher.Invoke(ctx, call.Method, call.Params)
	if call.Notification {
		// Dispatched, never answered.
		return
	}
	if invokeErr != nil && !errors.Is(invokeErr, jsonrpc.ErrMethodNotFound) {
		// Unknown errors are internal: codecs render apierror messages, so
		// wrap raw handler errors to keep internal details off the wire.
		if _, ok := errors.AsType[*apierror.Error](invokeErr); !ok {
			invokeErr = apierror.New(apierror.KindInternal, apierror.ClientMessage(invokeErr))
		}
	}
	if invokeErr != nil {
		s.writeCodedError(ctx, connection, call, invokeErr)
		return
	}

	frame, err := s.codec.Encode(call, result, nil)
	if err != nil {
		s.logger.Warn("tcp codec failed to encode response", "error", err, "remote", connection.RemoteAddr().String())
		return
	}
	s.write(ctx, connection, frame)
}

// writeCodedError renders callErr through the codec; it never uses the
// JSON-RPC constants, because the dialect owns its error shape.
func (s *Server) writeCodedError(ctx context.Context, connection netpoll.Connection, call Call, callErr error) {
	frame, err := s.codec.Encode(call, nil, callErr)
	if err != nil {
		s.logger.Warn("tcp codec failed to encode error", "error", err, "remote", connection.RemoteAddr().String())
		return
	}
	s.write(ctx, connection, frame)
}

// write serializes writes per connection, as required by netpoll.
func (s *Server) write(ctx context.Context, connection netpoll.Connection, payload []byte) {
	if !connection.IsActive() {
		return
	}
	state, _ := ctx.Value(connStateKey{}).(*connState)
	if state == nil {
		return
	}

	frame := make([]byte, 0, len(payload)+1)
	frame = append(frame, payload...)
	frame = append(frame, '\n')

	state.writeMu.Lock()
	defer state.writeMu.Unlock()
	if _, err := connection.Write(frame); err != nil {
		s.logger.Debug("tcp write failed", "error", err, "remote", connection.RemoteAddr().String())
	}
}

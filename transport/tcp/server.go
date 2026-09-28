// Package tcp serves line-delimited JSON-RPC 2.0 over netpoll.
//
// Design notes:
//   - requests are read on the netpoll event loop (one OnRequest at a time per
//     connection) and handed to a bounded worker pool, so slow handlers never
//     block the event loop and the queue provides explicit backpressure;
//   - netpoll requires serialized writes per connection, so every connection
//     carries a write mutex in its context;
//   - frames are length-limited BEFORE they are buffered: Until accumulates
//     the whole line in memory, so a length check after the fact would let a
//     hostile client grow the read buffer without bound;
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

	// shutdownFrame is the going-away frame rendered in the active dialect.
	shutdownFrame []byte
	// draining marks the shutdown phase, so OnDisconnect reports
	// ErrServerShutdown for connections closed during the drain.
	draining atomic.Bool

	// sessions tracks live push sessions for the shutdown broadcast.
	sessions sync.Map
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

	// connInfo is the lifecycle-callback view of the connection; it is only
	// set when callbacks are installed, so OnDisconnect can distinguish
	// prepared connections.
	connInfo *Conn
	// closeReason is set (before the close) for server-initiated
	// disconnects, so OnDisconnect can report the cause. Peer-initiated
	// closes leave it nil.
	closeReason atomic.Pointer[error]
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
		return nil, fmt.Errorf("netpoll event loop: %w", err)
	}
	s.eventLoop = eventLoop
	return s, nil
}

// Dispatcher exposes the shared JSON-RPC dispatcher.
func (s *Server) Dispatcher() *jsonrpc.Dispatcher { return s.dispatcher }

// Addr returns the effective listen address.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Serve blocks until ctx is cancelled or the event loop fails. When ctx is
// cancelled it broadcasts a going-away notification to live push sessions,
// waits for the event loop to stop and for queued requests to drain before
// returning, so callers (gosvc.App.Run) do not exit the process while
// in-flight requests are still running.
func (s *Server) Serve(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		s.draining.Store(true)
		s.notifyShutdown()
		time.Sleep(shutdownGrace) // let pumps flush the going-away frames
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout.D())
		defer cancel()
		if err := s.eventLoop.Shutdown(shutdownCtx); err != nil {
			s.logger.Warn("tcp shutdown returned error", "error", err)
		}
		s.pool.Stop()
		s.closeSessions()
	}()

	err := s.eventLoop.Serve(s.listener)
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("tcp serve: %w", err)
	}
	// eventLoop.Serve returns when the loop stops; the shutdown goroutine is
	// still draining the worker pool and pushing sessions, so join it.
	<-done
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

// handleRequest runs on the netpoll event loop: it must only read and dispatch.
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

	// Bound the line before it is buffered. Until accumulates the entire line
	// in memory, so MaxFrameBytes must be enforced on the amount currently
	// buffered, not only on the completed line. The line includes the
	// trailing newline, so a frame of exactly MaxFrameBytes still passes.
	if connection.Reader().Len() > s.cfg.MaxFrameBytes {
		s.reject(ctx, connection, tooLargeError(), tooLargeFrame)
		_ = connection.Close()
		return nil
	}

	line, err := connection.Reader().Until('\n')
	if err != nil {
		// No newline yet (or the connection died): the partial frame stays
		// buffered and OnRequest fires again when more data arrives.
		return nil
	}

	if len(line) > s.cfg.MaxFrameBytes {
		s.reject(ctx, connection, tooLargeError(), tooLargeFrame)
		_ = connection.Close()
		return nil
	}

	// The slice is only valid until Release, so copy before handing it off.
	body := make([]byte, len(line))
	copy(body, line)
	_ = connection.Reader().Release()

	if err := s.pool.Submit(func() { s.process(connection, ctx, body) }); err != nil {
		s.reject(ctx, connection, busyError(), busyFrame)
	}
	return nil
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

// Package tcp serves line-delimited JSON-RPC 2.0 over netpoll.
//
// Design notes:
//   - requests are read on the netpoll event loop (one OnRequest at a time per
//     connection) and handed to a bounded worker pool, so slow handlers never
//     block the event loop and the queue provides explicit backpressure;
//   - netpoll requires serialized writes per connection, so every connection
//     carries a write mutex in its context;
//   - TLS is intentionally not handled here: terminate it at a gateway and use
//     this listener for internal traffic.
package tcp

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/cloudwego/netpoll"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"example.com/gosvc/config"
	"example.com/gosvc/health"
	"example.com/gosvc/internal/workerpool"
	"example.com/gosvc/jsonrpc"
	"example.com/gosvc/logging"
	"example.com/gosvc/observability"
	"example.com/gosvc/ratelimit"
	"example.com/gosvc/store"
)

const requestTimeout = 5 * time.Second

var (
	busyFrame     = []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32004,"message":"server busy"}}`)
	notReadyFrame = []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32004,"message":"service not ready"}}`)
	tooLargeFrame = []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"frame too large"}}`)
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
}

// Server is the netpoll based JSON-RPC server.
type Server struct {
	eventLoop  netpoll.EventLoop
	listener   net.Listener
	pool       *workerpool.Pool
	dispatcher *jsonrpc.Dispatcher
	cfg        config.TCPConfig
	logger     *slog.Logger
	limiter    *ratelimit.Limiter
	recorder   store.Recorder
	ready      *health.Ready
	tracer     trace.Tracer
}

type connState struct {
	writeMu sync.Mutex
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
		limiter:    opts.Limiter,
		recorder:   opts.Recorder,
		ready:      opts.Ready,
		tracer:     opts.Tracer,
	}
	s.pool.SetPanicHandler(func(recovered any) {
		opts.Logger.Error("tcp worker panic", "panic", recovered)
	})

	eventLoop, err := netpoll.NewEventLoop(
		s.handleRequest,
		netpoll.WithOnPrepare(func(netpoll.Connection) context.Context {
			return context.WithValue(context.Background(), connStateKey{}, &connState{})
		}),
		netpoll.WithReadTimeout(opts.Config.TCP.ReadTimeout.D()),
	)
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

// Serve blocks until ctx is cancelled or the event loop fails.
func (s *Server) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout.D())
		defer cancel()
		if err := s.eventLoop.Shutdown(shutdownCtx); err != nil {
			s.logger.Warn("tcp shutdown returned error", "error", err)
		}
		s.pool.Stop()
	}()

	err := s.eventLoop.Serve(s.listener)
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("tcp serve: %w", err)
	}
	return nil
}

// handleRequest runs on the netpoll event loop: it must only read and dispatch.
func (s *Server) handleRequest(ctx context.Context, connection netpoll.Connection) error {
	if s.ready != nil && !s.ready.IsReady() {
		s.write(ctx, connection, notReadyFrame)
		return nil
	}
	if s.limiter != nil && !s.limiter.Allow(connection.RemoteAddr().String()) {
		s.write(ctx, connection, busyFrame)
		return nil
	}

	line, err := connection.Reader().Until('\n')
	if err != nil {
		// Read timeout or closed connection: netpoll will call OnRequest again
		// when more data arrives. Partial frames without a newline are dropped.
		return nil
	}

	if len(line) > s.cfg.MaxFrameBytes {
		s.write(ctx, connection, tooLargeFrame)
		_ = connection.Close()
		return nil
	}

	// The slice is only valid until Release, so copy before handing it off.
	body := make([]byte, len(line))
	copy(body, line)
	_ = connection.Reader().Release()

	if err := s.pool.Submit(func() { s.process(connection, ctx, body) }); err != nil {
		s.write(ctx, connection, busyFrame)
	}
	return nil
}

// process runs on a worker goroutine and may block on business logic.
func (s *Server) process(connection netpoll.Connection, parent context.Context, body []byte) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), requestTimeout)
	defer cancel()
	ctx = logging.WithRequestID(ctx, logging.NewRequestID())

	if s.tracer != nil {
		var span trace.Span
		ctx, span = s.tracer.Start(ctx, "tcp.request",
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("network.transport", "tcp"),
				attribute.String("rpc.system", "jsonrpc"),
				attribute.String("server.address", connection.RemoteAddr().String()),
			))
		defer span.End()
		ctx = logging.WithTrace(ctx)

		defer func() {
			if recover() != nil {
				span.SetStatus(codes.Error, "panic")
			}
		}()
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

package tcp

import (
	"context"
	"errors"

	"github.com/cloudwego/netpoll"
)

// Connection lifecycle callbacks.
//
// Push sessions are created lazily and reaped by the pump, which is right for
// connections that may subscribe at some point — but registries that key on
// connections (for example a mining pool's worker set) need a reliable
// create/destroy pair, not opportunistic pruning. Callbacks provide exactly
// that pair; they run on the netpoll event loop goroutine, so they must be
// fast and non-blocking:
//
//   - OnConnect runs after the connection is prepared and before the first
//     request; the returned context becomes the parent of every request on
//     that connection (request id and trace context are derived from it);
//   - OnDisconnect runs exactly once when the connection ends, no matter
//     how: client hangup, server-side error frame, push policy or shutdown.
//     reason is best-effort: nil for peer-initiated closes, a protocol-level
//     error for server-initiated ones, ErrServerShutdown while draining.
//
// When callbacks are set, every connection gets its push Session eagerly, so
// OnConnect can hand out the push handle immediately; without callbacks the
// lazy behaviour is kept (connections that never subscribe pay nothing).
// A panic in OnConnect recovers by closing the connection; a panic in
// OnDisconnect is logged and the server keeps running.

// ErrServerShutdown is the OnDisconnect reason while the server is draining
// connections during graceful shutdown.
var ErrServerShutdown = errors.New("tcp: server shutdown")

// Conn describes one live connection for the lifecycle callbacks. The same
// *Conn pointer is handed to OnConnect and OnDisconnect, so it can be used
// as a registry key.
type Conn struct {
	RemoteAddr string
	LocalAddr  string
	Session    *Session
}

// Callbacks observes connection lifecycle events.
type Callbacks interface {
	// OnConnect runs after the connection is established and before the
	// first request; the returned context parents all requests on the
	// connection.
	OnConnect(ctx context.Context, conn *Conn) context.Context
	// OnDisconnect runs exactly once when the connection ends.
	OnDisconnect(ctx context.Context, conn *Conn, reason error)
}

// onPrepare prepares the per-connection state and context. With callbacks
// installed it eagerly creates the session and runs OnConnect; a panic there
// closes the connection instead of taking down the event loop.
//
// netpoll distinguishes two close paths: the event-loop OnDisconnect only
// fires when the peer goes away, while per-connection close callbacks run
// exactly once no matter who closed (peer EOF, a reject frame, the push
// policy or shutdown). The reliable cleanup contract is the latter, so the
// callbacks are registered through AddCloseCallback.
func (s *Server) onPrepare(connection netpoll.Connection) context.Context {
	state := &connState{server: s, conn: connection, remote: connection.RemoteAddr().String()}
	ctx := context.WithValue(context.Background(), connStateKey{}, state)

	// The serial-dispatch chain starts "previous already completed" so the
	// first frame never waits on a zero-value channel.
	firstDone := make(chan struct{})
	close(firstDone)
	state.prevDone = firstDone

	if s.callbacks != nil {
		state.ensureSession()
		conn := &Conn{
			RemoteAddr: state.remote,
			LocalAddr:  connection.LocalAddr().String(),
			Session:    state.session,
		}
		state.connInfo = conn

		cbCtx, panicked := s.safeOnConnect(ctx, conn)
		if panicked != nil {
			_ = connection.Close()
			return ctx
		}
		if cbCtx != nil {
			ctx = cbCtx
		}
		// The connection state must survive whatever the callback returned.
		ctx = context.WithValue(ctx, connStateKey{}, state)

		// OnDisconnect fires exactly once when the connection ends, however
		// it ends. It runs after netpoll's own request processing finished;
		// handlers submitted to the worker pool may still be in flight.
		_ = connection.AddCloseCallback(func(netpoll.Connection) error {
			s.safeOnDisconnect(ctx, state)
			return nil
		})
	}
	return ctx
}

func (s *Server) safeOnConnect(ctx context.Context, conn *Conn) (cbCtx context.Context, panicked any) {
	defer func() {
		if r := recover(); r != nil {
			panicked = r
			s.logger.Error("tcp OnConnect panicked, closing connection", "panic", r, "remote", conn.RemoteAddr)
		}
	}()
	cbCtx = s.callbacks.OnConnect(ctx, conn)
	return cbCtx, nil
}

func (s *Server) safeOnDisconnect(ctx context.Context, state *connState) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("tcp OnDisconnect panicked", "panic", r, "remote", state.remote)
		}
	}()
	s.callbacks.OnDisconnect(ctx, state.connInfo, s.disconnectReason(state))
}

// disconnectReason returns the best-effort cause of the connection ending:
// nil when the peer simply went away, the protocol-level error behind a
// server-initiated close, or ErrServerShutdown while draining.
func (s *Server) disconnectReason(state *connState) error {
	if reason := state.closeReason.Load(); reason != nil {
		return *reason
	}
	if s.draining.Load() {
		return ErrServerShutdown
	}
	return nil
}

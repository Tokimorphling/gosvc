package tcp

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/Tokimorphling/gosvc/jsonrpc"
)

// Push semantics for the TCP transport.
//
// Inbound requests stay exactly as they were: the dispatcher is
// request/response and knows nothing about connections. Push is a separate,
// explicit capability: a handler that wants to receive notifications for the
// connection it is serving asks tcp.SessionFromContext(ctx) and keeps the
// returned *Session beyond the request. Every session owns a bounded send
// queue drained by its own pump goroutine, so a slow consumer can never
// unbalance the event loop, the worker pool or other sessions — it only ever
// blocks its own queue, and the configured policy (drop or disconnect) applies
// when that queue is full.
//
// Lifecycle:
//   - the session is created lazily on first use, one per connection;
//   - Notify after the connection died returns ErrSessionClosed (cheap
//     IsActive check plus a reaper in the pump goroutine);
//   - on shutdown the server broadcasts a going-away notification
//     (gosvc.shutdown) before the event loop stops;
//   - metrics: gosvc_notify_sent_total / gosvc_notify_dropped_total.

var (
	// ErrSessionClosed is returned when the connection behind the session is
	// gone (or the server is shutting down).
	ErrSessionClosed = errors.New("tcp: session is closed")
	// ErrNotifyDropped is returned when the send queue is full and the
	// configured policy is drop.
	ErrNotifyDropped = errors.New("tcp: notification dropped, send queue is full")
)

// MethodShutdown is the notification method the server broadcasts before it
// stops draining TCP connections. Clients should treat it as a signal to
// reconnect elsewhere.
const MethodShutdown = "gosvc.shutdown"

// sessionReapInterval is how often an idle pump goroutine re-checks whether
// its connection is still alive, so abandoned sessions do not linger.
const sessionReapInterval = 2 * time.Second

// shutdownGrace gives pump goroutines a moment to flush going-away frames
// before the event loop closes the listeners' connections.
const shutdownGrace = 100 * time.Millisecond

// Session sends JSON-RPC notifications to one TCP client. It is safe for
// concurrent use and cheap to keep beyond the request that created it.
type Session struct {
	server *Server
	state  *connState
	queue  chan []byte
	done   chan struct{}
	closed atomic.Bool
}

// SessionFromContext returns the push session of the TCP connection serving
// ctx, or nil when ctx does not belong to the TCP transport. The session is
// created on first use; requests that never call this pay nothing.
//
// Typical usage inside a "subscribe" handler:
//
//	session := tcp.SessionFromContext(ctx)
//	if session == nil {
//		return nil, apierror.New(apierror.KindInvalidArgument, "subscription requires the TCP transport")
//	}
//	broker.Add(session)
func SessionFromContext(ctx context.Context) *Session {
	if ctx == nil {
		return nil
	}
	state, ok := ctx.Value(connStateKey{}).(*connState)
	if !ok || state == nil || state.server == nil {
		return nil
	}
	return state.ensureSession()
}

// Notify encodes params and queues a JSON-RPC notification for this client.
// It never blocks: when the queue is full the configured policy applies
// (drop: ErrNotifyDropped and the metric increments; disconnect: the
// connection is closed and ErrSessionClosed is returned). When the connection
// is gone it returns ErrSessionClosed. Use NotifyTyped for the typed form.
func (s *Session) Notify(method string, params any) error {
	if s == nil {
		return ErrSessionClosed
	}
	frame, err := jsonrpc.EncodeNotification(method, params)
	if err != nil {
		return err
	}
	return s.enqueue(frame)
}

// NotifyTyped is the typed form of Notify; the type parameters exist for API
// symmetry with RegisterTyped on the sending side.
func (s *Session) NotifyTyped[Params any](method string, params Params) error {
	return s.Notify(method, params)
}

// RemoteAddr returns the client address of the session.
func (s *Session) RemoteAddr() string {
	if s == nil || s.state == nil {
		return ""
	}
	return s.state.remote
}

// Close marks the session closed, closes the underlying connection and stops
// the pump. Subsequent Notify calls return ErrSessionClosed.
func (s *Session) Close() error {
	if s == nil {
		return nil
	}
	s.server.removeSession(s)
	s.close()
	return nil
}

func (s *Session) enqueue(frame []byte) error {
	if s.closed.Load() {
		s.server.metrics.ObserveNotifyDropped("tcp", "closed")
		return ErrSessionClosed
	}
	select {
	case s.queue <- frame:
		s.server.metrics.ObserveNotifySent("tcp")
		return nil
	default:
		// Queue full: apply the configured slow-consumer policy.
		s.server.metrics.ObserveNotifyDropped("tcp", "queue_full")
		if s.server.cfg.NotifyPolicy == "disconnect" {
			_ = s.Close()
			return ErrSessionClosed
		}
		return ErrNotifyDropped
	}
}

// close tears the session down without touching the registry; used both by
// Close and by the server on shutdown. Idempotent.
func (s *Session) close() {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	s.state.sessionMu.Lock()
	if s.state.session == s {
		s.state.session = nil
	}
	s.state.sessionMu.Unlock()
	// close(done) wakes the pump immediately; it drains whatever is still
	// queued, but writes to a dead connection fail harmlessly.
	closeOnce(s.done)
	_ = s.state.conn.Close()
}

func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// pump drains the send queue onto the connection, one frame at a time and
// serialized with request responses through connState.writeMu, until the
// session is closed or the connection dies.
func (s *Session) pump() {
	ticker := time.NewTicker(sessionReapInterval)
	defer ticker.Stop()

	for {
		select {
		case frame := <-s.queue:
			if err := s.writeFrame(frame); err != nil {
				s.server.logger.Debug("tcp notify write failed", "error", err, "remote", s.state.remote)
				_ = s.Close()
				return
			}
		case <-s.done:
			// Best-effort drain, then stop: the connection is closing anyway.
			for {
				select {
				case frame := <-s.queue:
					if err := s.writeFrame(frame); err != nil {
						return
					}
				default:
					return
				}
			}
		case <-ticker.C:
			if !s.state.conn.IsActive() {
				_ = s.Close()
				return
			}
		}
	}
}

// writeFrame appends the newline delimiter and writes under the per-connection
// write mutex, as netpoll requires.
func (s *Session) writeFrame(frame []byte) error {
	payload := make([]byte, 0, len(frame)+1)
	payload = append(payload, frame...)
	payload = append(payload, '\n')

	s.state.writeMu.Lock()
	defer s.state.writeMu.Unlock()
	_, err := s.state.conn.Write(payload)
	return err
}

// ensureSession returns the connection's session, creating it (and starting
// the pump goroutine) on first use. It is called from worker goroutines, so
// creation is serialised.
func (state *connState) ensureSession() *Session {
	state.sessionMu.Lock()
	defer state.sessionMu.Unlock()
	if state.session != nil {
		return state.session
	}
	session := &Session{
		server: state.server,
		state:  state,
		queue:  make(chan []byte, state.server.cfg.NotifyQueueSize),
		done:   make(chan struct{}),
	}
	state.session = session
	state.server.addSession(session)
	go session.pump()
	return session
}

// encodeShuttingDownFrame is precomputed so the shutdown broadcast stays
// allocation-light across many sessions.
var shuttingDownFrame = func() []byte {
	raw, err := jsonrpc.EncodeNotification(MethodShutdown, nil)
	if err != nil {
		panic(err) // unreachable: nil params always encode
	}
	return raw
}()

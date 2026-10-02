package tcp

import (
	"errors"

	"github.com/Tokimorphling/gosvc/push"
)

// Sink adapters and transport identity injection for the generic push
// broker. TCP requests always carry a push.Sink in their context: resolving
// it is free, while Send, Done and Close lazily create the push session like
// tcp.SessionFromContext would.

// AsSink adapts the session to a push.Sink.
func (s *Session) AsSink() push.Sink {
	if s == nil {
		return nil
	}
	return sessionSink{s}
}

type sessionSink struct {
	session *Session
}

func (x sessionSink) Send(event string, data any) error {
	return notifySink(x.session, event, data)
}
func (x sessionSink) Done() <-chan struct{} { return x.session.done }
func (x sessionSink) Close() error          { return x.session.Close() }
func (x sessionSink) ID() string            { return "tcp/" + x.session.state.remote }

// connSink is the context-injected sink: it resolves the connection's push
// session lazily when Send, Done or Close is called.
type connSink struct {
	state *connState
}

func (x connSink) session() *Session { return x.state.ensureSession() }

func (x connSink) Send(event string, data any) error {
	return notifySink(x.session(), event, data)
}
func (x connSink) Done() <-chan struct{} { return x.session().done }
func (x connSink) Close() error          { return x.session().Close() }
func (x connSink) ID() string            { return "tcp/" + x.state.remote }

func notifySink(s *Session, event string, data any) error {
	err := s.Notify(event, data)
	if errors.Is(err, ErrNotifyDropped) {
		return push.ErrDropped
	}
	return err
}

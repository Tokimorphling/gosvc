package push

import "context"

// StreamSink adapts any server stream to a Sink. It is structurally typed: a
// gRPC ServerStream satisfies it without importing gRPC here, and so does
// anything else with the same shape.
//
// The event name is not carried on the wire for stream sinks: the stream's
// message type already defines the shape, so Send delivers the payload as
// the stream message. Close is a no-op — the pump exits on the first failed
// Send or on stream cancellation through Done.
type StreamSink struct {
	Stream interface {
		Context() context.Context
		SendMsg(any) error
	}
	// Ident identifies the sink in logs; typically the RPC method.
	Ident string
}

// Send implements Sink by sending data as one stream message.
func (s StreamSink) Send(_ string, data any) error {
	return s.Stream.SendMsg(data)
}

// Done implements Sink: the stream's context cancellation.
func (s StreamSink) Done() <-chan struct{} {
	return s.Stream.Context().Done()
}

// Close implements Sink as a no-op.
func (s StreamSink) Close() error { return nil }

// ID implements Sink.
func (s StreamSink) ID() string { return s.Ident }

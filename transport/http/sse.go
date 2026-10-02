package http

import (
	"context"
	"time"

	"github.com/bytedance/sonic"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	hertzsse "github.com/cloudwego/hertz/pkg/protocol/sse"

	"github.com/Tokimorphling/gosvc/observability"
	"github.com/Tokimorphling/gosvc/push"
)

// Server-sent events.
//
// SSE is the HTTP way to push: it stays a plain GET route, so the existing
// middleware chain keeps applying (authentication, rate limiting, tracing,
// access logs) and browsers consume it natively with EventSource. The wire
// handling is delegated to Hertz's protocol/sse package; gosvc only adds the
// lifecycle conventions:
//
//   - the handler owns a *SSEStream and runs until it returns;
//   - Done() fires when the client disconnects (gosvc enables Hertz's
//     connection-close detection) or the runtime shuts down, so the handler
//     can exit and the request log entry is written;
//   - errors from Send/Ping mean the client is gone: stop and return.
//
// Slow consumers are cut off by the http write timeout: a frame that cannot
// be flushed within it fails, the handler returns and the connection closes.
// For long-lived streams send heartbeats (Ping) from time to time so proxies
// do not time the connection out.

// sseHeartbeat is the recommended keep-alive interval for SSE handlers,
// exported as SSEHeartbeat.
const sseHeartbeat = 15 * time.Second

// SSEHeartbeat is the recommended keep-alive interval between application
// events, keeping intermediaries from closing an otherwise idle connection.
const SSEHeartbeat = sseHeartbeat

// SSEStream writes server-sent events to one connected client.
type SSEStream struct {
	ctx     context.Context
	cancel  context.CancelFunc
	writer  *hertzsse.Writer
	metrics *observability.Metrics
	remote  string
}

// Send writes one event with the given event type and JSON-encoded data.
func (s *SSEStream) Send(event string, data any) error {
	raw, err := sonic.Marshal(data)
	if err != nil {
		return err
	}
	if err := s.writer.WriteEvent("", event, raw); err != nil {
		s.metrics.ObserveNotifyDropped("sse", "", "write_error")
		return err
	}
	s.metrics.ObserveNotifySent("sse", "")
	return nil
}

// Ping writes a comment frame that keeps intermediaries from closing an
// otherwise idle connection. Clients ignore it.
func (s *SSEStream) Ping() error {
	return s.writer.WriteKeepAlive()
}

// RemoteAddr returns the client address of the stream.
func (s *SSEStream) RemoteAddr() string {
	if s == nil {
		return ""
	}
	return s.remote
}

// Done is closed when the client disconnects or the runtime shuts down; the
// handler should return promptly afterwards. A client that vanishes without
// closing the TCP connection (half-open link, network partition) is not
// distinguishable from a silent one: only the next failed Send or Ping
// detects it, so heartbeats bound that delay.
func (s *SSEStream) Done() <-chan struct{} { return s.ctx.Done() }

// Close terminates the stream end to end: the handler's context cancels, so
// Done fires and the request finishes. It is the sink termination for the
// broker's Disconnect policy.
func (s *SSEStream) Close() error {
	s.cancel()
	return nil
}

// AsSink adapts the stream to a push.Sink, so a push.Broker can fan events
// out to browsers through the same API it uses for TCP sessions. The broker
// owns the writes; the handler subscribes and then waits on Done.
func (s *SSEStream) AsSink() push.Sink {
	if s == nil {
		return nil
	}
	return sseSink{s}
}

type sseSink struct {
	stream *SSEStream
}

func (x sseSink) Send(event string, data any) error { return x.stream.Send(event, data) }
func (x sseSink) Done() <-chan struct{}             { return x.stream.Done() }
func (x sseSink) Close() error                      { return x.stream.Close() }
func (x sseSink) ID() string                        { return "sse/" + x.stream.RemoteAddr() }

// RegisterSSE serves server-sent events at path. handler runs for the whole
// lifetime of the streaming response, on the request's goroutine, and should
// loop until Done() or a failed Send/Ping. metrics may be nil.
// A comment frame is flushed before handler runs, so clients receive the
// response headers even when the handler has no event to send yet.
//
// RegisterSSE is used inside gosvc.App.RegisterHTTP:
//
//	application.RegisterHTTP(func(h *server.Hertz) {
//		httptransport.RegisterSSE(h, "/api/v1/events", application.Metrics(),
//			func(ctx context.Context, stream *httptransport.SSEStream) {
//				if err := stream.Send("greet", map[string]string{"hello": "stream"}); err != nil {
//					return
//				}
//				heartbeat := time.NewTicker(httptransport.SSEHeartbeat)
//				defer heartbeat.Stop()
//				for {
//					select {
//					case <-stream.Done():
//						return
//					case <-heartbeat.C:
//						if err := stream.Ping(); err != nil {
//							return
//						}
//					}
//				}
//			})
//	})
//
// Authentication and rate limiting apply like on any other route; the access
// log entry is written when the stream ends.
func RegisterSSE(h *server.Hertz, path string, metrics *observability.Metrics, handler func(ctx context.Context, stream *SSEStream)) {
	h.GET(path, func(ctx context.Context, c *app.RequestContext) {
		// The cancellable context is what Close() triggers, so broker-driven
		// disconnects end the handler exactly like a client hangup.
		streamCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		writer := hertzsse.NewWriter(c)
		defer func() { _ = writer.Close() }()
		stream := &SSEStream{
			ctx:     streamCtx,
			cancel:  cancel,
			writer:  writer,
			metrics: metrics,
			remote:  c.RemoteAddr().String(),
		}
		if err := writer.WriteComment("connected"); err != nil {
			return
		}
		handler(streamCtx, stream)
	})
}

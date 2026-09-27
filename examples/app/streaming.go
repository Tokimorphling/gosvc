package app

import (
	"context"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/Tokimorphling/gosvc/apierror"
	"github.com/Tokimorphling/gosvc/jsonrpc"
	"github.com/Tokimorphling/gosvc/observability"
	httptransport "github.com/Tokimorphling/gosvc/transport/http"
	tcptransport "github.com/Tokimorphling/gosvc/transport/tcp"
)

// Push examples: the three push shapes the runtime supports.
//
//   - TCP sessions: a "subscribe" request grabs the connection's push
//     session and keeps it; a later request broadcasts an event to every
//     subscribed client. Every session owns a bounded queue (drop policy by
//     default) and drops are counted on gosvc_notify_dropped_total.
//   - SSE: a plain GET route that streams events, authenticated and logged by
//     the ordinary middleware chain.
//   - gRPC server streaming: declared in the proto and bound in bindings.go,
//     sharing the same interceptor chain as unary RPCs.

// eventsBroadcaster fans JSON-RPC notifications out to subscribed TCP
// sessions. It is deliberately simple: the template is about showing the
// runtime API, not about building a pubsub system.
type eventsBroadcaster struct {
	mu       sync.RWMutex
	sessions map[*tcptransport.Session]struct{}
}

func newEventsBroadcaster() *eventsBroadcaster {
	return &eventsBroadcaster{sessions: make(map[*tcptransport.Session]struct{})}
}

// subscribe registers the calling TCP connection for broadcasts.
func (b *eventsBroadcaster) subscribe(ctx context.Context) (int, error) {
	session := tcptransport.SessionFromContext(ctx)
	if session == nil {
		return 0, apierror.New(apierror.KindInvalidArgument, "events.subscribe requires the TCP transport")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.sessions[session] = struct{}{}
	return len(b.sessions), nil
}

// notify broadcasts params as a JSON-RPC notification and reports how many
// sessions received it. Sessions that dropped (slow) or died are pruned.
func (b *eventsBroadcaster) notify(method string, params any) int {
	b.mu.RLock()
	sessions := make([]*tcptransport.Session, 0, len(b.sessions))
	for session := range b.sessions {
		sessions = append(sessions, session)
	}
	b.mu.RUnlock()

	delivered := 0
	var pruned []*tcptransport.Session
	for _, session := range sessions {
		if err := session.Notify(method, params); err != nil {
			// ErrNotifyDropped: the client is too slow (policy: drop);
			// ErrSessionClosed: the client is gone. Both are visible on
			// gosvc_notify_dropped_total.
			pruned = append(pruned, session)
			continue
		}
		delivered++
	}
	if len(pruned) > 0 {
		b.mu.Lock()
		for _, session := range pruned {
			delete(b.sessions, session)
		}
		b.mu.Unlock()
	}
	return delivered
}

// registerPushBindings wires the push demo endpoints:
// JSON-RPC events.subscribe/events.ping and the SSE route.
func registerPushBindings(h *server.Hertz, metrics *observability.Metrics, d *jsonrpc.Dispatcher, b *eventsBroadcaster) {
	// JSON-RPC: subscribe over TCP, then trigger from any transport.
	d.RegisterTyped("events.subscribe", func(ctx context.Context, _ struct{}) (map[string]any, error) {
		count, err := b.subscribe(ctx)
		if err != nil {
			return nil, err
		}
		return map[string]any{"subscribers": count}, nil
	})
	d.RegisterTyped("events.ping", func(_ context.Context, _ struct{}) (map[string]any, error) {
		delivered := b.notify("events.pong", map[string]any{"at": time.Now().UTC().Format(time.RFC3339Nano)})
		return map[string]any{"delivered": delivered}, nil
	})

	// SSE: the same broker with the HTTP flavour.
	httptransport.RegisterSSE(h, "/api/v1/events", metrics, func(ctx context.Context, stream *httptransport.SSEStream) {
		if err := stream.Send("greet", map[string]string{"message": "hello from the event stream"}); err != nil {
			return
		}
		heartbeat := time.NewTicker(httptransport.SSEHeartbeat)
		defer heartbeat.Stop()
		for {
			select {
			case <-stream.Done():
				return
			case <-heartbeat.C:
				if err := stream.Ping(); err != nil {
					return
				}
			}
		}
	})
}

package app

import (
	"context"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/Tokimorphling/gosvc/apierror"
	"github.com/Tokimorphling/gosvc/jsonrpc"
	"github.com/Tokimorphling/gosvc/observability"
	"github.com/Tokimorphling/gosvc/push"
	httptransport "github.com/Tokimorphling/gosvc/transport/http"
)

// Push examples: the three push shapes the runtime supports, all wired
// through the framework broker instead of hand-rolled registries.
//
//   - TCP sessions: a "subscribe" request resolves the connection's push
//     sink from the dispatch context; the broker owns delivery, backpressure
//     and pruning from there on;
//   - SSE: a plain GET route subscribes its stream as a sink and then just
//     waits — the broker pumps events into it;
//   - gRPC server streaming: declared in the proto, bound in bindings.go,
//     sharing the same interceptor chain as unary RPCs.

// Event is the payload broadcast on the "events.pong" channel.
type Event struct {
	At string `json:"at"`
}

// registerPushBindings wires the push demo endpoints: JSON-RPC
// events.subscribe/events.ping and the SSE route, all backed by one broker.
func registerPushBindings(h *server.Hertz, metrics *observability.Metrics, d *jsonrpc.Dispatcher, broker *push.Broker[Event]) {
	broker.SetMetrics(metrics)

	// JSON-RPC: subscribe over any push-capable transport, trigger from any.
	d.RegisterTyped("events.subscribe", func(ctx context.Context, _ struct{}) (map[string]any, error) {
		sink, ok := push.SinkFromContext(ctx)
		if !ok {
			return nil, apierror.New(apierror.KindInvalidArgument, "events.subscribe requires the TCP transport")
		}
		broker.Subscribe(sink)
		return map[string]any{"subscribers": broker.Len()}, nil
	})
	d.RegisterTyped("events.ping", func(_ context.Context, _ struct{}) (map[string]any, error) {
		accepted := broker.Publish("events.pong", Event{At: time.Now().UTC().Format(time.RFC3339Nano)})
		return map[string]any{"delivered": accepted}, nil
	})

	// SSE: the same broker with the HTTP flavour. The broker owns the writes
	// once the welcome event is out, so the handler only waits for the end.
	httptransport.RegisterSSE(h, "/api/v1/events", metrics, func(ctx context.Context, stream *httptransport.SSEStream) {
		if err := stream.Send("greet", map[string]string{"message": "hello from the event stream"}); err != nil {
			return
		}
		sub := broker.Subscribe(stream.AsSink())
		defer sub.Unsubscribe()
		<-stream.Done()
	})
}

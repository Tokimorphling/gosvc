package greeter

import (
	"context"
	"encoding/json"

	"example.com/gosvc/internal/jsonrpc"
)

// RegisterJSONRPC binds the service to a JSON-RPC dispatcher. It is shared by
// the HTTP and TCP transports so both expose exactly the same methods.
func RegisterJSONRPC(dispatcher *jsonrpc.Dispatcher, svc *Service) {
	dispatcher.Register("greeter.sayHello", func(ctx context.Context, params json.RawMessage) (any, error) {
		var req HelloRequest
		if err := jsonrpc.DecodeParams(params, &req); err != nil {
			return nil, err
		}
		return svc.SayHello(ctx, req, "jsonrpc")
	})

	dispatcher.Register("greeter.getGreeting", func(ctx context.Context, params json.RawMessage) (any, error) {
		var req struct {
			ID int64 `json:"id"`
		}
		if err := jsonrpc.DecodeParams(params, &req); err != nil {
			return nil, err
		}
		return svc.GetGreeting(ctx, req.ID)
	})

	dispatcher.Register("greeter.info", func(ctx context.Context, _ json.RawMessage) (any, error) {
		return svc.Info(ctx)
	})
}

package greeter

import (
	"context"

	"example.com/gosvc/jsonrpc"
)

// RegisterJSONRPC binds the service to a JSON-RPC dispatcher using typed
// handlers: request and response types are checked at compile time and the
// same methods are served by the HTTP /rpc endpoint and the TCP transport.
func RegisterJSONRPC(dispatcher *jsonrpc.Dispatcher, svc *Service) {
	dispatcher.RegisterTyped("greeter.sayHello", func(ctx context.Context, req HelloRequest) (*HelloResponse, error) {
		return svc.SayHello(ctx, req, "jsonrpc")
	})

	dispatcher.RegisterTyped("greeter.getGreeting", func(ctx context.Context, req GetGreetingRequest) (*Greeting, error) {
		return svc.GetGreeting(ctx, req.ID)
	})

	dispatcher.RegisterTyped("greeter.info", func(ctx context.Context, _ EmptyRequest) (*Info, error) {
		return svc.Info(ctx)
	})
}

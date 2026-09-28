package app

import (
	"github.com/cloudwego/hertz/pkg/app/server"
	ggrpc "google.golang.org/grpc"

	"github.com/Tokimorphling/gosvc"
	greeterv1 "github.com/Tokimorphling/gosvc/examples/app/api/greeter/v1"
	"github.com/Tokimorphling/gosvc/examples/app/greeter"
	"github.com/Tokimorphling/gosvc/jsonrpc"
	"github.com/Tokimorphling/gosvc/logging"
	"github.com/Tokimorphling/gosvc/push"
)

// Options wires the example application.
type Options struct {
	Config     *Config
	Log        *logging.Handle
	ConfigPath string
	EnvPrefix  string
	Version    string
	// PublicPaths are HTTP routes that bypass authentication, in addition to
	// /healthz and /readyz. Both literal paths and route patterns
	// ("/api/v1/greetings/:id") are accepted.
	PublicPaths []string
}

// Build assembles the runtime and registers the greeter bindings on all
// transports, including the push examples (TCP sessions, SSE, gRPC streams).
func Build(opts Options) (*gosvc.App, error) {
	cfg := opts.Config
	service := greeter.New(cfg.Service.Name, opts.Version, cfg.Greeting.Prefix, cfg.Greeting.MaxNameLen)
	events := push.NewBroker[Event]("events")

	runtimeOptions := []gosvc.Option{
		gosvc.WithLogger(opts.Log),
		gosvc.WithVersion(FullVersion()),
	}
	if len(opts.PublicPaths) > 0 {
		runtimeOptions = append(runtimeOptions, gosvc.WithPublicPaths(opts.PublicPaths...))
	}
	if opts.ConfigPath != "" {
		runtimeOptions = append(runtimeOptions, gosvc.WithHotReload(opts.ConfigPath, opts.EnvPrefix))
	}

	application, err := gosvc.New(&cfg.Config, runtimeOptions...)
	if err != nil {
		return nil, err
	}

	if err := application.RegisterHTTP(func(h *server.Hertz) {
		registerHTTP(h, service)
		registerPushBindings(h, application.Metrics(), application.JSONRPCDispatcher(), events)
	}); err != nil {
		return nil, err
	}
	if err := application.RegisterJSONRPC(func(d *jsonrpc.Dispatcher) { greeter.RegisterJSONRPC(d, service) }); err != nil {
		return nil, err
	}
	if err := application.RegisterGRPC(func(s *ggrpc.Server) { greeterv1.RegisterGreeterServer(s, newGreeterServer(service)) }); err != nil {
		return nil, err
	}

	return application, nil
}

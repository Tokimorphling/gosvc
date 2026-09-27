package app

import (
	"github.com/cloudwego/hertz/pkg/app/server"
	ggrpc "google.golang.org/grpc"

	"example.com/gosvc"
	greeterv1 "example.com/gosvc/examples/app/api/greeter/v1"
	"example.com/gosvc/examples/app/greeter"
	"example.com/gosvc/jsonrpc"
	"example.com/gosvc/logging"
)

// Options wires the example application.
type Options struct {
	Config     *Config
	Log        *logging.Handle
	ConfigPath string
	EnvPrefix  string
	Version    string
}

// Build assembles the runtime and registers the greeter bindings on all
// transports.
func Build(opts Options) (*gosvc.App, error) {
	cfg := opts.Config
	service := greeter.New(cfg.Service.Name, opts.Version, cfg.Greeting.Prefix, cfg.Greeting.MaxNameLen)

	runtimeOptions := []gosvc.Option{
		gosvc.WithLogger(opts.Log),
		gosvc.WithVersion(FullVersion()),
	}
	if opts.ConfigPath != "" {
		runtimeOptions = append(runtimeOptions, gosvc.WithHotReload(opts.ConfigPath, opts.EnvPrefix))
	}

	application, err := gosvc.New(&cfg.Config, runtimeOptions...)
	if err != nil {
		return nil, err
	}

	if err := application.RegisterHTTP(func(h *server.Hertz) { registerHTTP(h, service) }); err != nil {
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

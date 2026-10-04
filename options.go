package gosvc

import (
	"context"
	"time"

	"github.com/Tokimorphling/gosvc/config"
	"github.com/Tokimorphling/gosvc/jsonrpc"
	"github.com/Tokimorphling/gosvc/logging"
	tcptransport "github.com/Tokimorphling/gosvc/transport/tcp"
)

// Option customises the runtime.
type Option func(*options)

type options struct {
	logger      *logging.Handle
	version     string
	hotReload   bool
	source      config.Source
	publicPaths []string
	onReload    func(*config.Config) error

	tcpCodec          tcptransport.Codec
	tcpCallbacks      tcptransport.Callbacks
	tcpDispatcher     *jsonrpc.Dispatcher
	jsonrpcMiddleware []jsonrpc.Middleware

	onShutdown      []func(context.Context) error
	shutdownTimeout time.Duration
}

// WithLogger supplies the logging handle. When omitted, the runtime builds one
// from cfg.Log.
func WithLogger(handle *logging.Handle) Option {
	return func(o *options) { o.logger = handle }
}

// WithVersion overrides the build version reported by telemetry and the admin
// /version endpoint.
func WithVersion(v string) Option {
	return func(o *options) { o.version = v }
}

// WithHotReload watches path and applies reloadable settings (log, auth,
// limiter) at runtime. envPrefix selects the environment variable prefix used
// when re-reading the file; empty means "GOSVC".
func WithHotReload(path, envPrefix string) Option {
	return func(o *options) {
		o.hotReload = true
		o.source = config.Source{Path: path, EnvPrefix: envPrefix, StrictRuntime: true}
	}
}

// WithPublicPaths lets the listed HTTP paths bypass authentication, in
// addition to /healthz and /readyz.
func WithPublicPaths(paths ...string) Option {
	return func(o *options) { o.publicPaths = append(o.publicPaths, paths...) }
}

// WithOnReload registers an application hook invoked after the runtime applied
// its own reloadable sections. Use it to reload application-specific settings.
// The hook runs inside the reload transaction and must not call Reload; before
// Run, it must not call Close either, as both wait for that transaction.
func WithOnReload(fn func(*config.Config) error) Option {
	return func(o *options) { o.onReload = fn }
}

// WithTCPCodec installs a custom frame dialect on the TCP transport (see
// transport/tcp.Codec). Nil keeps the strict JSON-RPC 2.0 behaviour.
func WithTCPCodec(codec tcptransport.Codec) Option {
	return func(o *options) { o.tcpCodec = codec }
}

// WithTCPCallbacks installs connection lifecycle callbacks on the TCP
// transport: OnConnect runs when a connection is established (and eagerly
// creates its push Session), OnDisconnect exactly once when it ends. Use it
// for reliable connection-keyed registry cleanup.
func WithTCPCallbacks(cb tcptransport.Callbacks) Option {
	return func(o *options) { o.tcpCallbacks = cb }
}

// WithJSONRPCMiddleware appends middlewares to the shared JSON-RPC
// dispatcher. They apply to every method on every transport that dispatches
// through it (HTTP /rpc, TCP default path and custom codecs), which makes
// them the natural place for per-method authorisation, validation or
// feature switches. Equivalent to calling Dispatcher.Use inside
// RegisterJSONRPC.
func WithJSONRPCMiddleware(mw ...jsonrpc.Middleware) Option {
	return func(o *options) { o.jsonrpcMiddleware = append(o.jsonrpcMiddleware, mw...) }
}

// WithTCPDispatcher gives the TCP transport a dedicated method table. The
// shared dispatcher stays with the HTTP /rpc endpoint; the TCP transport
// dispatches only on the given dispatcher, which the application builds and
// populates before gosvc.New. Use it to keep a wire-specific protocol (for
// example stratum mining.*) off HTTP /rpc or to apply different middleware.
// The runtime installs its default metrics observer unless the dispatcher
// already has one, so per-method RPC metrics are recorded without wiring;
// call SetObserver first to substitute a different observer. RegisterJSONRPC
// still registers on the shared dispatcher.
func WithTCPDispatcher(d *jsonrpc.Dispatcher) Option {
	return func(o *options) { o.tcpDispatcher = d }
}

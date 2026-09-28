package jsonrpc

import "context"

// Transport identity in the dispatch context.
//
// A method dispatched through the shared Dispatcher can arrive over the
// HTTP /rpc endpoint or the TCP transport (default or custom codec), and
// middlewares frequently need to know which: TCP has no transport-level
// authentication, so authorisation middleware treats "tcp" strictly, while
// the HTTP path is already guarded by the HTTP middleware chain. Transports
// inject their identity before dispatch; middleware reads it with
// TransportFromContext.

type transportKey struct{}

// WithTransport records the transport identity ("tcp", "http") in ctx.
func WithTransport(ctx context.Context, transport string) context.Context {
	return context.WithValue(ctx, transportKey{}, transport)
}

// TransportFromContext returns the transport identity serving the call, or
// "" when the transport did not record one.
func TransportFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if name, ok := ctx.Value(transportKey{}).(string); ok {
		return name
	}
	return ""
}

// JSON-RPC dispatch middleware: the natural place to enforce authentication
// for methods reached over transports that have none of their own (TCP), and
// for per-method authorisation on transports that do.
//
// The HTTP /rpc endpoint is already guarded by the HTTP auth middleware, which
// resolves credentials from headers and stores the identity in the context; a
// middleware installed here sees that identity and passes through, so one
// middleware can wrap a method reachable from both transports.

package auth

import (
	"context"
	"encoding/json"

	"github.com/bytedance/sonic"

	"github.com/Tokimorphling/gosvc/jsonrpc"
	"github.com/Tokimorphling/gosvc/logging"
)

// JSONRPCCredentials extracts credentials from one JSON-RPC call. bearerToken
// is a JWT (the Authorization header equivalent), apiKey an API key. Empty
// strings mean "no credential of this kind".
type JSONRPCCredentials func(ctx context.Context, params json.RawMessage) (bearerToken, apiKey string)

// JSONRPCMiddleware returns a jsonrpc.Middleware that authenticates every call
// it wraps:
//
//   - when the dispatching transport already authenticated the caller (a
//     non-anonymous identity is in the context, as on HTTP /rpc), the
//     middleware is a pass-through;
//   - otherwise credentials come from extract (see CredentialsFromParams) and
//     are checked against the authenticator; a failure aborts the call with an
//     apierror.KindUnauthenticated error;
//   - when the authenticator is disabled the call passes through, mirroring
//     the explicit-enable posture of auth.enabled.
//
// The resolved identity is stored in the context (FromIdentity) and on the
// context logger (subject, auth_method), so handlers and their logs see the
// caller. Use it with Dispatcher.Use/UseFor or gosvc.WithJSONRPCMiddleware;
// the usual place is per-method (UseFor) for protocols like TCP whose
// "authorize" step is protocol-specific.
func JSONRPCMiddleware(a *Authenticator, extract JSONRPCCredentials) jsonrpc.Middleware {
	return func(next jsonrpc.HandlerFunc) jsonrpc.HandlerFunc {
		return func(ctx context.Context, params json.RawMessage) (any, error) {
			if identity := FromIdentity(ctx); identity != nil && identity.Method != MethodAnonymous {
				return next(ctx, params)
			}
			if extract == nil {
				extract = func(context.Context, json.RawMessage) (string, string) { return "", "" }
			}
			bearerToken, apiKey := extract(ctx, params)
			identity, err := a.Authenticate(bearerToken, apiKey)
			if err != nil {
				return nil, err
			}
			if identity.Method == MethodAnonymous {
				// Authentication is disabled; keep the call as it was.
				return next(ctx, params)
			}
			ctx = WithIdentity(ctx, identity)
			ctx = logging.WithLogger(ctx, logging.FromContext(ctx).With(
				"subject", identity.Subject,
				"auth_method", string(identity.Method),
			))
			return next(ctx, params)
		}
	}
}

// CredentialsFromParams extracts credentials from named params fields, for
// wire dialects that carry credentials inside the request instead of headers
// (the TCP transport has no headers). bearerField and apiKeyField name the
// JSON object keys holding the bearer token and the API key; either may be
// empty to skip that credential. Missing fields or non-string values read as
// empty strings, which the authenticator reports as a missing credential.
func CredentialsFromParams(bearerField, apiKeyField string) JSONRPCCredentials {
	return func(_ context.Context, params json.RawMessage) (string, string) {
		var bearer, apiKey string
		if len(params) == 0 {
			return bearer, apiKey
		}
		var fields map[string]json.RawMessage
		if err := sonic.Unmarshal(params, &fields); err != nil {
			// Positional params or malformed objects carry no named fields.
			return bearer, apiKey
		}
		if bearerField != "" {
			_ = sonic.Unmarshal(fields[bearerField], &bearer)
		}
		if apiKeyField != "" {
			_ = sonic.Unmarshal(fields[apiKeyField], &apiKey)
		}
		return bearer, apiKey
	}
}

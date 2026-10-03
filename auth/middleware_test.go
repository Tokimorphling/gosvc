package auth

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Tokimorphling/gosvc/apierror"
	"github.com/Tokimorphling/gosvc/config"
	"github.com/Tokimorphling/gosvc/jsonrpc"
)

// dispatchWithMiddleware runs one call through mw and reports what the wrapped
// handler saw: its result, error and the identity visible in its context.
func dispatchWithMiddleware(t *testing.T, mw jsonrpc.Middleware, ctx context.Context, params json.RawMessage) (any, *Identity, error) {
	t.Helper()
	called := false
	var seen *Identity
	var handler jsonrpc.HandlerFunc = func(ctx context.Context, _ json.RawMessage) (any, error) {
		called = true
		seen = FromIdentity(ctx)
		return "ok", nil
	}
	if mw != nil {
		handler = mw(handler)
	}
	result, err := handler(ctx, params)
	if err == nil && !called {
		t.Fatal("handler was not reached")
	}
	return result, seen, err
}

func TestJSONRPCMiddlewareRejectsMissingCredentials(t *testing.T) {
	a, err := New(config.AuthConfig{Enabled: true, APIKeys: []string{"secret"}})
	if err != nil {
		t.Fatal(err)
	}
	mw := JSONRPCMiddleware(a, CredentialsFromParams("", "apiKey"))
	_, _, err = dispatchWithMiddleware(t, mw, context.Background(), json.RawMessage(`{}`))
	if apierror.KindOf(err) != apierror.KindUnauthenticated {
		t.Fatalf("expected unauthenticated, got %v", err)
	}
}

func TestJSONRPCMiddlewareRejectsWrongCredentials(t *testing.T) {
	a, err := New(config.AuthConfig{Enabled: true, APIKeys: []string{"secret"}})
	if err != nil {
		t.Fatal(err)
	}
	mw := JSONRPCMiddleware(a, CredentialsFromParams("", "apiKey"))
	_, _, err = dispatchWithMiddleware(t, mw, context.Background(), json.RawMessage(`{"apiKey":"wrong"}`))
	if apierror.KindOf(err) != apierror.KindUnauthenticated {
		t.Fatalf("expected unauthenticated, got %v", err)
	}
}

func TestJSONRPCMiddlewareAuthenticatesFromParams(t *testing.T) {
	a, err := New(config.AuthConfig{Enabled: true, APIKeys: []string{"secret"}})
	if err != nil {
		t.Fatal(err)
	}
	mw := JSONRPCMiddleware(a, CredentialsFromParams("", "apiKey"))
	result, identity, err := dispatchWithMiddleware(t, mw, context.Background(), json.RawMessage(`{"apiKey":"secret"}`))
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if result != "ok" {
		t.Fatalf("result = %v", result)
	}
	if identity == nil || identity.Method != MethodAPIKey || identity.Subject != "api-key" {
		t.Fatalf("handler identity = %+v", identity)
	}
}

func TestJSONRPCMiddlewareDisabledAuthenticatorPassesThrough(t *testing.T) {
	a, err := New(config.AuthConfig{Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	mw := JSONRPCMiddleware(a, CredentialsFromParams("", "apiKey"))
	result, identity, err := dispatchWithMiddleware(t, mw, context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if result != "ok" {
		t.Fatalf("result = %v", result)
	}
	if identity != nil {
		t.Fatalf("disabled authenticator must not inject an identity, got %+v", identity)
	}
}

func TestJSONRPCMiddlewarePassesThroughTransportIdentity(t *testing.T) {
	// HTTP /rpc already authenticated the caller; the middleware must not
	// re-authenticate (and cannot: the credentials are not in params).
	a, err := New(config.AuthConfig{Enabled: true, APIKeys: []string{"secret"}})
	if err != nil {
		t.Fatal(err)
	}
	mw := JSONRPCMiddleware(a, CredentialsFromParams("", "apiKey"))
	ctx := WithIdentity(context.Background(), &Identity{Subject: "api-key", Method: MethodAPIKey})
	_, identity, err := dispatchWithMiddleware(t, mw, ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("dispatch with existing identity: %v", err)
	}
	if identity == nil || identity.Method != MethodAPIKey {
		t.Fatalf("transport identity lost: %+v", identity)
	}
}

func TestCredentialsFromParams(t *testing.T) {
	extract := CredentialsFromParams("token", "apiKey")
	bearer, apiKey := extract(context.Background(), json.RawMessage(`{"token":"jwt-value","apiKey":"key-value"}`))
	if bearer != "jwt-value" || apiKey != "key-value" {
		t.Fatalf("bearer=%q apiKey=%q", bearer, apiKey)
	}

	// Missing fields and non-strings read as empty.
	bearer, apiKey = extract(context.Background(), json.RawMessage(`{}`))
	if bearer != "" || apiKey != "" {
		t.Fatalf("missing fields: bearer=%q apiKey=%q", bearer, apiKey)
	}
	bearer, _ = extract(context.Background(), json.RawMessage(`{"token":42}`))
	if bearer != "" {
		t.Fatalf("non-string bearer = %q", bearer)
	}

	// Positional params carry no named fields.
	bearer, apiKey = extract(context.Background(), json.RawMessage(`["jwt-value"]`))
	if bearer != "" || apiKey != "" {
		t.Fatalf("positional params: bearer=%q apiKey=%q", bearer, apiKey)
	}

	// Absent params.
	bearer, apiKey = extract(context.Background(), nil)
	if bearer != "" || apiKey != "" {
		t.Fatalf("absent params: bearer=%q apiKey=%q", bearer, apiKey)
	}
}

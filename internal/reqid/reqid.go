// Package reqid generates and propagates request identifiers.
package reqid

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

type ctxKey struct{}

// New returns a random 128-bit request id as 32 hex characters.
func New() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand never fails on supported platforms; fall back to a
		// constant so callers still get a usable (if less unique) id.
		return "00000000000000000000000000000000"
	}
	return hex.EncodeToString(b[:])
}

// With stores the request id in the context.
func With(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// From returns the request id stored in the context, or an empty string.
func From(ctx context.Context) string {
	if ctx != nil {
		if id, ok := ctx.Value(ctxKey{}).(string); ok {
			return id
		}
	}
	return ""
}

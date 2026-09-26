// Package jsonrpc implements JSON-RPC 2.0 request handling.
//
// The package is transport independent: Serve takes the raw HTTP body and
// returns the raw response body. Bindings for specific HTTP frameworks live in
// the transport packages.
package jsonrpc

import (
	"context"
	"encoding/json"

	"github.com/bytedance/sonic"
)

// Request is a JSON-RPC 2.0 request or notification.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// IsNotification reports whether the request omits the id field (or sets it to
// null), in which case no response must be sent.
func (r *Request) IsNotification() bool {
	return len(r.ID) == 0 || string(r.ID) == "null"
}

// Response is a JSON-RPC 2.0 response.
type Response struct {
	JSONRPC string
	ID      json.RawMessage
	Result  any
	Error   *Error
}

// Error is a JSON-RPC 2.0 error object.
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// MarshalJSON emits either a result or an error, never both, and always emits
// the id field (null when unknown), as required by the specification.
func (r *Response) MarshalJSON() ([]byte, error) {
	id := normalizeID(r.ID)
	if r.Error != nil {
		return sonic.Marshal(struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.RawMessage `json:"id"`
			Error   *Error          `json:"error"`
		}{JSONRPC: r.JSONRPC, ID: id, Error: r.Error})
	}
	return sonic.Marshal(struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  any             `json:"result"`
	}{JSONRPC: r.JSONRPC, ID: id, Result: r.Result})
}

// HandlerFunc handles one JSON-RPC method. params is the raw params value
// (object, array or absent); decode it with DecodeParams.
type HandlerFunc func(ctx context.Context, params json.RawMessage) (any, error)

func normalizeID(id json.RawMessage) json.RawMessage {
	if len(id) == 0 {
		return json.RawMessage("null")
	}
	return id
}

package jsonrpc

import (
	"encoding/json"

	"github.com/bytedance/sonic"
)

// EncodeNotification builds the wire form of a JSON-RPC 2.0 notification:
// a request object without an id, which the peer must not answer. It is the
// shared encoding used by the client (Notify) and by transports that push
// server-originated notifications (for example transport/tcp sessions).
// Empty params ("{}" or nil) are omitted from the frame.
func EncodeNotification(method string, params any) ([]byte, error) {
	encodedParams, err := encodeParamsOf(params)
	if err != nil {
		return nil, err
	}
	request := struct {
		JSONRPC string          `json:"jsonrpc"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params,omitempty"`
	}{
		JSONRPC: "2.0",
		Method:  method,
		Params:  encodedParams,
	}
	body, err := sonic.Marshal(request)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// encodeParamsOf marshals params for a notification or call, dropping empty
// objects so methods without arguments stay clean on the wire.
func encodeParamsOf(params any) (json.RawMessage, error) {
	if params == nil {
		return nil, nil
	}
	raw, err := sonic.Marshal(params)
	if err != nil {
		return nil, err
	}
	switch string(raw) {
	case "null", "{}":
		return nil, nil
	}
	return raw, nil
}

package tcp

import (
	"encoding/json"

	"github.com/Tokimorphling/gosvc/apierror"
)

// Frame dialects.
//
// The TCP transport and the JSON-RPC 2.0 dialect are separate concerns:
// connection management, backpressure, push sessions and graceful shutdown
// are generic, while the bytes on the wire are a policy. A Codec owns that
// policy — protocols like stratum differ from JSON-RPC in every observable
// detail (no "jsonrpc" field, positional params, error arrays, id:null
// notifications), so a codec takes over all of them at once:
//
//   - Decode turns one inbound frame into a single Call (custom codecs are
//     single frame, single call; batch stays a capability of the default
//     JSON-RPC path);
//   - Encode renders a response; a zero Call with a non-nil callErr marks a
//     protocol-level error (busy, not ready, frame too large, decode error,
//     internal error), where callErr is an *apierror.Error describing the
//     kind and message;
//   - EncodeNotification renders server-originated pushes, so the shutdown
//     broadcast and Session.Notify speak the dialect too.
//
// Encode/Decode run on the event loop for the server-level error frames and
// on worker goroutines otherwise; implementations must be fast,
// non-blocking and safe for concurrent use. The dispatcher, middleware,
// observer and metrics are shared with the default path: only the wire
// shape is the codec's. When Options.Codec is nil the transport keeps its
// strict JSON-RPC 2.0 behaviour byte for byte.

// Call is one decoded inbound invocation. ID is echoed into Encode so the
// codec controls how it travels back; Notification requests are dispatched
// but never answered.
type Call struct {
	ID           json.RawMessage
	Method       string
	Params       json.RawMessage
	Notification bool
}

// Codec owns the frame dialect of the TCP transport.
type Codec interface {
	// Name identifies the dialect on metrics and logs.
	Name() string
	// Decode parses one inbound frame.
	Decode(body []byte) (Call, error)
	// Encode renders a response for call. A zero call with a non-nil callErr
	// marks a protocol-level error; callErr is an *apierror.Error.
	Encode(call Call, result any, callErr error) ([]byte, error)
	// EncodeNotification renders a server-originated push. The dialect
	// defines what "no id" means on the wire, for example {"id":null,...}
	// in stratum.
	EncodeNotification(method string, params any) ([]byte, error)
}

// Serial is the optional capability a codec can implement to request
// per-connection serial dispatch: frames on the same connection are executed
// strictly in arrival order. Order-dependent protocols (stratum's
// authorize-before-submit invariant) need it; the default JSON-RPC path
// stays concurrent, since JSON-RPC clients must tolerate out-of-order
// responses anyway.
type Serial interface {
	// SerialPerConn reports whether the dialect's calls are order-dependent.
	SerialPerConn() bool
}

// Protocol-level errors behind the server frames; codecs map these onto
// their own error shapes through apierror.KindOf.
func notReadyError() error { return apierror.New(apierror.KindUnavailable, "service not ready") }
func busyError() error     { return apierror.New(apierror.KindRateLimited, "server busy") }
func tooLargeError() error { return apierror.New(apierror.KindInvalidArgument, "frame too large") }
func internalError() error { return apierror.New(apierror.KindInternal, "internal error") }

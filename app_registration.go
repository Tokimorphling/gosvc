package gosvc

import (
	"net/http"

	"github.com/cloudwego/hertz/pkg/app/server"
	ggrpc "google.golang.org/grpc"

	"github.com/Tokimorphling/gosvc/jsonrpc"
)

// RegisterHTTP adds routes to the Hertz engine. It must be called before Run.
func (a *App) RegisterHTTP(fn func(*server.Hertz)) error {
	return a.register(fn, &a.httpRegs)
}

// RegisterGRPC registers gRPC services. It must be called before Run.
func (a *App) RegisterGRPC(fn func(*ggrpc.Server)) error {
	return a.register(fn, &a.grpcRegs)
}

// RegisterJSONRPC registers methods shared by HTTP /rpc and TCP. It must be
// called before Run.
func (a *App) RegisterJSONRPC(fn func(*jsonrpc.Dispatcher)) error {
	return a.register(fn, &a.rpcRegs)
}

// RegisterAdmin adds routes to the admin mux. It must be called before Run.
func (a *App) RegisterAdmin(fn func(*http.ServeMux)) error {
	return a.register(fn, &a.adminRegs)
}

// register keeps lifecycle checks identical across transports while retaining
// each transport's concrete callback type. Callbacks run later, outside mu.
func (a *App) register[T any](fn func(T), registrations *[]func(T)) error {
	if fn == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return ErrClosed
	}
	if a.started {
		return ErrStarted
	}
	*registrations = append(*registrations, fn)
	return nil
}

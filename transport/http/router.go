package http

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/Tokimorphling/gosvc/apierror"
	"github.com/Tokimorphling/gosvc/jsonrpc"
)

// registerRoutes wires the endpoints owned by the runtime. Application routes
// are added through gosvc.App.RegisterHTTP.
func (s *Server) registerRoutes(h *server.Hertz) {
	h.GET("/healthz", s.handleHealthz)
	h.GET("/readyz", s.handleReadyz)

	// JSON-RPC 2.0 endpoint, backed by the shared dispatcher. Authentication is
	// applied globally by the middleware chain.
	h.POST("/rpc", s.handleJSONRPC)

	h.NoRoute(func(ctx context.Context, c *app.RequestContext) {
		WriteError(ctx, c, apierror.Newf(apierror.KindNotFound, "route %s %s not found", string(c.Method()), string(c.Path())))
	})
}

func (s *Server) handleHealthz(_ context.Context, c *app.RequestContext) {
	writeJSON(c, 200, map[string]string{"status": "ok", "version": s.version})
}

func (s *Server) handleReadyz(ctx context.Context, c *app.RequestContext) {
	// Keep public output terse; dependency details stay on the admin port.
	if !s.ready.Healthy(ctx) {
		writeJSON(c, 503, map[string]string{"status": "not_ready"})
		return
	}
	writeJSON(c, 200, map[string]string{"status": "ready"})
}

func (s *Server) handleJSONRPC(ctx context.Context, c *app.RequestContext) {
	ctx = jsonrpc.WithTransport(ctx, "http")
	resp, ok := s.dispatcher.Serve(ctx, c.Request.Body())
	if !ok {
		c.SetStatusCode(204)
		return
	}
	writeRawJSON(c, 200, resp)
}

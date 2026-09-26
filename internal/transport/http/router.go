package http

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/bytedance/sonic"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"example.com/gosvc/internal/apierror"
	"example.com/gosvc/internal/service/greeter"
	"example.com/gosvc/internal/version"
)

func (s *Server) registerRoutes(h *server.Hertz) {
	h.GET("/healthz", s.handleHealthz)
	h.GET("/readyz", s.handleReadyz)

	// RESTful API, protected by the shared authenticator.
	api := h.Group("/api/v1", Auth(s.authenticator))
	api.GET("/hello", s.handleHelloGET)
	api.POST("/hello", s.handleHelloPOST)
	api.GET("/greetings/:id", s.handleGetGreeting)
	api.GET("/info", s.handleInfo)

	// JSON-RPC 2.0 endpoint.
	h.POST("/rpc", Auth(s.authenticator), s.handleJSONRPC)

	h.NoRoute(func(ctx context.Context, c *app.RequestContext) {
		WriteError(ctx, c, apierror.Newf(apierror.KindNotFound, "route %s %s not found", string(c.Method()), string(c.Path())))
	})
}

func (s *Server) registerJSONRPCMethods() {
	greeter.RegisterJSONRPC(s.dispatcher, s.service)
	s.dispatcher.Register("system.methods", func(_ context.Context, _ json.RawMessage) (any, error) {
		return s.dispatcher.Methods(), nil
	})
}

func (s *Server) handleHealthz(_ context.Context, c *app.RequestContext) {
	writeJSON(c, 200, map[string]string{"status": "ok", "version": version.Full()})
}

func (s *Server) handleReadyz(_ context.Context, c *app.RequestContext) {
	if !s.ready.IsReady() {
		writeJSON(c, 503, map[string]string{"status": "not_ready"})
		return
	}
	writeJSON(c, 200, map[string]string{"status": "ready"})
}

func (s *Server) handleHelloGET(ctx context.Context, c *app.RequestContext) {
	resp, err := s.service.SayHello(ctx, greeter.HelloRequest{Name: c.Query("name")}, "rest")
	if err != nil {
		WriteError(ctx, c, err)
		return
	}
	writeJSON(c, 200, resp)
}

func (s *Server) handleHelloPOST(ctx context.Context, c *app.RequestContext) {
	var req greeter.HelloRequest
	if err := sonic.Unmarshal(c.Request.Body(), &req); err != nil {
		WriteError(ctx, c, apierror.Wrap(err, apierror.KindInvalidArgument, "invalid JSON body"))
		return
	}
	resp, err := s.service.SayHello(ctx, req, "rest")
	if err != nil {
		WriteError(ctx, c, err)
		return
	}
	writeJSON(c, 200, resp)
}

func (s *Server) handleGetGreeting(ctx context.Context, c *app.RequestContext) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		WriteError(ctx, c, apierror.New(apierror.KindInvalidArgument, "id must be an integer"))
		return
	}
	resp, err := s.service.GetGreeting(ctx, id)
	if err != nil {
		WriteError(ctx, c, err)
		return
	}
	writeJSON(c, 200, resp)
}

func (s *Server) handleInfo(ctx context.Context, c *app.RequestContext) {
	resp, err := s.service.Info(ctx)
	if err != nil {
		WriteError(ctx, c, err)
		return
	}
	writeJSON(c, 200, resp)
}

func (s *Server) handleJSONRPC(ctx context.Context, c *app.RequestContext) {
	resp, ok := s.dispatcher.Serve(ctx, c.Request.Body())
	if !ok {
		c.SetStatusCode(204)
		return
	}
	writeRawJSON(c, 200, resp)
}

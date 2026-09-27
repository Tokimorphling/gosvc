package app

import (
	"context"
	"strconv"

	"github.com/bytedance/sonic"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/Tokimorphling/gosvc/apierror"
	greeterv1 "github.com/Tokimorphling/gosvc/examples/app/api/greeter/v1"
	"github.com/Tokimorphling/gosvc/examples/app/greeter"
	grpctransport "github.com/Tokimorphling/gosvc/transport/grpc"
	httptransport "github.com/Tokimorphling/gosvc/transport/http"
)

// registerHTTP adds the REST routes. Errors flow through the runtime's error
// mapping so every transport answers consistently.
func registerHTTP(h *server.Hertz, svc *greeter.Service) {
	api := h.Group("/api/v1")
	api.GET("/hello", handleHelloGET(svc))
	api.POST("/hello", handleHelloPOST(svc))
	api.GET("/greetings/:id", handleGetGreeting(svc))
	api.GET("/info", handleInfo(svc))
}

func handleHelloGET(svc *greeter.Service) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		resp, err := svc.SayHello(ctx, greeter.HelloRequest{Name: c.Query("name")}, "rest")
		if err != nil {
			httptransport.WriteError(ctx, c, err)
			return
		}
		writeJSON(c, 200, resp)
	}
}

func handleHelloPOST(svc *greeter.Service) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		var req greeter.HelloRequest
		if err := sonic.Unmarshal(c.Request.Body(), &req); err != nil {
			httptransport.WriteError(ctx, c, apierror.Wrap(err, apierror.KindInvalidArgument, "invalid JSON body"))
			return
		}
		resp, err := svc.SayHello(ctx, req, "rest")
		if err != nil {
			httptransport.WriteError(ctx, c, err)
			return
		}
		writeJSON(c, 200, resp)
	}
}

func handleGetGreeting(svc *greeter.Service) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			httptransport.WriteError(ctx, c, apierror.New(apierror.KindInvalidArgument, "id must be an integer"))
			return
		}
		resp, err := svc.GetGreeting(ctx, id)
		if err != nil {
			httptransport.WriteError(ctx, c, err)
			return
		}
		writeJSON(c, 200, resp)
	}
}

func handleInfo(svc *greeter.Service) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		resp, err := svc.Info(ctx)
		if err != nil {
			httptransport.WriteError(ctx, c, err)
			return
		}
		writeJSON(c, 200, resp)
	}
}

func writeJSON(c *app.RequestContext, status int, v any) {
	raw, err := sonic.Marshal(v)
	if err != nil {
		c.SetStatusCode(500)
		return
	}
	c.SetStatusCode(status)
	c.SetContentType("application/json; charset=utf-8")
	_, _ = c.Write(raw)
}

// greeterServer adapts the domain service to the generated gRPC interface.
type greeterServer struct {
	greeterv1.UnimplementedGreeterServer
	service *greeter.Service
}

func newGreeterServer(service *greeter.Service) *greeterServer {
	return &greeterServer{service: service}
}

func (g *greeterServer) SayHello(ctx context.Context, req *greeterv1.SayHelloRequest) (*greeterv1.SayHelloResponse, error) {
	resp, err := g.service.SayHello(ctx, greeter.HelloRequest{Name: req.GetName()}, "grpc")
	if err != nil {
		return nil, grpctransport.ToStatus(err)
	}
	return &greeterv1.SayHelloResponse{
		Message:  resp.Message,
		Server:   resp.Server,
		Protocol: resp.Protocol,
		ServedAt: resp.ServedAt,
	}, nil
}

func (g *greeterServer) GetGreeting(ctx context.Context, req *greeterv1.GetGreetingRequest) (*greeterv1.GetGreetingResponse, error) {
	resp, err := g.service.GetGreeting(ctx, req.GetId())
	if err != nil {
		return nil, grpctransport.ToStatus(err)
	}
	return &greeterv1.GetGreetingResponse{Id: resp.ID, Text: resp.Text}, nil
}

func (g *greeterServer) Info(ctx context.Context, _ *greeterv1.InfoRequest) (*greeterv1.InfoResponse, error) {
	resp, err := g.service.Info(ctx)
	if err != nil {
		return nil, grpctransport.ToStatus(err)
	}
	return &greeterv1.InfoResponse{
		Name:          resp.Name,
		Version:       resp.Version,
		StartedAt:     resp.StartedAt,
		UptimeSeconds: resp.UptimeSeconds,
		Requests:      resp.Requests,
	}, nil
}

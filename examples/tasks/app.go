package tasks

import (
	"context"
	"errors"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/Tokimorphling/gosvc"
	"github.com/Tokimorphling/gosvc/apierror"
	"github.com/Tokimorphling/gosvc/jsonrpc"
	httptransport "github.com/Tokimorphling/gosvc/transport/http"
)

// Build wires the domain service to the runtime. Runtime options stay available
// to the executable and tests; the business service has no transport dependency.
func Build(cfg *Config, opts ...gosvc.Option) (*gosvc.App, error) {
	if cfg == nil {
		return nil, errors.New("tasks: nil config")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	svc := NewService(cfg.Tasks)
	application, err := gosvc.New(&cfg.Config, opts...)
	if err != nil {
		return nil, err
	}
	if err := application.RegisterHTTP(func(h *server.Hertz) { registerHTTP(h, svc) }); err != nil {
		_ = application.Close()
		return nil, err
	}
	if err := application.RegisterJSONRPC(func(d *jsonrpc.Dispatcher) {
		d.RegisterTyped("tasks.create", svc.Create)
		d.RegisterTyped("tasks.get", svc.Get)
		d.RegisterTyped("tasks.list", svc.List)
		d.RegisterTyped("tasks.complete", svc.Complete)
		d.RegisterTyped("tasks.delete", svc.Delete)
	}); err != nil {
		_ = application.Close()
		return nil, err
	}
	return application, nil
}

func registerHTTP(h *server.Hertz, svc *Service) {
	h.POST("/api/v1/tasks", func(ctx context.Context, c *app.RequestContext) {
		var req CreateRequest
		if err := c.BindJSON(&req); err != nil {
			httptransport.WriteError(ctx, c, apierror.New(apierror.KindInvalidArgument, "invalid JSON body"))
			return
		}
		result, err := svc.Create(ctx, req)
		respond(ctx, c, 201, result, err)
	})
	h.GET("/api/v1/tasks", func(ctx context.Context, c *app.RequestContext) {
		result, err := svc.List(ctx, struct{}{})
		respond(ctx, c, 200, result, err)
	})
	h.GET("/api/v1/tasks/:id", func(ctx context.Context, c *app.RequestContext) {
		req, err := parseID(c)
		if err != nil {
			httptransport.WriteError(ctx, c, err)
			return
		}
		result, err := svc.Get(ctx, req)
		respond(ctx, c, 200, result, err)
	})
	h.POST("/api/v1/tasks/:id/complete", func(ctx context.Context, c *app.RequestContext) {
		req, err := parseID(c)
		if err != nil {
			httptransport.WriteError(ctx, c, err)
			return
		}
		result, err := svc.Complete(ctx, req)
		respond(ctx, c, 200, result, err)
	})
	h.DELETE("/api/v1/tasks/:id", func(ctx context.Context, c *app.RequestContext) {
		req, err := parseID(c)
		if err != nil {
			httptransport.WriteError(ctx, c, err)
			return
		}
		result, err := svc.Delete(ctx, req)
		respond(ctx, c, 200, result, err)
	})
}

func parseID(c *app.RequestContext) (IDRequest, error) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		return IDRequest{}, apierror.New(apierror.KindInvalidArgument, "id must be an integer")
	}
	return IDRequest{ID: id}, nil
}

func respond(ctx context.Context, c *app.RequestContext, status int, result any, err error) {
	if err != nil {
		httptransport.WriteError(ctx, c, err)
		return
	}
	c.JSON(status, result)
}

package deploydemo

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/Tokimorphling/gosvc"
	"github.com/Tokimorphling/gosvc/apierror"
	"github.com/Tokimorphling/gosvc/jsonrpc"
	httptransport "github.com/Tokimorphling/gosvc/transport/http"
)

//go:embed web/index.html web/app.js web/style.css
var assets embed.FS

type Status struct {
	Service       string `json:"service"`
	Version       string `json:"version"`
	UptimeSeconds int64  `json:"uptimeSeconds"`
	Storage       string `json:"storage"`
}

// Build registers a single domain service with REST and JSON-RPC. The runtime
// owns authentication, request budgets, metrics, health and graceful shutdown.
func Build(cfg *Config, version string, opts ...gosvc.Option) (*gosvc.App, error) {
	if cfg == nil {
		return nil, errors.New("deploydemo: nil config")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	svc, err := NewService(cfg.Tasks)
	if err != nil {
		return nil, err
	}
	static, err := loadAssets()
	if err != nil {
		return nil, err
	}
	opts = append(opts, gosvc.WithVersion(version), gosvc.WithPublicPaths("/", "/app.js", "/style.css"))
	application, err := gosvc.New(&cfg.Config, opts...)
	if err != nil {
		return nil, err
	}
	started := time.Now()
	if err := application.RegisterHTTP(func(h *server.Hertz) {
		registerAssets(h, static)
		registerHTTP(h, svc)
		h.GET("/api/v1/status", func(_ context.Context, c *app.RequestContext) {
			c.Header("Cache-Control", "no-store")
			c.JSON(200, Status{Service: cfg.Service.Name, Version: version, UptimeSeconds: int64(time.Since(started).Seconds()), Storage: "JSON file"})
		})
	}); err != nil {
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

type asset struct {
	path        string
	contentType string
	body        []byte
}

func loadAssets() ([]asset, error) {
	files := []asset{
		{path: "/", contentType: "text/html; charset=utf-8"},
		{path: "/app.js", contentType: "application/javascript; charset=utf-8"},
		{path: "/style.css", contentType: "text/css; charset=utf-8"},
	}
	for i := range files {
		name := "web" + files[i].path
		if files[i].path == "/" {
			name = "web/index.html"
		}
		raw, err := assets.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("load embedded asset: %w", err)
		}
		files[i].body = raw
	}
	return files, nil
}

func registerAssets(h *server.Hertz, files []asset) {
	for _, file := range files {
		h.GET(file.path, func(_ context.Context, c *app.RequestContext) {
			c.Header("Cache-Control", "no-cache")
			c.Header("X-Content-Type-Options", "nosniff")
			c.Header("Referrer-Policy", "no-referrer")
			c.Header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'self'; frame-ancestors 'none'; form-action 'self'")
			c.Data(200, file.contentType, file.body)
		})
	}
}

func registerHTTP(h *server.Hertz, svc *Service) {
	h.POST("/api/v1/tasks", func(ctx context.Context, c *app.RequestContext) {
		var req CreateRequest
		decoder := json.NewDecoder(bytes.NewReader(c.Request.Body()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil {
			httptransport.WriteError(ctx, c, apierror.New(apierror.KindInvalidArgument, "invalid JSON body"))
			return
		}
		var extra any
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			httptransport.WriteError(ctx, c, apierror.New(apierror.KindInvalidArgument, "expected one JSON object"))
			return
		}
		result, err := svc.Create(ctx, req)
		respond(ctx, c, 201, result, err)
	})
	h.GET("/api/v1/tasks", func(ctx context.Context, c *app.RequestContext) {
		result, err := svc.List(ctx, struct{}{})
		respond(ctx, c, 200, result, err)
	})
	h.GET("/api/v1/tasks/:id", withID(svc.Get))
	h.POST("/api/v1/tasks/:id/complete", withID(svc.Complete))
	h.DELETE("/api/v1/tasks/:id", withID(svc.Delete))
}

func withID[Response any](handle func(context.Context, IDRequest) (Response, error)) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			httptransport.WriteError(ctx, c, apierror.New(apierror.KindInvalidArgument, "id must be an integer"))
			return
		}
		result, err := handle(ctx, IDRequest{ID: id})
		respond(ctx, c, 200, result, err)
	}
}

func respond(ctx context.Context, c *app.RequestContext, status int, result any, err error) {
	c.Header("Cache-Control", "no-store")
	if err != nil {
		httptransport.WriteError(ctx, c, err)
		return
	}
	c.JSON(status, result)
}

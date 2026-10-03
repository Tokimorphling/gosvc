package http

import (
	"context"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	hconfig "github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/route"

	"github.com/Tokimorphling/gosvc/config"
)

func corsEngine(cfg config.CORSConfig) *route.Engine {
	engine := route.NewEngine(&hconfig.Options{})
	engine.Use(CORS(cfg))
	engine.GET("/ok", func(ctx context.Context, c *app.RequestContext) {
		c.String(200, "ok")
	})
	return engine
}

func TestCORSPreflightEmitsHeaders(t *testing.T) {
	cfg := config.CORSConfig{
		Enabled:      true,
		AllowOrigins: []string{"*"},
		MaxAge:       config.Duration(10 * time.Minute),
	}
	resp := ut.PerformRequest(corsEngine(cfg), "OPTIONS", "/ok", nil, ut.Header{Key: "Origin", Value: "https://app.example.com"})
	if resp.Code != 204 {
		t.Fatalf("preflight status = %d, want 204", resp.Code)
	}
	if got := resp.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want *", got)
	}
	if got := resp.Header().Get("Access-Control-Max-Age"); got != "600" {
		t.Fatalf("Access-Control-Max-Age = %q, want 600", got)
	}
}

func TestCORSPreflightOmitsMaxAgeWhenZero(t *testing.T) {
	cfg := config.CORSConfig{Enabled: true, AllowOrigins: []string{"*"}}
	resp := ut.PerformRequest(corsEngine(cfg), "OPTIONS", "/ok", nil, ut.Header{Key: "Origin", Value: "https://app.example.com"})
	if got := resp.Header().Get("Access-Control-Max-Age"); got != "" {
		t.Fatalf("Access-Control-Max-Age = %q, want omitted", got)
	}
}

func TestCORSCredentialsReflectOriginInsteadOfWildcard(t *testing.T) {
	cfg := config.CORSConfig{
		Enabled:          true,
		AllowOrigins:     []string{"*"},
		AllowCredentials: true,
	}
	resp := ut.PerformRequest(corsEngine(cfg), "GET", "/ok", nil, ut.Header{Key: "Origin", Value: "https://app.example.com"})
	if got := resp.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want the reflected origin", got)
	}
	if got := resp.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Fatalf("Access-Control-Allow-Credentials = %q, want true", got)
	}
}

func TestCORSUnmatchedOriginGetsNoHeaders(t *testing.T) {
	cfg := config.CORSConfig{Enabled: true, AllowOrigins: []string{"https://allowed.example.com"}}
	resp := ut.PerformRequest(corsEngine(cfg), "GET", "/ok", nil, ut.Header{Key: "Origin", Value: "https://denied.example.com"})
	if got := resp.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want none", got)
	}
}

func TestCORSDisabledIsTransparent(t *testing.T) {
	engine := corsEngine(config.CORSConfig{Enabled: false})
	resp := ut.PerformRequest(engine, "GET", "/ok", nil, ut.Header{Key: "Origin", Value: "https://app.example.com"})
	if resp.Code != 200 {
		t.Fatalf("status = %d, want 200 (middleware disabled)", resp.Code)
	}
	if got := resp.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want none", got)
	}
}

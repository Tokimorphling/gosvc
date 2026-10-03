package http

import (
	"context"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	hconfig "github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/route"
)

func TestRequestTimeoutDerivesDeadline(t *testing.T) {
	engine := route.NewEngine(&hconfig.Options{})
	engine.Use(RequestTimeout(50 * time.Millisecond))
	engine.GET("/slow", func(ctx context.Context, c *app.RequestContext) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Error("handler context carries no deadline")
			c.String(200, "no-deadline")
			return
		}
		if remaining := time.Until(deadline); remaining <= 0 || remaining > time.Second {
			t.Errorf("deadline remaining = %v, want within (0, 1s]", remaining)
		}
		// Respect the budget like a well-behaved handler and observe the
		// cancellation instead of blocking the test.
		<-ctx.Done()
		c.String(200, "cancelled")
	})

	start := time.Now()
	resp := ut.PerformRequest(engine, "GET", "/slow", nil)
	elapsed := time.Since(start)
	if resp.Code != 200 {
		t.Fatalf("status = %d, want 200", resp.Code)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("handler returned in %v, deadline did not apply", elapsed)
	}
}

func TestRequestTimeoutDisabledPassesPlainContext(t *testing.T) {
	engine := route.NewEngine(&hconfig.Options{})
	engine.Use(RequestTimeout(0))
	engine.GET("/plain", func(ctx context.Context, c *app.RequestContext) {
		if _, hasDeadline := ctx.Deadline(); hasDeadline {
			t.Error("zero timeout must not derive a deadline")
		}
		c.String(200, "ok")
	})

	resp := ut.PerformRequest(engine, "GET", "/plain", nil)
	if resp.Code != 200 {
		t.Fatalf("status = %d, want 200", resp.Code)
	}
}

package grpc

import (
	"context"
	"sync"
	"time"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	apphealth "github.com/Tokimorphling/gosvc/health"
)

// readinessHealth uses the same aggregate as HTTP/admin/JSON-RPC. Check and
// List probe on demand; one shared monitor publishes changes to Watch clients.
type readinessHealth struct {
	*health.Server
	ready        *apphealth.Ready
	serviceNames func() map[string]struct{}
}

func newReadinessHealth(ready *apphealth.Ready, services func() map[string]ggrpc.ServiceInfo) *readinessHealth {
	h := &readinessHealth{Server: health.NewServer(), ready: ready}
	// Registration completes before serving. GetServiceInfo constructs a deep
	// map of descriptors; snapshot names once instead of rebuilding it per probe.
	h.serviceNames = sync.OnceValue(func() map[string]struct{} {
		info := services()
		names := make(map[string]struct{}, len(info))
		for name := range info {
			names[name] = struct{}{}
		}
		return names
	})
	h.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	return h
}

func (h *readinessHealth) refresh(ctx context.Context) {
	ready := h.ready.Healthy(ctx)
	state := healthpb.HealthCheckResponse_NOT_SERVING
	if ready {
		state = healthpb.HealthCheckResponse_SERVING
	}
	h.SetServingStatus("", state)
	for service := range h.serviceNames() {
		h.SetServingStatus(service, state)
	}
}

func (h *readinessHealth) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if req.Service != "" {
		if _, ok := h.serviceNames()[req.Service]; !ok {
			return nil, status.Error(codes.NotFound, "unknown service")
		}
	}
	h.refresh(ctx)
	return h.Server.Check(ctx, req)
}

func (h *readinessHealth) List(ctx context.Context, req *healthpb.HealthListRequest) (*healthpb.HealthListResponse, error) {
	h.refresh(ctx)
	return h.Server.List(ctx, req)
}

func (h *readinessHealth) Watch(req *healthpb.HealthCheckRequest, stream healthpb.Health_WatchServer) error {
	h.refresh(stream.Context())
	return h.Server.Watch(req, stream)
}

func (h *readinessHealth) monitor(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.refresh(ctx)
		}
	}
}

// BeginShutdown publishes NOT_SERVING before application hooks run, while
// existing RPC connections remain available for draining.
func (s *Server) BeginShutdown() { s.health.Shutdown() }

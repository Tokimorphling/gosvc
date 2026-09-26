package grpc

import (
	"context"
	"log/slog"

	greeterv1 "example.com/gosvc/api/greeter/v1"
	"example.com/gosvc/internal/service/greeter"
)

// greeterServer adapts the domain service to the generated gRPC interface.
type greeterServer struct {
	greeterv1.UnimplementedGreeterServer
	service *greeter.Service
}

func newGreeterServer(service *greeter.Service, _ *slog.Logger) *greeterServer {
	return &greeterServer{service: service}
}

func (g *greeterServer) SayHello(ctx context.Context, req *greeterv1.SayHelloRequest) (*greeterv1.SayHelloResponse, error) {
	resp, err := g.service.SayHello(ctx, greeter.HelloRequest{Name: req.GetName()}, "grpc")
	if err != nil {
		return nil, toStatus(err)
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
		return nil, toStatus(err)
	}
	return &greeterv1.GetGreetingResponse{Id: resp.ID, Text: resp.Text}, nil
}

func (g *greeterServer) Info(ctx context.Context, _ *greeterv1.InfoRequest) (*greeterv1.InfoResponse, error) {
	resp, err := g.service.Info(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	return &greeterv1.InfoResponse{
		Name:          resp.Name,
		Version:       resp.Version,
		StartedAt:     resp.StartedAt,
		UptimeSeconds: resp.UptimeSeconds,
		Requests:      resp.Requests,
	}, nil
}

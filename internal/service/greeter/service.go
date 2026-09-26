// Package greeter is the example domain service.
//
// It is deliberately transport-agnostic: REST, JSON-RPC and gRPC handlers all
// call the same methods and return the same errors.
package greeter

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"example.com/gosvc/internal/apierror"
)

// Service implements the greeting domain logic.
type Service struct {
	name      string
	version   string
	startedAt time.Time
	requests  atomic.Int64
	greetings map[int64]string
}

// New creates the service.
func New(name, version string) *Service {
	return &Service{
		name:      name,
		version:   version,
		startedAt: time.Now(),
		greetings: map[int64]string{
			1: "hello",
			2: "hi",
			3: "你好",
			4: "こんにちは",
		},
	}
}

// HelloRequest is the input of SayHello.
type HelloRequest struct {
	Name string `json:"name"`
}

// HelloResponse is the output of SayHello.
type HelloResponse struct {
	Message  string `json:"message"`
	Server   string `json:"server"`
	Protocol string `json:"protocol"`
	ServedAt string `json:"servedAt"`
}

// Greeting is a stored greeting.
type Greeting struct {
	ID   int64  `json:"id"`
	Text string `json:"text"`
}

// Info describes the running service.
type Info struct {
	Name          string `json:"name"`
	Version       string `json:"version"`
	StartedAt     string `json:"startedAt"`
	UptimeSeconds int64  `json:"uptimeSeconds"`
	Requests      int64  `json:"requests"`
}

// SayHello validates the request and greets the caller.
func (s *Service) SayHello(_ context.Context, req HelloRequest, protocol string) (*HelloResponse, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, apierror.New(apierror.KindInvalidArgument, "name must not be empty")
	}
	if len(name) > 64 {
		return nil, apierror.New(apierror.KindInvalidArgument, "name must be at most 64 bytes")
	}

	s.requests.Add(1)
	return &HelloResponse{
		Message:  "hello, " + name,
		Server:   s.name,
		Protocol: protocol,
		ServedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}, nil
}

// GetGreeting returns a stored greeting or a not-found error.
func (s *Service) GetGreeting(_ context.Context, id int64) (*Greeting, error) {
	if id <= 0 {
		return nil, apierror.New(apierror.KindInvalidArgument, "id must be a positive integer")
	}
	text, ok := s.greetings[id]
	if !ok {
		return nil, apierror.Newf(apierror.KindNotFound, "greeting %d not found", id)
	}

	s.requests.Add(1)
	return &Greeting{ID: id, Text: text}, nil
}

// Info returns runtime information about the service.
func (s *Service) Info(_ context.Context) (*Info, error) {
	return &Info{
		Name:          s.name,
		Version:       s.version,
		StartedAt:     s.startedAt.UTC().Format(time.RFC3339),
		UptimeSeconds: int64(time.Since(s.startedAt).Seconds()),
		Requests:      s.requests.Load(),
	}, nil
}

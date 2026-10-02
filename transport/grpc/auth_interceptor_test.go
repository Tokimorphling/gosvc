package grpc

import (
	"context"
	"testing"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Tokimorphling/gosvc/auth"
	"github.com/Tokimorphling/gosvc/config"
)

type authTestStream struct{ ctx context.Context }

func (s authTestStream) SetHeader(metadata.MD) error  { return nil }
func (s authTestStream) SendHeader(metadata.MD) error { return nil }
func (s authTestStream) SetTrailer(metadata.MD)       {}
func (s authTestStream) Context() context.Context     { return s.ctx }
func (s authTestStream) SendMsg(any) error            { return nil }
func (s authTestStream) RecvMsg(any) error            { return nil }

func TestAuthInterceptorsObserveEnableTransition(t *testing.T) {
	a, err := auth.New(config.AuthConfig{Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	unary := authInterceptor(a)
	stream := authStreamInterceptor(a)
	unaryInfo := &ggrpc.UnaryServerInfo{FullMethod: "/example.Service/Call"}
	streamInfo := &ggrpc.StreamServerInfo{FullMethod: "/example.Service/Stream"}
	unaryHandler := func(ctx context.Context, _ any) (any, error) {
		return auth.FromIdentity(ctx), nil
	}
	streamHandler := func(_ any, ss ggrpc.ServerStream) error {
		if auth.FromIdentity(ss.Context()) != nil {
			t.Error("disabled stream auth injected an identity")
		}
		return nil
	}
	if identity, err := unary(context.Background(), nil, unaryInfo, unaryHandler); err != nil {
		t.Fatalf("disabled unary auth: identity=%v err=%v", identity, err)
	} else if got, ok := identity.(*auth.Identity); !ok || got != nil {
		t.Fatalf("disabled unary identity should remain absent: %v", identity)
	}
	if err := stream(nil, authTestStream{context.Background()}, streamInfo, streamHandler); err != nil {
		t.Fatalf("disabled stream auth: %v", err)
	}
	if err := a.Reload(config.AuthConfig{Enabled: true, APIKeys: []string{"key"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := unary(context.Background(), nil, unaryInfo, unaryHandler); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("enabled unary without key: %v", err)
	}
	if err := stream(nil, authTestStream{context.Background()}, streamInfo, streamHandler); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("enabled stream without key: %v", err)
	}
	keyCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", "key"))
	identity, err := unary(keyCtx, nil, unaryInfo, unaryHandler)
	got, ok := identity.(*auth.Identity)
	if err != nil || !ok || got.Method != auth.MethodAPIKey {
		t.Fatalf("enabled unary with key: identity=%v err=%v", identity, err)
	}
	if err := stream(nil, authTestStream{keyCtx}, streamInfo, func(_ any, ss ggrpc.ServerStream) error {
		identity := auth.FromIdentity(ss.Context())
		if identity == nil || identity.Method != auth.MethodAPIKey {
			t.Errorf("enabled stream identity = %+v", identity)
		}
		return nil
	}); err != nil {
		t.Fatalf("enabled stream with key: %v", err)
	}
}

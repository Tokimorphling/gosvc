package greeter

import (
	"context"
	"strings"
	"testing"

	"github.com/Tokimorphling/gosvc/apierror"
)

func newTestService() *Service {
	return New("test-svc", "v1", "hello", 8)
}

func TestSayHello(t *testing.T) {
	svc := newTestService()

	resp, err := svc.SayHello(context.Background(), HelloRequest{Name: " world "}, "unit")
	if err != nil {
		t.Fatalf("SayHello: %v", err)
	}
	if resp.Message != "hello, world" {
		t.Fatalf("message = %q", resp.Message)
	}
	if resp.Protocol != "unit" || resp.Server != "test-svc" {
		t.Fatalf("resp = %+v", resp)
	}
	if resp.ServedAt == "" {
		t.Fatal("servedAt must be set")
	}
}

func TestSayHelloValidation(t *testing.T) {
	svc := newTestService()

	if _, err := svc.SayHello(context.Background(), HelloRequest{}, "unit"); apierror.KindOf(err) != apierror.KindInvalidArgument {
		t.Fatalf("empty name: kind = %v", apierror.KindOf(err))
	}
	// maxNameLen is 8 in the test service.
	if _, err := svc.SayHello(context.Background(), HelloRequest{Name: strings.Repeat("a", 9)}, "unit"); apierror.KindOf(err) != apierror.KindInvalidArgument {
		t.Fatalf("long name: kind = %v", apierror.KindOf(err))
	}
}

func TestGetGreeting(t *testing.T) {
	svc := newTestService()

	greeting, err := svc.GetGreeting(context.Background(), 1)
	if err != nil {
		t.Fatalf("GetGreeting: %v", err)
	}
	if greeting.ID != 1 || greeting.Text == "" {
		t.Fatalf("greeting = %+v", greeting)
	}

	if _, err := svc.GetGreeting(context.Background(), 999); apierror.KindOf(err) != apierror.KindNotFound {
		t.Fatalf("missing id: kind = %v", apierror.KindOf(err))
	}
	if _, err := svc.GetGreeting(context.Background(), 0); apierror.KindOf(err) != apierror.KindInvalidArgument {
		t.Fatalf("zero id: kind = %v", apierror.KindOf(err))
	}
}

func TestInfoCountsRequests(t *testing.T) {
	svc := newTestService()

	if _, err := svc.SayHello(context.Background(), HelloRequest{Name: "a"}, "unit"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.GetGreeting(context.Background(), 2); err != nil {
		t.Fatal(err)
	}

	info, err := svc.Info(context.Background())
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Name != "test-svc" || info.Version != "v1" {
		t.Fatalf("info = %+v", info)
	}
	if info.Requests != 2 {
		t.Fatalf("requests = %d, want 2", info.Requests)
	}
}

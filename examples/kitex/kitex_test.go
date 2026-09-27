package kitexexample

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/server"

	"github.com/Tokimorphling/gosvc/examples/kitex/api/echo/echoservice"
)

func TestKitexEchoService(t *testing.T) {
	klog.SetOutput(io.Discard) // keep the test output clean

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()

	svr := echoservice.NewServer(&EchoServiceImpl{Prefix: "echo: "}, server.WithListener(listener))
	serverErr := make(chan error, 1)
	go func() { serverErr <- svr.Run() }()

	var stopOnce sync.Once
	stopServer := func() { stopOnce.Do(func() { _ = svr.Stop() }) }
	defer stopServer()

	t.Run("host ports", func(t *testing.T) {
		cli, err := echoservice.NewClient("echo", client.WithHostPorts(addr))
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := cli.Echo(ctx, "hello")
		if err != nil {
			t.Fatalf("Echo: %v", err)
		}
		if resp != "echo: hello" {
			t.Fatalf("resp = %q", resp)
		}

		if _, err := cli.Echo(ctx, ""); err == nil {
			t.Fatal("expected an error for an empty message")
		}
	})

	t.Run("static resolver", func(t *testing.T) {
		instance := discovery.NewInstance("tcp", addr, 1, nil)
		cli, err := echoservice.NewClient("echo", client.WithResolver(NewStaticResolver(instance)))
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := cli.Echo(ctx, "resolver")
		if err != nil {
			t.Fatalf("Echo: %v", err)
		}
		if resp != "echo: resolver" {
			t.Fatalf("resp = %q", resp)
		}
	})

	stopServer()
	select {
	case err := <-serverErr:
		if err != nil {
			t.Fatalf("server exited with error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop within 5s")
	}
}

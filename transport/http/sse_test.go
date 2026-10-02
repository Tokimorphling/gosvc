package http

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	nethttp "net/http"
	"strings"
	"testing"
	"time"

	"github.com/Tokimorphling/gosvc/config"
	"github.com/Tokimorphling/gosvc/observability"
)

func TestRegisterSSEFlushesIdleStreamAndDetectsDisconnect(t *testing.T) {
	cfg := config.Default()
	cfg.HTTP.Host = "127.0.0.1"
	cfg.HTTP.Port = 0
	cfg.HTTP.ShutdownTimeout = config.Duration(2 * time.Second)

	s, err := New(Options{
		Config:  cfg,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics: observability.New("sse-test"),
	})
	if err != nil {
		t.Fatal(err)
	}

	handlerStarted := make(chan struct{})
	handlerDone := make(chan struct{})
	RegisterSSE(s.Engine(), "/events", nil, func(_ context.Context, stream *SSEStream) {
		if id := stream.AsSink().ID(); !strings.HasPrefix(id, "sse/127.0.0.1:") {
			t.Errorf("sink ID = %q, want the client address", id)
		}
		if stream.RemoteAddr() == "" {
			t.Error("RemoteAddr is empty")
		}
		close(handlerStarted)
		<-stream.Done()
		close(handlerDone)
	})

	serveCtx, cancelServe := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- s.Serve(serveCtx) }()
	t.Cleanup(func() {
		cancelServe()
		select {
		case err := <-serveDone:
			if err != nil {
				t.Errorf("serve: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("server did not stop")
		}
	})

	requestCtx, cancelRequest := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelRequest()
	req, err := nethttp.NewRequestWithContext(requestCtx, nethttp.MethodGet, "http://"+s.Addr()+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := nethttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("idle SSE response headers were not flushed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != nethttp.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("content-type = %q", got)
	}
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil {
		t.Fatalf("read initial comment: %v", err)
	}
	if line != ":connected\n" {
		t.Fatalf("initial SSE frame = %q", line)
	}
	select {
	case <-handlerStarted:
	case <-requestCtx.Done():
		t.Fatal("SSE handler did not start")
	}

	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("SSE handler did not observe client disconnect")
	}
}

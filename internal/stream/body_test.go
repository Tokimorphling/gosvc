package stream

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type closer struct {
	io.Reader
	count atomic.Int32
}

func (c *closer) Close() error { c.count.Add(1); return nil }

func TestBodyCleanupExactlyOnce(t *testing.T) {
	source := &closer{Reader: strings.NewReader("body")}
	var finished atomic.Int32
	b := Wrap(source, func(err error) {
		if err != nil {
			t.Error(err)
		}
		finished.Add(1)
	})
	if _, err := io.ReadAll(b); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { _ = b.Close() })
	}
	wg.Wait()
	if source.count.Load() != 1 || finished.Load() != 1 {
		t.Fatal("duplicate cleanup")
	}
}

func TestAlreadyCanceledContextClosesBody(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	source := &closer{Reader: strings.NewReader("body")}
	done := make(chan struct{})
	b := WrapContext(ctx, source, func(error) { close(done) })
	<-done
	_ = b.Close()
	if source.count.Load() != 1 {
		t.Fatal("duplicate close after canceled context")
	}
}

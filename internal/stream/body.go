// Package stream attaches resource cleanup to the lifetime of a response body.
package stream

import (
	"context"
	"io"
	"sync"
)

// Wrap closes the underlying body and calls finish exactly once, on EOF, read
// error or Close. This binds cancellation, observation and leases to streaming
// consumption instead of the earlier point where response headers arrive.
func Wrap(body io.ReadCloser, finish func(error)) io.ReadCloser {
	return &managedBody{ReadCloser: body, finish: finish}
}

// WrapContext also closes abandoned streams on cancellation or deadline. The
// underlying body must support Close during Read, as HTTP response bodies do.
func WrapContext(ctx context.Context, body io.ReadCloser, finish func(error)) io.ReadCloser {
	b := &managedBody{ReadCloser: body, finish: finish}
	b.mu.Lock()
	b.stop = context.AfterFunc(ctx, func() { b.complete(ctx.Err()) })
	b.mu.Unlock()
	return b
}

type managedBody struct {
	io.ReadCloser
	once     sync.Once
	mu       sync.Mutex
	stop     func() bool
	finish   func(error)
	closeErr error
}

func (b *managedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.complete(err)
	}
	return n, err
}

func (b *managedBody) Close() error {
	b.complete(nil)
	return b.closeErr
}

func (b *managedBody) complete(err error) {
	b.once.Do(func() {
		b.mu.Lock()
		stop := b.stop
		b.mu.Unlock()
		if stop != nil {
			stop()
		}
		b.closeErr = b.ReadCloser.Close()
		if err == io.EOF {
			err = nil
		}
		if err == nil {
			err = b.closeErr
		}
		b.finish(err)
	})
}

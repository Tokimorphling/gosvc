package gosvc

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/Tokimorphling/gosvc/internal/stream"
	"github.com/Tokimorphling/gosvc/store/object"
	"github.com/Tokimorphling/gosvc/store/s3"
)

// Objects returns a stable object store that follows storage reloads. Disabled
// storage returns object.ErrDisabled on use. Get holds its generation's lease
// until EOF, Close or its request deadline; always close download bodies.
func (a *App) Objects() object.Store { return a.objects }

type objectStore struct{ app *App }

var _ object.Store = (*objectStore)(nil)

// withObjectStore keeps both type safety and the existing storage lease rules
// for operations whose lifetime ends with the method call.
func withObjectStore[T any](a *App, fn func(*s3.Store) (T, error)) (T, error) {
	var result T
	err := leaseStorage(a, func(state *storageState) *s3.Store { return state.s3 }, func(client *s3.Store) error {
		var err error
		result, err = fn(client)
		return err
	})
	if errors.Is(err, ErrStorageDisabled) {
		err = object.ErrDisabled
	}
	return result, err
}

func (s *objectStore) Put(ctx context.Context, key string, body io.Reader, opts object.PutOptions) (object.PutResult, error) {
	return withObjectStore(s.app, func(client *s3.Store) (object.PutResult, error) { return client.Put(ctx, key, body, opts) })
}

func (s *objectStore) Head(ctx context.Context, key string) (object.Info, error) {
	return withObjectStore(s.app, func(client *s3.Store) (object.Info, error) { return client.Head(ctx, key) })
}

func (s *objectStore) Delete(ctx context.Context, key string) error {
	_, err := withObjectStore(s.app, func(client *s3.Store) (struct{}, error) { return struct{}{}, client.Delete(ctx, key) })
	return err
}

func (s *objectStore) List(ctx context.Context, opts object.ListOptions) (object.Page, error) {
	return withObjectStore(s.app, func(client *s3.Store) (object.Page, error) { return client.List(ctx, opts) })
}

func (s *objectStore) PresignGet(ctx context.Context, key string, expiry time.Duration) (object.SignedRequest, error) {
	return withObjectStore(s.app, func(client *s3.Store) (object.SignedRequest, error) { return client.PresignGet(ctx, key, expiry) })
}

func (s *objectStore) PresignPut(ctx context.Context, key string, opts object.PutOptions, expiry time.Duration) (object.SignedRequest, error) {
	return withObjectStore(s.app, func(client *s3.Store) (object.SignedRequest, error) { return client.PresignPut(ctx, key, opts, expiry) })
}

func (s *objectStore) Get(ctx context.Context, key string, opts object.GetOptions) (*object.Object, error) {
	a := s.app
	a.storageMu.Lock()
	state := a.storage
	if state == nil || state.s3 == nil {
		a.storageMu.Unlock()
		return nil, object.ErrDisabled
	}
	state.leases++
	a.storageMu.Unlock()
	// The outer deadline also releases abandoned download leases. It belongs
	// to this storage generation, not a possibly newer effective config.
	ctx, cancel := context.WithTimeout(ctx, state.s3Timeout)
	result, err := state.s3.Get(ctx, key, opts)
	if err != nil {
		cancel()
		a.releaseStorage(state)
		return nil, err
	}
	result.Body = stream.WrapContext(ctx, result.Body, func(error) { cancel(); a.releaseStorage(state) })
	return result, nil
}

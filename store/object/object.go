// Package object defines streaming object storage contracts independent of any
// cloud SDK. Applications can depend on just Reader, Writer, Lister or Presigner.
package object

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/Tokimorphling/gosvc/apierror"
)

var (
	ErrDisabled = apierror.New(apierror.KindUnavailable, "object storage is disabled")
	ErrClosed   = apierror.New(apierror.KindUnavailable, "object storage is closed")
)

type Reader interface {
	Get(context.Context, string, GetOptions) (*Object, error)
	Head(context.Context, string) (Info, error)
}

type Writer interface {
	Put(context.Context, string, io.Reader, PutOptions) (PutResult, error)
	Delete(context.Context, string) error
}

type Lister interface {
	List(context.Context, ListOptions) (Page, error)
}

type Presigner interface {
	PresignGet(context.Context, string, time.Duration) (SignedRequest, error)
	PresignPut(context.Context, string, PutOptions, time.Duration) (SignedRequest, error)
}

type Store interface {
	Reader
	Writer
	Lister
	Presigner
}

// Object owns a streaming response. Always close Body, including on early exit.
// The operation's deadline and runtime storage lease cover the body's lifetime.
type Object struct {
	Info
	Body io.ReadCloser
}

type Info struct {
	Key          string            `json:"key"`
	Size         int64             `json:"size"`
	ETag         string            `json:"etag,omitempty"`
	ContentType  string            `json:"contentType,omitempty"`
	LastModified time.Time         `json:"lastModified"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	VersionID    string            `json:"versionId,omitempty"`
}

type PutOptions struct {
	ContentType  string
	CacheControl string
	Metadata     map[string]string
	// Size may be nil for an unknown-length stream; a non-nil value must be >= 0.
	Size *int64
}

type GetOptions struct {
	// Range is an HTTP byte range, for example "bytes=0-1023".
	Range     string
	VersionID string
}

type PutResult struct {
	ETag      string `json:"etag"`
	VersionID string `json:"versionId,omitempty"`
	Size      int64  `json:"size"`
}

type ListOptions struct {
	Prefix            string
	Delimiter         string
	ContinuationToken string
	// Limit is the page size, 1..1000; zero defaults to 1000.
	Limit int32
}

type Page struct {
	// List returns key, size, ETag and last-modified time. Use Head for metadata.
	Objects        []Info   `json:"objects"`
	CommonPrefixes []string `json:"commonPrefixes,omitempty"`
	NextToken      string   `json:"nextToken,omitempty"`
	Truncated      bool     `json:"truncated"`
}

type SignedRequest struct {
	URL    string `json:"url"`
	Method string `json:"method"`
	// Send these headers with the request; they are part of its signature.
	Headers http.Header `json:"headers"`
	// ExpiresAt is an upper bound: temporary credentials can expire sooner.
	ExpiresAt time.Time `json:"expiresAt"`
}

// Observer records logical operations, including complete streaming downloads.
// Implementations must be concurrency safe and non-blocking. No object keys or
// signed URLs are passed to observers, keeping metric cardinality bounded.
type Observer interface {
	ObserveObjectStorage(operation, outcome string, elapsed time.Duration)
}

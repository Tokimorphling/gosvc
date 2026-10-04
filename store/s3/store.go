// Package s3 adapts the AWS SDK v2 to the SDK-independent object storage API.
package s3

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Tokimorphling/gosvc/apierror"
	"github.com/Tokimorphling/gosvc/config"
	"github.com/Tokimorphling/gosvc/health"
	"github.com/Tokimorphling/gosvc/store/object"
)

type options struct {
	httpClient *http.Client
	observer   object.Observer
	tracer     trace.Tracer
}
type Option func(*options)

// WithHTTPClient uses a caller-owned HTTP client. Its transport is never closed
// by Store.Close. RequestTimeout still bounds operations and response bodies.
func WithHTTPClient(client *http.Client) Option    { return func(o *options) { o.httpClient = client } }
func WithObserver(observer object.Observer) Option { return func(o *options) { o.observer = observer } }
func WithTracer(tracer trace.Tracer) Option        { return func(o *options) { o.tracer = tracer } }

// Store is concurrency safe. Its bucket, clients and options are immutable;
// reload constructs another Store. Multipart buffers are bounded per upload,
// and the upload gate also bounds the number of concurrent buffered uploads.
type Store struct {
	cfg       config.S3Config
	client    *awss3.Client
	uploader  *transfermanager.Client
	presigner *awss3.PresignClient
	uploads   chan struct{}
	transport *http.Transport
	observer  object.Observer
	tracer    trace.Tracer
	closed    atomic.Bool
}

var _ object.Store = (*Store)(nil)

// New builds a client using cfg (normally config.Default().Storage.S3 with
// bucket/credentials overridden). CheckBucket validates access before returning.
// It never creates a bucket or changes bucket policies.
func New(ctx context.Context, cfg config.S3Config, opts ...Option) (*Store, error) {
	cfg.Enabled = true
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	o := options{}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	s := &Store{cfg: cfg, observer: o.observer, tracer: o.tracer, uploads: make(chan struct{}, cfg.MaxConcurrentUploads)}
	client := o.httpClient
	if client == nil {
		s.transport = &http.Transport{
			Proxy:             http.ProxyFromEnvironment,
			DialContext:       (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			ForceAttemptHTTP2: true, MaxIdleConns: 100, MaxIdleConnsPerHost: 20,
			IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 10 * time.Second,
			ExpectContinueTimeout: time.Second,
		}
		client = &http.Client{Transport: s.transport}
	}
	loadOptions := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithDefaultRegion("us-east-1"), awsconfig.WithHTTPClient(client),
		awsconfig.WithRetryMaxAttempts(cfg.MaxAttempts),
	}
	if cfg.Region != "" {
		loadOptions = append(loadOptions, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.AccessKeyID != "" {
		loadOptions = append(loadOptions, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)))
	}
	setupCtx, cancel := context.WithTimeout(ctx, cfg.HealthTimeout.D())
	defer cancel()
	awsCfg, err := awsconfig.LoadDefaultConfig(setupCtx, loadOptions...)
	if err != nil {
		_ = s.Close()
		return nil, normalize(err)
	}
	s.client = awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		o.UsePathStyle = cfg.UsePathStyle
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(strings.TrimRight(cfg.Endpoint, "/"))
		}
		// Avoid optional aws-chunked/checksum extensions on compatible endpoints.
		// Required checksums and server-supplied response validation remain enabled.
		if cfg.Endpoint != "" {
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		}
	})
	s.uploader = transfermanager.New(s.client, func(o *transfermanager.Options) {
		o.PartSizeBytes = cfg.PartSizeBytes
		o.MultipartUploadThreshold = cfg.PartSizeBytes
		o.Concurrency = cfg.UploadConcurrency
		o.FailTimeout = cfg.HealthTimeout.D()
		if cfg.Endpoint != "" {
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		}
	})
	s.presigner = awss3.NewPresignClient(s.client)
	if cfg.CheckBucket {
		if err := s.Ping(setupCtx); err != nil {
			_ = s.Close()
			return nil, err
		}
	}
	return s, nil
}

// Close is idempotent. Runtime leases keep operations and downloads alive until
// completion before calling it. Standalone callers must coordinate that lifetime.
func (s *Store) Close() error {
	if s != nil && s.closed.CompareAndSwap(false, true) && s.transport != nil {
		s.transport.CloseIdleConnections()
	}
	return nil
}

// Ping performs HeadBucket. It requires bucket-level access, not just PutObject.
func (s *Store) Ping(ctx context.Context) (err error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.HealthTimeout.D())
	defer cancel()
	ctx, done := s.begin(ctx, "head_bucket")
	defer func() { done(err) }()
	if s.closed.Load() {
		return object.ErrClosed
	}
	_, err = s.client.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(s.cfg.Bucket)})
	return normalize(err)
}

// Readiness honors CheckBucket, allowing intentionally write-only IAM policies.
func (s *Store) Readiness(ctx context.Context) error {
	if !s.cfg.CheckBucket {
		return health.ErrSkipped
	}
	return s.Ping(ctx)
}

func (s *Store) begin(ctx context.Context, operation string) (context.Context, func(error)) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout.D())
	var span trace.Span
	if s.tracer != nil {
		ctx, span = s.tracer.Start(ctx, "s3."+operation, trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(attribute.String("aws.s3.bucket", s.cfg.Bucket)))
	}
	var start time.Time
	if s.observer != nil {
		start = time.Now()
	}
	return ctx, func(err error) {
		if err == nil {
			err = ctx.Err()
		}
		cancel()
		outcome := "ok"
		if err != nil {
			outcome = string(apierror.KindOf(normalize(err)))
		}
		if span != nil {
			if err != nil {
				span.SetStatus(codes.Error, outcome)
			}
			span.End()
		}
		if s.observer != nil {
			s.observer.ObserveObjectStorage(operation, outcome, time.Since(start))
		}
	}
}

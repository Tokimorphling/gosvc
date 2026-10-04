package s3

import (
	"context"
	"maps"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/Tokimorphling/gosvc/apierror"
	"github.com/Tokimorphling/gosvc/store/object"
)

func (s *Store) expiry(value time.Duration) (time.Duration, error) {
	if value == 0 {
		value = s.cfg.PresignExpiry.D()
	}
	if value < time.Second || value > 7*24*time.Hour {
		return 0, apierror.New(apierror.KindInvalidArgument, "expiry must be between 1s and 168h")
	}
	return value.Truncate(time.Second), nil
}

func (s *Store) PresignGet(ctx context.Context, key string, expires time.Duration) (result object.SignedRequest, err error) {
	ctx, done := s.begin(ctx, "presign_get")
	defer func() { done(err) }()
	if s.closed.Load() {
		return result, object.ErrClosed
	}
	if err := validKey(key); err != nil {
		return result, err
	}
	expires, err = s.expiry(expires)
	if err != nil {
		return result, err
	}
	start := time.Now()
	out, err := s.presigner.PresignGetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(key)},
		func(o *awss3.PresignOptions) { o.Expires = expires })
	if err != nil {
		return result, normalize(err)
	}
	return object.SignedRequest{URL: out.URL, Method: out.Method, Headers: out.SignedHeader.Clone(), ExpiresAt: start.Add(expires)}, nil
}

func (s *Store) PresignPut(ctx context.Context, key string, opts object.PutOptions, expires time.Duration) (result object.SignedRequest, err error) {
	ctx, done := s.begin(ctx, "presign_put")
	defer func() { done(err) }()
	if s.closed.Load() {
		return result, object.ErrClosed
	}
	if err := validKey(key); err != nil {
		return result, err
	}
	if opts.Size != nil && *opts.Size < 0 {
		return result, apierror.New(apierror.KindInvalidArgument, "size must not be negative")
	}
	expires, err = s.expiry(expires)
	if err != nil {
		return result, err
	}
	start := time.Now()
	out, err := s.presigner.PresignPutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(s.cfg.Bucket), Key: aws.String(key), ContentLength: opts.Size,
		ContentType: optional(opts.ContentType), CacheControl: optional(opts.CacheControl), Metadata: maps.Clone(opts.Metadata),
	}, func(o *awss3.PresignOptions) { o.Expires = expires })
	if err != nil {
		return result, normalize(err)
	}
	return object.SignedRequest{URL: out.URL, Method: out.Method, Headers: out.SignedHeader.Clone(), ExpiresAt: start.Add(expires)}, nil
}

package s3

import (
	"context"
	"io"
	"maps"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/Tokimorphling/gosvc/apierror"
	"github.com/Tokimorphling/gosvc/internal/stream"
	"github.com/Tokimorphling/gosvc/store/object"
)

func (s *Store) Put(ctx context.Context, key string, body io.Reader, opts object.PutOptions) (result object.PutResult, err error) {
	ctx, done := s.begin(ctx, "put")
	defer func() { done(err) }()
	if s.closed.Load() {
		return result, object.ErrClosed
	}
	if err := validKey(key); err != nil {
		return result, err
	}
	if body == nil || (opts.Size != nil && *opts.Size < 0) {
		return result, apierror.New(apierror.KindInvalidArgument, "body is required and size must not be negative")
	}
	if err := ctx.Err(); err != nil {
		return result, normalize(err)
	}
	select {
	case s.uploads <- struct{}{}:
		defer func() { <-s.uploads }()
	case <-ctx.Done():
		return result, normalize(ctx.Err())
	}
	out, err := s.uploader.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket: aws.String(s.cfg.Bucket), Key: aws.String(key), Body: contextReader{ctx: ctx, reader: body}, ContentLength: opts.Size,
		ContentType: optional(opts.ContentType), CacheControl: optional(opts.CacheControl), Metadata: maps.Clone(opts.Metadata),
	})
	if err != nil {
		return result, normalize(err)
	}
	return object.PutResult{ETag: aws.ToString(out.ETag), VersionID: aws.ToString(out.VersionID), Size: aws.ToInt64(out.ContentLength)}, nil
}

func (s *Store) Get(ctx context.Context, key string, opts object.GetOptions) (*object.Object, error) {
	ctx, done := s.begin(ctx, "get")
	if s.closed.Load() {
		done(object.ErrClosed)
		return nil, object.ErrClosed
	}
	if err := validKey(key); err != nil {
		done(err)
		return nil, err
	}
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.cfg.Bucket), Key: aws.String(key), Range: optional(opts.Range), VersionId: optional(opts.VersionID),
	})
	if err != nil {
		err = normalize(err)
		done(err)
		return nil, err
	}
	return &object.Object{
		Key: key, Size: aws.ToInt64(out.ContentLength), ETag: aws.ToString(out.ETag), ContentType: aws.ToString(out.ContentType),
		LastModified: aws.ToTime(out.LastModified), Metadata: out.Metadata, VersionID: aws.ToString(out.VersionId),
		Body: stream.WrapContext(ctx, out.Body, done),
	}, nil
}

func (s *Store) Head(ctx context.Context, key string) (result object.Info, err error) {
	ctx, done := s.begin(ctx, "head")
	defer func() { done(err) }()
	if s.closed.Load() {
		return result, object.ErrClosed
	}
	if err := validKey(key); err != nil {
		return result, err
	}
	out, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(key)})
	if err != nil {
		return result, normalize(err)
	}
	return object.Info{Key: key, Size: aws.ToInt64(out.ContentLength), ETag: aws.ToString(out.ETag), ContentType: aws.ToString(out.ContentType),
		LastModified: aws.ToTime(out.LastModified), Metadata: out.Metadata, VersionID: aws.ToString(out.VersionId)}, nil
}

func (s *Store) Delete(ctx context.Context, key string) (err error) {
	ctx, done := s.begin(ctx, "delete")
	defer func() { done(err) }()
	if s.closed.Load() {
		return object.ErrClosed
	}
	if err := validKey(key); err != nil {
		return err
	}
	_, err = s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(key)})
	return normalize(err)
}

func (s *Store) List(ctx context.Context, opts object.ListOptions) (page object.Page, err error) {
	ctx, done := s.begin(ctx, "list")
	defer func() { done(err) }()
	if s.closed.Load() {
		return page, object.ErrClosed
	}
	if opts.Limit < 0 || opts.Limit > 1000 {
		return page, apierror.New(apierror.KindInvalidArgument, "list limit must be between 1 and 1000, or 0 for default")
	}
	if opts.Limit == 0 {
		opts.Limit = 1000
	}
	out, err := s.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: aws.String(s.cfg.Bucket), Prefix: optional(opts.Prefix), Delimiter: optional(opts.Delimiter),
		ContinuationToken: optional(opts.ContinuationToken), MaxKeys: aws.Int32(opts.Limit),
	})
	if err != nil {
		return page, normalize(err)
	}
	page.Objects = make([]object.Info, 0, len(out.Contents))
	for _, item := range out.Contents {
		page.Objects = append(page.Objects, object.Info{Key: aws.ToString(item.Key), Size: aws.ToInt64(item.Size), ETag: aws.ToString(item.ETag), LastModified: aws.ToTime(item.LastModified)})
	}
	for _, prefix := range out.CommonPrefixes {
		page.CommonPrefixes = append(page.CommonPrefixes, aws.ToString(prefix.Prefix))
	}
	page.NextToken, page.Truncated = aws.ToString(out.NextContinuationToken), aws.ToBool(out.IsTruncated)
	return page, nil
}

func optional(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

// Check cancellation between reads without spawning a goroutine per read or
// taking ownership of the caller's input. An already-blocked Read must still
// be interrupted by its owner (for example by closing a pipe).
type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

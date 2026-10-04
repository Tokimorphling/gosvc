package s3

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/Tokimorphling/gosvc/apierror"
)

func normalize(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*apierror.Error](err); ok {
		return err
	}
	kind, message := apierror.KindUnavailable, "object storage request failed"
	if response, ok := errors.AsType[*smithyhttp.ResponseError](err); ok {
		switch response.HTTPStatusCode() {
		case 400, 416:
			kind, message = apierror.KindInvalidArgument, "invalid object storage request"
		case 401:
			kind, message = apierror.KindUnauthenticated, "object storage authentication failed"
		case 403:
			kind, message = apierror.KindPermissionDenied, "object storage access denied"
		case 404:
			kind, message = apierror.KindNotFound, "object or bucket not found"
		case 409, 412:
			kind, message = apierror.KindConflict, "object storage conflict"
		case 429:
			kind, message = apierror.KindRateLimited, "object storage rate limit exceeded"
		}
	}
	if service, ok := errors.AsType[smithy.APIError](err); ok {
		switch service.ErrorCode() {
		case "NoSuchKey", "NoSuchBucket", "NotFound":
			kind, message = apierror.KindNotFound, "object or bucket not found"
		case "SlowDown", "Throttling", "ThrottlingException":
			kind, message = apierror.KindRateLimited, "object storage rate limit exceeded"
		case "InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken", "InvalidToken":
			kind, message = apierror.KindUnauthenticated, "object storage authentication failed"
		}
	}
	if errors.Is(err, context.Canceled) {
		message = "object storage request canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		message = "object storage request timed out"
	}
	return apierror.Wrap(err, kind, message)
}

func validKey(key string) error {
	if key == "" || len(key) > 1024 || !utf8.ValidString(key) || strings.IndexByte(key, 0) >= 0 {
		return apierror.New(apierror.KindInvalidArgument, "key must contain 1 to 1024 UTF-8 bytes without NUL")
	}
	return nil
}

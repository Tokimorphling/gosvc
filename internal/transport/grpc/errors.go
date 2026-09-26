package grpc

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"example.com/gosvc/internal/apierror"
)

// toStatus maps domain errors onto gRPC status codes. Errors that already are
// status errors are passed through unchanged.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if s, ok := status.FromError(err); ok && s.Code() != codes.Unknown {
		return err
	}

	switch apierror.KindOf(err) {
	case apierror.KindInvalidArgument:
		return status.Error(codes.InvalidArgument, apierror.ClientMessage(err))
	case apierror.KindNotFound:
		return status.Error(codes.NotFound, apierror.ClientMessage(err))
	case apierror.KindConflict:
		return status.Error(codes.AlreadyExists, apierror.ClientMessage(err))
	case apierror.KindPermissionDenied:
		return status.Error(codes.PermissionDenied, apierror.ClientMessage(err))
	case apierror.KindRateLimited:
		return status.Error(codes.ResourceExhausted, apierror.ClientMessage(err))
	case apierror.KindUnavailable:
		return status.Error(codes.Unavailable, apierror.ClientMessage(err))
	default:
		return status.Error(codes.Internal, apierror.ClientMessage(err))
	}
}

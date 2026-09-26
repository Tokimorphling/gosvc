// Package kitexexample demonstrates adding Kitex as an optional
// service-to-service RPC layer on top of this template.
//
// The generated code lives in api/kitex/echo (see api/kitex/echo.thrift and
// `make kitex`). This example is deliberately isolated from the main binary:
// nothing under internal/ imports Kitex, so the core service stays lean.
package kitexexample

import (
	"context"
	"fmt"

	"example.com/gosvc/api/kitex/echo"
)

// EchoServiceImpl implements the generated echo.EchoService interface.
type EchoServiceImpl struct {
	Prefix string
}

// Echo returns the message, optionally prefixed.
func (s *EchoServiceImpl) Echo(_ context.Context, message string) (string, error) {
	if message == "" {
		return "", fmt.Errorf("message must not be empty")
	}
	return s.Prefix + message, nil
}

var _ echo.EchoService = (*EchoServiceImpl)(nil)

// Package kitexexample demonstrates adding Kitex as an optional
// service-to-service RPC layer on top of this template.
//
// The generated code lives in examples/kitex/api/echo (see
// examples/kitex/idl/echo.thrift and `make kitex`). This example is
// deliberately isolated from the library: no package under the module root
// imports Kitex, so importing gosvc stays lean.
package kitexexample

import (
	"context"
	"fmt"

	"github.com/Tokimorphling/gosvc/examples/kitex/api/echo"
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

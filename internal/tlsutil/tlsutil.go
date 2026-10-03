// Package tlsutil wraps listeners with TLS from configuration.
//
// It is shared by the HTTP and gRPC transports. The TCP transport does not use
// it: netpoll needs raw connections, so TLS there stays terminated at a
// gateway (see transport/tcp design notes).
package tlsutil

import (
	"crypto/tls"
	"fmt"
	"net"

	"github.com/Tokimorphling/gosvc/config"
)

// Wrap returns a TLS listener for cfg, or the plaintext listener when TLS is
// not configured. The certificate pair is loaded eagerly so a missing or
// invalid pair fails startup, not the first handshake.
func Wrap(listener net.Listener, cfg config.TLSConfig) (net.Listener, error) {
	if !cfg.Enabled() {
		return listener, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load TLS key pair (%s, %s): %w", cfg.CertFile, cfg.KeyFile, err)
	}
	return tls.NewListener(listener, &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}), nil
}

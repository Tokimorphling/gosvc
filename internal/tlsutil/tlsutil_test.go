package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Tokimorphling/gosvc/config"
)

// writeTestCert generates a self-signed certificate/key pair in dir and
// returns the TLSConfig pointing at it.
func writeTestCert(t *testing.T, dir string) config.TLSConfig {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "gosvc-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}

	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	certOut := pemEncode(t, "CERTIFICATE", der)
	if err := os.WriteFile(certPath, certOut, 0o600); err != nil {
		t.Fatal(err)
	}
	keyOut := pemEncode(t, "EC PRIVATE KEY", keyDER)
	if err := os.WriteFile(keyPath, keyOut, 0o600); err != nil {
		t.Fatal(err)
	}
	return config.TLSConfig{CertFile: certPath, KeyFile: keyPath}
}

func pemEncode(t *testing.T, blockType string, der []byte) []byte {
	t.Helper()
	return pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
}

func TestWrapKeepsPlaintextWhenDisabled(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	wrapped, err := Wrap(listener, config.TLSConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if wrapped != listener {
		t.Fatal("disabled TLS must return the listener unchanged")
	}
}

func TestWrapServesTLS(t *testing.T) {
	cfg := writeTestCert(t, t.TempDir())

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	wrapped, err := Wrap(listener, cfg)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		conn, err := wrapped.Accept()
		if err != nil {
			done <- err
			return
		}
		_, _ = conn.Write([]byte("hello-tls"))
		_ = conn.Close()
		done <- nil
	}()

	// InsecureSkipVerify is fine here: the test only checks that the
	// handshake succeeds against a self-signed certificate.
	clientConn, err := tls.Dial("tcp", listener.Addr().String(), &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // self-signed test certificate
		ServerName:         "gosvc-test",
	})
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	defer clientConn.Close()

	buf := make([]byte, len("hello-tls"))
	if _, err := clientConn.Read(buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != "hello-tls" {
		t.Fatalf("received %q", buf)
	}
	if err := <-done; err != nil {
		t.Fatalf("accept: %v", err)
	}
}

func TestWrapRejectsMissingPair(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	if _, err := Wrap(listener, config.TLSConfig{CertFile: "does-not-exist.pem", KeyFile: "neither.pem"}); err == nil {
		t.Fatal("a missing key pair must fail")
	}
}

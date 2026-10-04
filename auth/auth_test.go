package auth

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/Tokimorphling/gosvc/apierror"
	"github.com/Tokimorphling/gosvc/config"
)

func TestDisabledAllowsAnonymous(t *testing.T) {
	a, err := New(config.AuthConfig{Enabled: false})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	identity, err := a.Authenticate("", "")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if identity.Method != MethodAnonymous {
		t.Fatalf("method = %q", identity.Method)
	}
}

func TestNilAuthenticatorAllowsAnonymous(t *testing.T) {
	var a *Authenticator
	identity, err := a.Authenticate("", "")
	if err != nil || identity.Method != MethodAnonymous {
		t.Fatalf("nil authenticator: identity=%+v err=%v", identity, err)
	}
}

func TestAuthenticateAcrossConcurrentReloads(t *testing.T) {
	a, err := New(config.AuthConfig{Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 500 {
				identity, err := a.Authenticate("", "")
				if err == nil {
					if identity == nil || identity.Method != MethodAnonymous {
						t.Errorf("unexpected successful identity: %+v", identity)
						return
					}
				} else if apierror.KindOf(err) != apierror.KindUnauthenticated {
					t.Errorf("unexpected auth error: %v", err)
					return
				}
			}
		})
	}
	for range 200 {
		if err := a.Reload(config.AuthConfig{Enabled: true, APIKeys: []string{"key"}}); err != nil {
			t.Fatal(err)
		}
		if _, err := a.Authenticate("", ""); apierror.KindOf(err) != apierror.KindUnauthenticated {
			t.Fatalf("enabled authenticator allowed missing key: %v", err)
		}
		if err := a.Reload(config.AuthConfig{Enabled: false}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}

func TestAPIKey(t *testing.T) {
	a, err := New(config.AuthConfig{Enabled: true, APIKeys: []string{"secret-key"}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := a.Authenticate("", ""); apierror.KindOf(err) != apierror.KindUnauthenticated {
		t.Fatalf("missing key: kind = %v", apierror.KindOf(err))
	}
	if _, err := a.Authenticate("", "wrong"); apierror.KindOf(err) != apierror.KindUnauthenticated {
		t.Fatalf("wrong key: kind = %v", apierror.KindOf(err))
	}

	identity, err := a.Authenticate("", "secret-key")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if identity.Method != MethodAPIKey {
		t.Fatalf("method = %q", identity.Method)
	}
}

func TestJWT(t *testing.T) {
	const secret = "0123456789abcdef"
	a, err := New(config.AuthConfig{
		Enabled: true,
		JWT:     config.JWTConfig{Secret: secret, Issuer: "gosvc"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   "user-1",
		Issuer:    "gosvc",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
	})
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	identity, err := a.Authenticate(signed, "")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if identity.Subject != "user-1" || identity.Method != MethodJWT {
		t.Fatalf("identity = %+v", identity)
	}

	// Wrong secret must fail.
	other, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{Subject: "user-1"}).SignedString([]byte("another-secret-1234"))
	if _, err := a.Authenticate(other, ""); apierror.KindOf(err) != apierror.KindUnauthenticated {
		t.Fatalf("wrong secret: kind = %v", apierror.KindOf(err))
	}

	// Wrong algorithm must fail.
	none, _ := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.RegisteredClaims{Subject: "user-1"}).SignedString(jwt.UnsafeAllowNoneSignatureType)
	if _, err := a.Authenticate(none, ""); apierror.KindOf(err) != apierror.KindUnauthenticated {
		t.Fatalf("alg none: kind = %v", apierror.KindOf(err))
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	if _, err := New(config.AuthConfig{Enabled: true}); err == nil {
		t.Fatal("expected an error when enabled without credentials")
	}
	if _, err := New(config.AuthConfig{Enabled: true, JWT: config.JWTConfig{Secret: "short"}}); err == nil {
		t.Fatal("expected an error for a short secret")
	}
}

func TestBearerToken(t *testing.T) {
	if got := BearerToken("Bearer abc"); got != "abc" {
		t.Fatalf("got %q", got)
	}
	if got := BearerToken("bearer  abc "); got != "abc" {
		t.Fatalf("got %q", got)
	}
	if got := BearerToken("Basic abc"); got != "" {
		t.Fatalf("got %q", got)
	}
}

func TestReloadSwapsCredentials(t *testing.T) {
	a, err := New(config.AuthConfig{Enabled: true, APIKeys: []string{"key-1"}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := a.Authenticate("", "key-1"); err != nil {
		t.Fatalf("key-1 must work before reload: %v", err)
	}

	if err := a.Reload(config.AuthConfig{Enabled: true, APIKeys: []string{"key-2"}}); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	if _, err := a.Authenticate("", "key-1"); apierror.KindOf(err) != apierror.KindUnauthenticated {
		t.Fatalf("key-1 must be rejected after reload, kind = %v", apierror.KindOf(err))
	}
	if _, err := a.Authenticate("", "key-2"); err != nil {
		t.Fatalf("key-2 must work after reload: %v", err)
	}

	// Disabling accepts everyone again.
	if err := a.Reload(config.AuthConfig{Enabled: false}); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if identity, err := a.Authenticate("", ""); err != nil || identity.Method != MethodAnonymous {
		t.Fatalf("identity = %+v, err = %v", identity, err)
	}

	// Invalid configurations must not change the active state.
	if err := a.Reload(config.AuthConfig{Enabled: true}); err == nil {
		t.Fatal("expected an error for enabled without credentials")
	}
	if identity, err := a.Authenticate("", ""); err != nil || identity.Method != MethodAnonymous {
		t.Fatalf("failed reload must keep the previous state, identity = %+v, err = %v", identity, err)
	}
}

var _ = errors.Is // keep errors imported for future assertions

// Package auth authenticates API keys and HS256 JWTs for all transports.
package auth

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-jwt/jwt/v5"

	"example.com/gosvc/internal/apierror"
	"example.com/gosvc/internal/config"
)

// Method describes how a caller authenticated.
type Method string

const (
	MethodAnonymous Method = "anonymous"
	MethodAPIKey    Method = "api_key"
	MethodJWT       Method = "jwt"
)

// Identity is the authenticated caller.
type Identity struct {
	Subject string
	Method  Method
}

type ctxKey struct{}

// WithIdentity stores the identity in the context.
func WithIdentity(ctx context.Context, identity *Identity) context.Context {
	return context.WithValue(ctx, ctxKey{}, identity)
}

// FromIdentity returns the identity stored in the context, if any.
func FromIdentity(ctx context.Context) *Identity {
	if ctx == nil {
		return nil
	}
	if identity, ok := ctx.Value(ctxKey{}).(*Identity); ok {
		return identity
	}
	return nil
}

// Authenticator validates credentials. A nil or disabled Authenticator accepts
// everyone as anonymous.
type Authenticator struct {
	enabled     bool
	apiKeys     [][]byte
	jwtSecret   []byte
	jwtIssuer   string
	jwtAudience string
}

// New builds an Authenticator from config.
func New(cfg config.AuthConfig) (*Authenticator, error) {
	a := &Authenticator{
		enabled:     cfg.Enabled,
		jwtIssuer:   strings.TrimSpace(cfg.JWT.Issuer),
		jwtAudience: strings.TrimSpace(cfg.JWT.Audience),
	}

	for _, key := range cfg.APIKeys {
		if key = strings.TrimSpace(key); key != "" {
			a.apiKeys = append(a.apiKeys, []byte(key))
		}
	}
	if secret := strings.TrimSpace(cfg.JWT.Secret); secret != "" {
		if len(secret) < 16 {
			return nil, fmt.Errorf("auth.jwt.secret must be at least 16 characters")
		}
		a.jwtSecret = []byte(secret)
	}

	if a.enabled && len(a.apiKeys) == 0 && len(a.jwtSecret) == 0 {
		return nil, errors.New("auth is enabled but neither api keys nor a jwt secret are configured")
	}
	return a, nil
}

// Enabled reports whether authentication is enforced.
func (a *Authenticator) Enabled() bool { return a != nil && a.enabled }

// Authenticate resolves credentials, preferring the bearer token.
func (a *Authenticator) Authenticate(bearerToken, apiKey string) (*Identity, error) {
	if !a.Enabled() {
		return &Identity{Subject: "anonymous", Method: MethodAnonymous}, nil
	}
	if bearerToken != "" {
		return a.authenticateJWT(bearerToken)
	}
	return a.authenticateAPIKey(apiKey)
}

func (a *Authenticator) authenticateAPIKey(key string) (*Identity, error) {
	if len(a.apiKeys) == 0 {
		return nil, apierror.New(apierror.KindUnauthenticated, "API key authentication is not configured")
	}
	if key == "" {
		return nil, apierror.New(apierror.KindUnauthenticated, "missing API key")
	}
	provided := []byte(key)
	for _, candidate := range a.apiKeys {
		if subtle.ConstantTimeCompare(provided, candidate) == 1 {
			return &Identity{Subject: "api-key", Method: MethodAPIKey}, nil
		}
	}
	return nil, apierror.New(apierror.KindUnauthenticated, "invalid API key")
}

func (a *Authenticator) authenticateJWT(token string) (*Identity, error) {
	if len(a.jwtSecret) == 0 {
		return nil, apierror.New(apierror.KindUnauthenticated, "JWT authentication is not configured")
	}

	claims := &jwt.RegisteredClaims{}
	options := []jwt.ParserOption{jwt.WithValidMethods([]string{"HS256"})}
	if a.jwtIssuer != "" {
		options = append(options, jwt.WithIssuer(a.jwtIssuer))
	}
	if a.jwtAudience != "" {
		options = append(options, jwt.WithAudience(a.jwtAudience))
	}

	parsed, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method %q", t.Header["alg"])
		}
		return a.jwtSecret, nil
	}, options...)
	if err != nil || !parsed.Valid {
		if err == nil {
			err = errors.New("token is not valid")
		}
		return nil, apierror.Wrap(err, apierror.KindUnauthenticated, "invalid bearer token")
	}

	subject := claims.Subject
	if subject == "" {
		subject = "jwt"
	}
	return &Identity{Subject: subject, Method: MethodJWT}, nil
}

// BearerToken extracts the token from an Authorization header value.
func BearerToken(header string) string {
	header = strings.TrimSpace(header)
	if len(header) >= 7 && strings.EqualFold(header[:7], "bearer ") {
		return strings.TrimSpace(header[7:])
	}
	return ""
}

// Package config loads, merges and validates service configuration.
//
// Precedence: defaults < JSON file < environment variables.
package config

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the root configuration object.
type Config struct {
	Service ServiceConfig `json:"service"`
	HTTP    HTTPConfig    `json:"http"`
	GRPC    GRPCConfig    `json:"grpc"`
	Admin   AdminConfig   `json:"admin"`
	Log     LogConfig     `json:"log"`
	Limiter LimiterConfig `json:"limiter"`
}

// ServiceConfig holds service identity.
type ServiceConfig struct {
	Name string `json:"name"`
	Env  string `json:"env"` // dev | staging | prod
}

// HTTPConfig configures the REST/JSON-RPC server.
type HTTPConfig struct {
	Host            string   `json:"host"`
	Port            int      `json:"port"`
	ReadTimeout     Duration `json:"readTimeout"`
	WriteTimeout    Duration `json:"writeTimeout"`
	IdleTimeout     Duration `json:"idleTimeout"`
	ShutdownTimeout Duration `json:"shutdownTimeout"`
	MaxBodyBytes    int      `json:"maxBodyBytes"`
}

// Addr returns the host:port listen address.
func (c HTTPConfig) Addr() string { return net.JoinHostPort(c.Host, strconv.Itoa(c.Port)) }

// GRPCConfig configures the gRPC server.
type GRPCConfig struct {
	Host            string   `json:"host"`
	Port            int      `json:"port"`
	ShutdownTimeout Duration `json:"shutdownTimeout"`
}

// Addr returns the host:port listen address.
func (c GRPCConfig) Addr() string { return net.JoinHostPort(c.Host, strconv.Itoa(c.Port)) }

// AdminConfig configures the operations server (metrics, pprof, health).
type AdminConfig struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

// Addr returns the host:port listen address.
func (c AdminConfig) Addr() string { return net.JoinHostPort(c.Host, strconv.Itoa(c.Port)) }

// LogConfig configures the slog logger.
type LogConfig struct {
	Level     string `json:"level"`  // debug | info | warn | error
	Format    string `json:"format"` // json | text
	AddSource bool   `json:"addSource"`
}

// LimiterConfig configures the per-client token bucket limiter.
type LimiterConfig struct {
	RPS   float64 `json:"rps"`   // 0 disables rate limiting
	Burst int     `json:"burst"` // required when RPS > 0
}

// Duration is a time.Duration that unmarshals from either a Go duration string
// ("5s", "1m") or a number of seconds in JSON.
type Duration time.Duration

// D returns the underlying time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

func (d Duration) String() string { return time.Duration(d).String() }

// MarshalJSON implements json.Marshaler.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var raw any
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	switch v := raw.(type) {
	case nil:
		return nil
	case string:
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", v, err)
		}
		*d = Duration(parsed)
	case float64:
		*d = Duration(time.Duration(v * float64(time.Second)))
	default:
		return fmt.Errorf("invalid duration: %v", raw)
	}
	return nil
}

// Default returns a configuration with sensible defaults.
func Default() *Config {
	return &Config{
		Service: ServiceConfig{Name: "gosvc", Env: "dev"},
		HTTP: HTTPConfig{
			Host:            "0.0.0.0",
			Port:            8080,
			ReadTimeout:     Duration(10 * time.Second),
			WriteTimeout:    Duration(10 * time.Second),
			IdleTimeout:     Duration(60 * time.Second),
			ShutdownTimeout: Duration(10 * time.Second),
			MaxBodyBytes:    1 << 20, // 1 MiB
		},
		GRPC: GRPCConfig{
			Host:            "0.0.0.0",
			Port:            9090,
			ShutdownTimeout: Duration(10 * time.Second),
		},
		Admin:   AdminConfig{Host: "127.0.0.1", Port: 6060},
		Log:     LogConfig{Level: "info", Format: "json"},
		Limiter: LimiterConfig{RPS: 0, Burst: 0},
	}
}

// Load builds a Config from defaults, an optional JSON file and environment
// overrides, then validates it. An empty path skips the file.
func Load(path string) (*Config, error) {
	cfg := Default()

	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config %s: %w", path, err)
		}
		if err := json.Unmarshal(raw, cfg); err != nil {
			return nil, fmt.Errorf("parse config %s: %w", path, err)
		}
	}

	if err := cfg.applyEnv(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
}

func (c *Config) applyEnv() error {
	if v := os.Getenv("GOSVC_SERVICE_NAME"); v != "" {
		c.Service.Name = v
	}
	if v := os.Getenv("GOSVC_ENV"); v != "" {
		c.Service.Env = v
	}
	if v := os.Getenv("GOSVC_LOG_LEVEL"); v != "" {
		c.Log.Level = v
	}
	if v := os.Getenv("GOSVC_LOG_FORMAT"); v != "" {
		c.Log.Format = v
	}
	for _, item := range []struct {
		env  string
		host *string
		port *int
	}{
		{"GOSVC_HTTP_ADDR", &c.HTTP.Host, &c.HTTP.Port},
		{"GOSVC_GRPC_ADDR", &c.GRPC.Host, &c.GRPC.Port},
		{"GOSVC_ADMIN_ADDR", &c.Admin.Host, &c.Admin.Port},
	} {
		v := os.Getenv(item.env)
		if v == "" {
			continue
		}
		host, portStr, err := net.SplitHostPort(v)
		if err != nil {
			return fmt.Errorf("%s=%q: %w", item.env, v, err)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return fmt.Errorf("%s=%q: invalid port: %w", item.env, v, err)
		}
		*item.host = host
		*item.port = port
	}
	return nil
}

// Validate checks the configuration for obvious mistakes.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.Service.Name) == "" {
		return fmt.Errorf("service.name must not be empty")
	}
	switch c.Service.Env {
	case "dev", "staging", "prod":
	default:
		return fmt.Errorf("service.env must be one of dev|staging|prod, got %q", c.Service.Env)
	}

	for name, port := range map[string]int{
		"http.port":  c.HTTP.Port,
		"grpc.port":  c.GRPC.Port,
		"admin.port": c.Admin.Port,
	} {
		if port < 0 || port > 65535 {
			return fmt.Errorf("%s must be within [0,65535], got %d", name, port)
		}
	}

	if c.HTTP.ReadTimeout <= 0 || c.HTTP.WriteTimeout <= 0 {
		return fmt.Errorf("http read/write timeouts must be positive")
	}
	if c.HTTP.ShutdownTimeout <= 0 || c.GRPC.ShutdownTimeout <= 0 {
		return fmt.Errorf("shutdown timeouts must be positive")
	}
	if c.HTTP.MaxBodyBytes <= 0 {
		return fmt.Errorf("http.maxBodyBytes must be positive")
	}

	switch strings.ToLower(c.Log.Level) {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log.level must be one of debug|info|warn|error, got %q", c.Log.Level)
	}
	switch strings.ToLower(c.Log.Format) {
	case "json", "text":
	default:
		return fmt.Errorf("log.format must be one of json|text, got %q", c.Log.Format)
	}

	if c.Limiter.RPS < 0 || c.Limiter.Burst < 0 {
		return fmt.Errorf("limiter.rps and limiter.burst must not be negative")
	}
	if c.Limiter.RPS > 0 && c.Limiter.Burst == 0 {
		return fmt.Errorf("limiter.burst must be > 0 when limiter.rps is enabled")
	}
	return nil
}

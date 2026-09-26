// Package config loads, merges and validates service configuration.
//
// Precedence: defaults < JSON file < environment variables.
package config

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"example.com/gosvc/internal/slogx"
)

// Config is the root configuration object.
type Config struct {
	Service   ServiceConfig   `json:"service"`
	HTTP      HTTPConfig      `json:"http"`
	GRPC      GRPCConfig      `json:"grpc"`
	TCP       TCPConfig       `json:"tcp"`
	Admin     AdminConfig     `json:"admin"`
	Log       LogConfig       `json:"log"`
	Limiter   LimiterConfig   `json:"limiter"`
	Auth      AuthConfig      `json:"auth"`
	Telemetry TelemetryConfig `json:"telemetry"`
	Storage   StorageConfig   `json:"storage"`
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
	Level     string        `json:"level"`     // trace | debug | info | warn | error
	Format    string        `json:"format"`    // auto | terminal | json | logfmt
	Output    string        `json:"output"`    // stdout | file | both
	Color     string        `json:"color"`     // auto | always | never
	AddSource bool          `json:"addSource"` // add source location (JSON sinks)
	File      FileLogConfig `json:"file"`
}

// FileLogConfig configures the rotating file sink.
type FileLogConfig struct {
	Path       string `json:"path"`
	MaxSizeMB  int    `json:"maxSizeMB"`
	MaxBackups int    `json:"maxBackups"`
	MaxAgeDays int    `json:"maxAgeDays"`
	Compress   bool   `json:"compress"`
}

// LimiterConfig configures the per-client token bucket limiter.
type LimiterConfig struct {
	RPS   float64 `json:"rps"`   // 0 disables rate limiting
	Burst int     `json:"burst"` // required when RPS > 0
}

// TCPConfig configures the optional netpoll based line-delimited JSON-RPC
// server. It is meant for internal, high-connection-count traffic: terminate
// TLS at a gateway in front of it.
type TCPConfig struct {
	Enabled         bool     `json:"enabled"`
	Host            string   `json:"host"`
	Port            int      `json:"port"`
	Workers         int      `json:"workers"`       // bounded worker pool size
	QueueSize       int      `json:"queueSize"`     // bounded queue size
	MaxFrameBytes   int      `json:"maxFrameBytes"` // maximum request line size
	ReadTimeout     Duration `json:"readTimeout"`
	ShutdownTimeout Duration `json:"shutdownTimeout"`
}

// Addr returns the host:port listen address.
func (c TCPConfig) Addr() string { return net.JoinHostPort(c.Host, strconv.Itoa(c.Port)) }

// AuthConfig configures API key and JWT authentication.
type AuthConfig struct {
	Enabled bool      `json:"enabled"`
	APIKeys []string  `json:"apiKeys"`
	JWT     JWTConfig `json:"jwt"`
}

// JWTConfig configures HS256 JWT verification.
type JWTConfig struct {
	Secret   string `json:"secret"`
	Issuer   string `json:"issuer"`
	Audience string `json:"audience"`
}

// TelemetryConfig configures OpenTelemetry tracing (OTLP/HTTP).
type TelemetryConfig struct {
	Enabled      bool    `json:"enabled"`
	OTLPEndpoint string  `json:"otlpEndpoint"` // host:port
	Insecure     bool    `json:"insecure"`
	SampleRatio  float64 `json:"sampleRatio"` // 0..1
}

// StorageConfig configures optional persistence.
type StorageConfig struct {
	Redis RedisConfig `json:"redis"`
}

// RedisConfig configures the Redis backed time-series store.
type RedisConfig struct {
	Enabled   bool     `json:"enabled"`
	Addr      string   `json:"addr"`
	Password  string   `json:"password"`
	DB        int      `json:"db"`
	Prefix    string   `json:"prefix"`
	BucketTTL Duration `json:"bucketTtl"`
	QueueSize int      `json:"queueSize"`
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
		Admin: AdminConfig{Host: "127.0.0.1", Port: 6060},
		TCP: TCPConfig{
			Enabled:         false,
			Host:            "0.0.0.0",
			Port:            7070,
			Workers:         runtime.NumCPU(),
			QueueSize:       1024,
			MaxFrameBytes:   1 << 20,
			ReadTimeout:     Duration(60 * time.Second),
			ShutdownTimeout: Duration(5 * time.Second),
		},
		Log: LogConfig{
			Level:  "info",
			Format: "auto",
			Output: "stdout",
			Color:  "auto",
			File: FileLogConfig{
				MaxSizeMB:  100,
				MaxBackups: 5,
				MaxAgeDays: 7,
				Compress:   true,
			},
		},
		Limiter: LimiterConfig{RPS: 0, Burst: 0},
		Auth:    AuthConfig{Enabled: false},
		Telemetry: TelemetryConfig{
			Enabled:      false,
			OTLPEndpoint: "127.0.0.1:4318",
			Insecure:     true,
			SampleRatio:  1.0,
		},
		Storage: StorageConfig{
			Redis: RedisConfig{
				Enabled:   false,
				Addr:      "127.0.0.1:6379",
				Prefix:    "gosvc",
				BucketTTL: Duration(25 * time.Hour),
				QueueSize: 4096,
			},
		},
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
	if v := os.Getenv("GOSVC_AUTH_API_KEYS"); v != "" {
		c.Auth.Enabled = true
		c.Auth.APIKeys = nil
		for _, key := range strings.Split(v, ",") {
			if key = strings.TrimSpace(key); key != "" {
				c.Auth.APIKeys = append(c.Auth.APIKeys, key)
			}
		}
	}
	if v := os.Getenv("GOSVC_OTLP_ENDPOINT"); v != "" {
		c.Telemetry.Enabled = true
		c.Telemetry.OTLPEndpoint = v
	}
	if v := os.Getenv("GOSVC_REDIS_ADDR"); v != "" {
		c.Storage.Redis.Enabled = true
		c.Storage.Redis.Addr = v
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
		"tcp.port":   c.TCP.Port,
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

	if _, err := slogx.ParseLevel(c.Log.Level); err != nil {
		return fmt.Errorf("log.level: %w", err)
	}
	if _, err := slogx.NormalizeFormat(c.Log.Format); err != nil {
		return fmt.Errorf("log.format: %w", err)
	}
	switch strings.ToLower(strings.TrimSpace(c.Log.Output)) {
	case "", "stdout", "file", "both":
	default:
		return fmt.Errorf("log.output must be one of stdout|file|both, got %q", c.Log.Output)
	}
	switch strings.ToLower(strings.TrimSpace(c.Log.Color)) {
	case "", "auto", "always", "never":
	default:
		return fmt.Errorf("log.color must be one of auto|always|never, got %q", c.Log.Color)
	}
	if strings.EqualFold(c.Log.Output, "file") || strings.EqualFold(c.Log.Output, "both") {
		if strings.TrimSpace(c.Log.File.Path) == "" {
			return fmt.Errorf("log.file.path must be set when log.output is %q", c.Log.Output)
		}
	}
	if c.Log.File.MaxSizeMB < 0 || c.Log.File.MaxBackups < 0 || c.Log.File.MaxAgeDays < 0 {
		return fmt.Errorf("log.file.maxSizeMB, maxBackups and maxAgeDays must not be negative")
	}

	if c.Limiter.RPS < 0 || c.Limiter.Burst < 0 {
		return fmt.Errorf("limiter.rps and limiter.burst must not be negative")
	}
	if c.Limiter.RPS > 0 && c.Limiter.Burst == 0 {
		return fmt.Errorf("limiter.burst must be > 0 when limiter.rps is enabled")
	}

	if c.Auth.Enabled {
		if len(c.Auth.APIKeys) == 0 && strings.TrimSpace(c.Auth.JWT.Secret) == "" {
			return fmt.Errorf("auth.enabled requires auth.apiKeys or auth.jwt.secret")
		}
		if secret := strings.TrimSpace(c.Auth.JWT.Secret); secret != "" && len(secret) < 16 {
			return fmt.Errorf("auth.jwt.secret must be at least 16 characters")
		}
	}

	if c.Telemetry.Enabled {
		if strings.TrimSpace(c.Telemetry.OTLPEndpoint) == "" {
			return fmt.Errorf("telemetry.otlpEndpoint must be set when telemetry is enabled")
		}
		if c.Telemetry.SampleRatio < 0 || c.Telemetry.SampleRatio > 1 {
			return fmt.Errorf("telemetry.sampleRatio must be within [0,1]")
		}
	}

	if c.TCP.Enabled {
		if c.TCP.Workers <= 0 {
			return fmt.Errorf("tcp.workers must be > 0")
		}
		if c.TCP.QueueSize <= 0 {
			return fmt.Errorf("tcp.queueSize must be > 0")
		}
		if c.TCP.MaxFrameBytes <= 0 {
			return fmt.Errorf("tcp.maxFrameBytes must be > 0")
		}
		if c.TCP.ReadTimeout <= 0 || c.TCP.ShutdownTimeout <= 0 {
			return fmt.Errorf("tcp read/shutdown timeouts must be positive")
		}
	}

	if c.Storage.Redis.Enabled {
		if strings.TrimSpace(c.Storage.Redis.Addr) == "" {
			return fmt.Errorf("storage.redis.addr must be set when redis is enabled")
		}
		if c.Storage.Redis.BucketTTL <= 0 {
			return fmt.Errorf("storage.redis.bucketTtl must be positive")
		}
		if c.Storage.Redis.QueueSize <= 0 {
			return fmt.Errorf("storage.redis.queueSize must be > 0")
		}
	}
	return nil
}

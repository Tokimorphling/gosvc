// Package config loads, merges and validates service configuration.
//
// Precedence: defaults < TOML file < environment variables. Durations are
// written as strings ("10s", "1h30m") or numbers (seconds).
package config

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"

	"example.com/gosvc/slogx"
)

// Config is the runtime configuration. Applications embed it and add their own
// sections.
type Config struct {
	Service   ServiceConfig   `json:"service" toml:"service"`
	HTTP      HTTPConfig      `json:"http" toml:"http"`
	GRPC      GRPCConfig      `json:"grpc" toml:"grpc"`
	TCP       TCPConfig       `json:"tcp" toml:"tcp"`
	Admin     AdminConfig     `json:"admin" toml:"admin"`
	Log       LogConfig       `json:"log" toml:"log"`
	Limiter   LimiterConfig   `json:"limiter" toml:"limiter"`
	Auth      AuthConfig      `json:"auth" toml:"auth"`
	Telemetry TelemetryConfig `json:"telemetry" toml:"telemetry"`
	Storage   StorageConfig   `json:"storage" toml:"storage"`
}

// ServiceConfig holds service identity.
type ServiceConfig struct {
	Name string `json:"name" toml:"name"`
	Env  string `json:"env" toml:"env"` // dev | staging | prod
}

// HTTPConfig configures the REST/JSON-RPC server.
type HTTPConfig struct {
	Host            string   `json:"host" toml:"host"`
	Port            int      `json:"port" toml:"port"`
	ReadTimeout     Duration `json:"readTimeout" toml:"readTimeout"`
	WriteTimeout    Duration `json:"writeTimeout" toml:"writeTimeout"`
	IdleTimeout     Duration `json:"idleTimeout" toml:"idleTimeout"`
	ShutdownTimeout Duration `json:"shutdownTimeout" toml:"shutdownTimeout"`
	MaxBodyBytes    int      `json:"maxBodyBytes" toml:"maxBodyBytes"`
}

// Addr returns the host:port listen address.
func (c HTTPConfig) Addr() string { return net.JoinHostPort(c.Host, strconv.Itoa(c.Port)) }

// GRPCConfig configures the gRPC server.
type GRPCConfig struct {
	Host            string   `json:"host" toml:"host"`
	Port            int      `json:"port" toml:"port"`
	ShutdownTimeout Duration `json:"shutdownTimeout" toml:"shutdownTimeout"`
}

// Addr returns the host:port listen address.
func (c GRPCConfig) Addr() string { return net.JoinHostPort(c.Host, strconv.Itoa(c.Port)) }

// TCPConfig configures the optional netpoll based line-delimited JSON-RPC
// server. It is meant for internal, high-connection-count traffic: terminate
// TLS at a gateway in front of it.
type TCPConfig struct {
	Enabled         bool     `json:"enabled" toml:"enabled"`
	Host            string   `json:"host" toml:"host"`
	Port            int      `json:"port" toml:"port"`
	Workers         int      `json:"workers" toml:"workers"`     // bounded worker pool size
	QueueSize       int      `json:"queueSize" toml:"queueSize"` // bounded queue size
	MaxFrameBytes   int      `json:"maxFrameBytes" toml:"maxFrameBytes"`
	ReadTimeout     Duration `json:"readTimeout" toml:"readTimeout"`
	ShutdownTimeout Duration `json:"shutdownTimeout" toml:"shutdownTimeout"`
}

// Addr returns the host:port listen address.
func (c TCPConfig) Addr() string { return net.JoinHostPort(c.Host, strconv.Itoa(c.Port)) }

// AdminConfig configures the operations server (metrics, pprof, health).
type AdminConfig struct {
	Host string `json:"host" toml:"host"`
	Port int    `json:"port" toml:"port"`
}

// Addr returns the host:port listen address.
func (c AdminConfig) Addr() string { return net.JoinHostPort(c.Host, strconv.Itoa(c.Port)) }

// LogConfig configures the slog logger.
type LogConfig struct {
	Level     string            `json:"level" toml:"level"`
	Format    string            `json:"format" toml:"format"` // auto | terminal | json | logfmt
	Output    string            `json:"output" toml:"output"` // stdout | file | both
	Color     string            `json:"color" toml:"color"`   // auto | always | never
	AddSource bool              `json:"addSource" toml:"addSource"`
	File      FileLogConfig     `json:"file" toml:"file"`
	Sampling  SamplingLogConfig `json:"sampling" toml:"sampling"`
	Access    AccessLogConfig   `json:"access" toml:"access"`
}

// AccessLogConfig routes request logs (HTTP/gRPC) to a dedicated sink. When
// disabled, access logs share the main logger. Enabling or disabling it
// requires a restart; format, output and level are hot reloadable.
type AccessLogConfig struct {
	Enabled bool          `json:"enabled" toml:"enabled"`
	Level   string        `json:"level" toml:"level"`   // debug | info | warn | error
	Format  string        `json:"format" toml:"format"` // auto | terminal | json | logfmt
	Output  string        `json:"output" toml:"output"` // stdout | file | both
	Color   string        `json:"color" toml:"color"`   // auto | always | never
	File    FileLogConfig `json:"file" toml:"file"`
}

// FileLogConfig configures the rotating file sink.
type FileLogConfig struct {
	Path       string `json:"path" toml:"path"`
	MaxSizeMB  int    `json:"maxSizeMB" toml:"maxSizeMB"`
	MaxBackups int    `json:"maxBackups" toml:"maxBackups"`
	MaxAgeDays int    `json:"maxAgeDays" toml:"maxAgeDays"`
	Compress   bool   `json:"compress" toml:"compress"`
}

// SamplingLogConfig configures per-message log sampling for high QPS services.
type SamplingLogConfig struct {
	Enabled    bool     `json:"enabled" toml:"enabled"`
	Initial    int      `json:"initial" toml:"initial"`
	Thereafter int      `json:"thereafter" toml:"thereafter"`
	Tick       Duration `json:"tick" toml:"tick"`
}

// LimiterConfig configures the per-client token bucket limiter.
type LimiterConfig struct {
	RPS   float64 `json:"rps" toml:"rps"`     // 0 disables rate limiting
	Burst int     `json:"burst" toml:"burst"` // required when RPS > 0
}

// AuthConfig configures API key and JWT authentication.
type AuthConfig struct {
	Enabled bool      `json:"enabled" toml:"enabled"`
	APIKeys []string  `json:"apiKeys" toml:"apiKeys"`
	JWT     JWTConfig `json:"jwt" toml:"jwt"`
}

// JWTConfig configures HS256 JWT verification.
type JWTConfig struct {
	Secret   string `json:"secret" toml:"secret"`
	Issuer   string `json:"issuer" toml:"issuer"`
	Audience string `json:"audience" toml:"audience"`
}

// TelemetryConfig configures OpenTelemetry tracing (OTLP/HTTP).
type TelemetryConfig struct {
	Enabled      bool     `json:"enabled" toml:"enabled"`
	OTLPEndpoint string   `json:"otlpEndpoint" toml:"otlpEndpoint"` // host:port
	Insecure     bool     `json:"insecure" toml:"insecure"`
	SampleRatio  float64  `json:"sampleRatio" toml:"sampleRatio"` // 0..1
	BatchTimeout Duration `json:"batchTimeout" toml:"batchTimeout"`
}

// StorageConfig configures optional persistence.
type StorageConfig struct {
	Redis    RedisConfig    `json:"redis" toml:"redis"`
	Postgres PostgresConfig `json:"postgres" toml:"postgres"`
}

// RedisConfig configures the Redis backed time-series store.
type RedisConfig struct {
	Enabled    bool     `json:"enabled" toml:"enabled"`
	Mode       string   `json:"mode" toml:"mode"` // single | cluster | sentinel
	Addr       string   `json:"addr" toml:"addr"` // single
	Addrs      []string `json:"addrs" toml:"addrs"`
	MasterName string   `json:"masterName" toml:"masterName"` // sentinel
	Password   string   `json:"password" toml:"password"`
	DB         int      `json:"db" toml:"db"` // single/sentinel
	Prefix     string   `json:"prefix" toml:"prefix"`
	BucketTTL  Duration `json:"bucketTtl" toml:"bucketTtl"`
	QueueSize  int      `json:"queueSize" toml:"queueSize"`
	PoolSize   int      `json:"poolSize" toml:"poolSize"`
}

// PostgresConfig configures the PostgreSQL connection pool.
type PostgresConfig struct {
	Enabled         bool     `json:"enabled" toml:"enabled"`
	DSN             string   `json:"dsn" toml:"dsn"`
	MaxOpenConns    int      `json:"maxOpenConns" toml:"maxOpenConns"`
	MaxIdleConns    int      `json:"maxIdleConns" toml:"maxIdleConns"`
	ConnMaxLifetime Duration `json:"connMaxLifetime" toml:"connMaxLifetime"`
	ConnMaxIdleTime Duration `json:"connMaxIdleTime" toml:"connMaxIdleTime"`
	PingTimeout     Duration `json:"pingTimeout" toml:"pingTimeout"`
}

// Duration is a time.Duration that decodes from a TOML string ("10s", "1h30m")
// or a number of seconds, and marshals to a string for the admin JSON API.
type Duration time.Duration

// D returns the underlying time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

func (d Duration) String() string { return time.Duration(d).String() }

// MarshalJSON implements json.Marshaler.
func (d Duration) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(time.Duration(d).String())), nil
}

// UnmarshalTOML implements toml.Unmarshaler.
func (d *Duration) UnmarshalTOML(value any) error {
	switch v := value.(type) {
	case string:
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", v, err)
		}
		*d = Duration(parsed)
	case int64:
		*d = Duration(time.Duration(v) * time.Second)
	case float64:
		*d = Duration(time.Duration(v * float64(time.Second)))
	case nil:
		return nil
	default:
		return fmt.Errorf("invalid duration: %v", value)
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
		Admin: AdminConfig{Host: "127.0.0.1", Port: 6060},
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
			Sampling: SamplingLogConfig{
				Enabled:    false,
				Initial:    100,
				Thereafter: 100,
				Tick:       Duration(time.Second),
			},
			Access: AccessLogConfig{
				Enabled: false,
				Level:   "info",
				Format:  "json",
				Output:  "stdout",
				Color:   "auto",
				File: FileLogConfig{
					MaxSizeMB:  100,
					MaxBackups: 5,
					MaxAgeDays: 7,
					Compress:   true,
				},
			},
		},
		Limiter: LimiterConfig{RPS: 0, Burst: 0},
		Auth:    AuthConfig{Enabled: false},
		Telemetry: TelemetryConfig{
			Enabled:      false,
			OTLPEndpoint: "127.0.0.1:4318",
			Insecure:     true,
			SampleRatio:  1.0,
			BatchTimeout: Duration(5 * time.Second),
		},
		Storage: StorageConfig{
			Redis: RedisConfig{
				Enabled:   false,
				Mode:      "single",
				Addr:      "127.0.0.1:6379",
				Prefix:    "gosvc",
				BucketTTL: Duration(25 * time.Hour),
				QueueSize: 4096,
			},
			Postgres: PostgresConfig{
				Enabled:         false,
				MaxOpenConns:    16,
				MaxIdleConns:    4,
				ConnMaxLifetime: Duration(time.Hour),
				ConnMaxIdleTime: Duration(10 * time.Minute),
				PingTimeout:     Duration(3 * time.Second),
			},
		},
	}
}

// Configurable is implemented by application configuration types that embed
// Config. Because Config provides SetDefaults and Validate with pointer
// receivers, an embedding type satisfies this interface automatically and can
// override either method to add its own defaults or checks.
type Configurable interface {
	SetDefaults()
	Validate() error
}

// EnvApplier is implemented by Config and promoted to embedding types.
type EnvApplier interface {
	ApplyEnv(prefix string) error
}

// Source describes where a configuration comes from.
type Source struct {
	// Path is the TOML file path; empty means defaults plus environment only.
	Path string
	// EnvPrefix is the environment variable prefix, for example "MYAPP" for
	// MYAPP_HTTP_ADDR. Empty means "GOSVC".
	EnvPrefix string
	// Strict rejects unknown keys in the file, which catches typos. Hot reload
	// typically leaves it off so application sections can coexist with the
	// runtime config.
	Strict bool
}

const defaultEnvPrefix = "GOSVC"

// Load reads defaults, then the TOML file, then environment overrides into a
// freshly allocated T and validates the result.
//
// The PT type parameter is the idiomatic way to require that *T satisfies
// Configurable, which keeps the check at compile time:
//
//	cfg, err := config.Source{Path: "config.toml"}.Load[config.Config]()
//	appCfg, err := config.Source{Path: "config.toml", EnvPrefix: "MYAPP"}.Load[app.Config]()
func (s Source) Load[T any, PT interface {
	*T
	Configurable
}]() (*T, error) {
	target := PT(new(T))
	target.SetDefaults()

	if s.Path != "" {
		meta, err := toml.DecodeFile(s.Path, target)
		if err != nil {
			return nil, fmt.Errorf("parse config %s: %w", s.Path, err)
		}
		if s.Strict {
			if undecoded := meta.Undecoded(); len(undecoded) > 0 {
				keys := make([]string, 0, len(undecoded))
				for _, key := range undecoded {
					keys = append(keys, key.String())
				}
				return nil, fmt.Errorf("config %s: unknown keys: %s", s.Path, strings.Join(keys, ", "))
			}
		}
	}

	if applier, ok := any(target).(EnvApplier); ok {
		if err := applier.ApplyEnv(s.EnvPrefix); err != nil {
			return nil, err
		}
	}

	if err := target.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return (*T)(target), nil
}

// SetDefaults resets the configuration to the built-in defaults.
func (c *Config) SetDefaults() { *c = *Default() }

// ApplyEnv applies environment overrides with the given prefix (empty means
// "GOSVC").
func (c *Config) ApplyEnv(prefix string) error {
	if prefix == "" {
		prefix = defaultEnvPrefix
	}
	env := func(suffix string) string { return prefix + "_" + suffix }

	if v := os.Getenv(env("SERVICE_NAME")); v != "" {
		c.Service.Name = v
	}
	if v := os.Getenv(env("ENV")); v != "" {
		c.Service.Env = v
	}
	if v := os.Getenv(env("LOG_LEVEL")); v != "" {
		c.Log.Level = v
	}
	if v := os.Getenv(env("LOG_FORMAT")); v != "" {
		c.Log.Format = v
	}
	if v := os.Getenv(env("AUTH_API_KEYS")); v != "" {
		c.Auth.Enabled = true
		c.Auth.APIKeys = nil
		for _, key := range strings.Split(v, ",") {
			if key = strings.TrimSpace(key); key != "" {
				c.Auth.APIKeys = append(c.Auth.APIKeys, key)
			}
		}
	}
	if v := os.Getenv(env("OTLP_ENDPOINT")); v != "" {
		c.Telemetry.Enabled = true
		c.Telemetry.OTLPEndpoint = v
	}
	if v := os.Getenv(env("REDIS_ADDR")); v != "" {
		c.Storage.Redis.Enabled = true
		c.Storage.Redis.Addr = v
	}
	if v := os.Getenv(env("POSTGRES_DSN")); v != "" {
		c.Storage.Postgres.Enabled = true
		c.Storage.Postgres.DSN = v
	}
	for _, item := range []struct {
		suffix string
		host   *string
		port   *int
	}{
		{"HTTP_ADDR", &c.HTTP.Host, &c.HTTP.Port},
		{"GRPC_ADDR", &c.GRPC.Host, &c.GRPC.Port},
		{"ADMIN_ADDR", &c.Admin.Host, &c.Admin.Port},
	} {
		v := os.Getenv(env(item.suffix))
		if v == "" {
			continue
		}
		host, portStr, err := net.SplitHostPort(v)
		if err != nil {
			return fmt.Errorf("%s=%q: %w", env(item.suffix), v, err)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return fmt.Errorf("%s=%q: invalid port: %w", env(item.suffix), v, err)
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
	if c.Log.Sampling.Enabled {
		if c.Log.Sampling.Initial < 0 {
			return fmt.Errorf("log.sampling.initial must not be negative")
		}
		if c.Log.Sampling.Thereafter < 1 {
			return fmt.Errorf("log.sampling.thereafter must be >= 1")
		}
		if c.Log.Sampling.Tick <= 0 {
			return fmt.Errorf("log.sampling.tick must be positive")
		}
	}
	if err := c.Log.Access.Validate(); err != nil {
		return err
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
		if c.Telemetry.BatchTimeout < 0 {
			return fmt.Errorf("telemetry.batchTimeout must not be negative")
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

	if err := c.Storage.Redis.Validate(); err != nil {
		return err
	}
	if err := c.Storage.Postgres.Validate(); err != nil {
		return err
	}
	return nil
}

// Validate checks the access log section.
func (c AccessLogConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if _, err := slogx.ParseLevel(c.Level); err != nil {
		return fmt.Errorf("log.access.level: %w", err)
	}
	if _, err := slogx.NormalizeFormat(c.Format); err != nil {
		return fmt.Errorf("log.access.format: %w", err)
	}
	switch strings.ToLower(strings.TrimSpace(c.Output)) {
	case "", "stdout", "file", "both":
	default:
		return fmt.Errorf("log.access.output must be one of stdout|file|both, got %q", c.Output)
	}
	switch strings.ToLower(strings.TrimSpace(c.Color)) {
	case "", "auto", "always", "never":
	default:
		return fmt.Errorf("log.access.color must be one of auto|always|never, got %q", c.Color)
	}
	if strings.EqualFold(c.Output, "file") || strings.EqualFold(c.Output, "both") {
		if strings.TrimSpace(c.File.Path) == "" {
			return fmt.Errorf("log.access.file.path must be set when log.access.output is %q", c.Output)
		}
	}
	return nil
}

// Validate checks the Redis section.
func (c RedisConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	switch strings.ToLower(strings.TrimSpace(c.Mode)) {
	case "", "single":
		if strings.TrimSpace(c.Addr) == "" {
			return fmt.Errorf("storage.redis.addr must be set in single mode")
		}
	case "cluster":
		if len(c.Addrs) == 0 {
			return fmt.Errorf("storage.redis.addrs must list seed nodes in cluster mode")
		}
	case "sentinel":
		if len(c.Addrs) == 0 {
			return fmt.Errorf("storage.redis.addrs must list sentinel nodes in sentinel mode")
		}
		if strings.TrimSpace(c.MasterName) == "" {
			return fmt.Errorf("storage.redis.masterName must be set in sentinel mode")
		}
	default:
		return fmt.Errorf("storage.redis.mode must be one of single|cluster|sentinel, got %q", c.Mode)
	}
	if c.BucketTTL <= 0 {
		return fmt.Errorf("storage.redis.bucketTtl must be positive")
	}
	if c.QueueSize <= 0 {
		return fmt.Errorf("storage.redis.queueSize must be > 0")
	}
	if c.PoolSize < 0 {
		return fmt.Errorf("storage.redis.poolSize must not be negative")
	}
	return nil
}

// Validate checks the PostgreSQL section.
func (c PostgresConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if strings.TrimSpace(c.DSN) == "" {
		return fmt.Errorf("storage.postgres.dsn must be set when postgres is enabled")
	}
	if c.MaxOpenConns < 0 || c.MaxIdleConns < 0 {
		return fmt.Errorf("storage.postgres maxOpenConns/maxIdleConns must not be negative")
	}
	if c.MaxIdleConns > c.MaxOpenConns && c.MaxOpenConns > 0 {
		return fmt.Errorf("storage.postgres.maxIdleConns must not exceed maxOpenConns")
	}
	if c.PingTimeout <= 0 {
		return fmt.Errorf("storage.postgres.pingTimeout must be positive")
	}
	return nil
}

// Redacted returns a shallow copy that is safe to expose over an API or to log:
// secrets are replaced with "***".
func (c *Config) Redacted() *Config {
	clone := *c

	clone.Auth.APIKeys = make([]string, len(c.Auth.APIKeys))
	for i := range clone.Auth.APIKeys {
		clone.Auth.APIKeys[i] = "***"
	}
	if clone.Auth.JWT.Secret != "" {
		clone.Auth.JWT.Secret = "***"
	}
	if clone.Storage.Redis.Password != "" {
		clone.Storage.Redis.Password = "***"
	}
	if clone.Storage.Postgres.DSN != "" {
		clone.Storage.Postgres.DSN = "***"
	}
	return &clone
}

package gosvc

import "example.com/gosvc/config"

// Convenience aliases so applications can build their configuration with a
// single import. They are aliases, not copies: config.Config remains the single
// source of truth.
type (
	// Config is the runtime configuration (http/grpc/tcp/admin/log/auth/...).
	Config = config.Config
	// ServiceConfig identifies the service.
	ServiceConfig = config.ServiceConfig
	// HTTPConfig configures the REST/JSON-RPC server.
	HTTPConfig = config.HTTPConfig
	// GRPCConfig configures the gRPC server.
	GRPCConfig = config.GRPCConfig
	// TCPConfig configures the netpoll based TCP transport.
	TCPConfig = config.TCPConfig
	// AdminConfig configures the operations server.
	AdminConfig = config.AdminConfig
	// LogConfig configures logging.
	LogConfig = config.LogConfig
	// FileLogConfig configures the rotating file sink.
	FileLogConfig = config.FileLogConfig
	// SamplingLogConfig configures log sampling.
	SamplingLogConfig = config.SamplingLogConfig
	// LimiterConfig configures rate limiting.
	LimiterConfig = config.LimiterConfig
	// AuthConfig configures API key and JWT authentication.
	AuthConfig = config.AuthConfig
	// JWTConfig configures HS256 JWT verification.
	JWTConfig = config.JWTConfig
	// TelemetryConfig configures OpenTelemetry tracing.
	TelemetryConfig = config.TelemetryConfig
	// StorageConfig configures optional persistence.
	StorageConfig = config.StorageConfig
	// RedisConfig configures the Redis time-series store.
	RedisConfig = config.RedisConfig
	// Duration is a JSON friendly time.Duration.
	Duration = config.Duration
	// Source describes where a configuration comes from; Source.Load is
	// generic over the application config type.
	Source = config.Source
	// Configurable is implemented by application configs that embed Config.
	Configurable = config.Configurable
)

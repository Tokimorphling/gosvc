package gosvc

import "github.com/Tokimorphling/gosvc/config"

// Convenience aliases so applications can build their configuration with a
// single import. They are aliases, not copies: config.Config remains the
// single source of truth, and the remaining config types stay importable from
// the config package itself.
type (
	// Config is the runtime configuration (http/grpc/tcp/admin/log/auth/...).
	Config = config.Config
	// Source describes where a configuration comes from; Source.Load is
	// generic over the application config type.
	Source = config.Source
	// Configurable is implemented by application configs that embed Config.
	Configurable = config.Configurable
)

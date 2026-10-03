package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
)

func TestDefaultIsValid(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatalf("default config must be valid: %v", err)
	}
}

func TestLoadFileMergesDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	content := `
[service]
name = "svc"
env = "prod"

[http]
port = 1234
readTimeout = "3s"

[log]
level = "debug"
format = "text"
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := (Source{Path: path}).Load[Config]()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Service.Name != "svc" || cfg.Service.Env != "prod" {
		t.Fatalf("service = %+v", cfg.Service)
	}
	if cfg.HTTP.Port != 1234 {
		t.Fatalf("http.port = %d, want 1234", cfg.HTTP.Port)
	}
	if cfg.HTTP.ReadTimeout.D() != 3*time.Second {
		t.Fatalf("http.readTimeout = %s, want 3s", cfg.HTTP.ReadTimeout)
	}
	if cfg.HTTP.IdleTimeout.D() != 60*time.Second {
		t.Fatalf("untouched defaults must survive, got %s", cfg.HTTP.IdleTimeout)
	}
	if cfg.Log.Level != "debug" || cfg.Log.Format != "text" {
		t.Fatalf("log = %+v", cfg.Log)
	}
}

func TestStrictRejectsUnknownKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	content := `
[service]
name = "svc"
env = "dev"

[typo]
enabled = true
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := (Source{Path: path, Strict: true}).Load[Config](); err == nil {
		t.Fatal("strict mode must reject unknown keys")
	} else if !strings.Contains(err.Error(), "typo") {
		t.Fatalf("error should name the unknown key, got %v", err)
	}

	if _, err := (Source{Path: path}).Load[Config](); err != nil {
		t.Fatalf("non-strict mode must ignore unknown keys: %v", err)
	}
}

func TestStrictRuntimeRejectsRuntimeTyposButAllowsAppSections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	content := "[auth]\nenabld = true\n[greeting]\nprefix = \"hi\"\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (Source{Path: path, StrictRuntime: true}).Load[Config](); err == nil || !strings.Contains(err.Error(), "auth.enabld") {
		t.Fatalf("runtime typo should be rejected, got %v", err)
	}
	content = "[auth]\nenabled = true\napiKeys = [\"key\"]\n[greeting]\nprefix = \"hi\"\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, meta, err := (Source{Path: path, StrictRuntime: true}).LoadWithMetadata[Config]()
	if err != nil || !cfg.Auth.Enabled || !meta.IsDefined("auth", "enabled") {
		t.Fatalf("valid runtime and app sections should load with key metadata: cfg=%+v err=%v", cfg, err)
	}
}

func TestEnvOverrides(t *testing.T) {
	t.Setenv("GOSVC_SERVICE_NAME", "from-env")
	t.Setenv("GOSVC_HTTP_ADDR", "127.0.0.1:9999")
	t.Setenv("GOSVC_LOG_LEVEL", "warn")

	cfg, err := (Source{}).Load[Config]()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Service.Name != "from-env" {
		t.Fatalf("service.name = %q", cfg.Service.Name)
	}
	if cfg.HTTP.Host != "127.0.0.1" || cfg.HTTP.Port != 9999 {
		t.Fatalf("http addr = %s", cfg.HTTP.Addr())
	}
	if cfg.Log.Level != "warn" {
		t.Fatalf("log.level = %q", cfg.Log.Level)
	}
}

func TestEnvPrefix(t *testing.T) {
	t.Setenv("MYAPP_SERVICE_NAME", "prefixed")
	t.Setenv("MYAPP_HTTP_ADDR", "127.0.0.1:7777")
	t.Setenv("GOSVC_SERVICE_NAME", "ignored")

	cfg, err := (Source{EnvPrefix: "MYAPP"}).Load[Config]()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Service.Name != "prefixed" {
		t.Fatalf("service.name = %q, want prefixed", cfg.Service.Name)
	}
	if cfg.HTTP.Port != 7777 {
		t.Fatalf("http.port = %d, want 7777", cfg.HTTP.Port)
	}
}

func TestInvalidEnvAddr(t *testing.T) {
	t.Setenv("GOSVC_HTTP_ADDR", "not-an-address")
	if _, err := (Source{}).Load[Config](); err == nil {
		t.Fatal("expected an error for a malformed GOSVC_HTTP_ADDR")
	}
}

func TestEnvTCPAddr(t *testing.T) {
	t.Setenv("GOSVC_TCP_ADDR", "10.0.0.1:7071")
	cfg, err := (Source{}).Load[Config]()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.TCP.Host != "10.0.0.1" || cfg.TCP.Port != 7071 {
		t.Fatalf("tcp addr = %s, want 10.0.0.1:7071", cfg.TCP.Addr())
	}
}

func TestTLSConfigValidation(t *testing.T) {
	complete := TLSConfig{CertFile: "cert.pem", KeyFile: "key.pem"}
	if err := complete.Validate(); err != nil {
		t.Fatalf("complete pair must validate: %v", err)
	}
	if !complete.Enabled() {
		t.Fatal("complete pair must report Enabled")
	}

	for name, cfg := range map[string]TLSConfig{
		"missing key":  {CertFile: "cert.pem"},
		"missing cert": {KeyFile: "key.pem"},
	} {
		if err := cfg.Validate(); err == nil {
			t.Fatalf("%s must fail validation", name)
		}
	}

	// The pair must be rejected wherever it appears.
	broken := Default()
	broken.HTTP.TLS = TLSConfig{CertFile: "cert.pem"}
	if err := broken.Validate(); err == nil {
		t.Fatal("http.tls with only a cert must fail validation")
	}
	broken = Default()
	broken.GRPC.TLS = TLSConfig{KeyFile: "key.pem"}
	if err := broken.Validate(); err == nil {
		t.Fatal("grpc.tls with only a key must fail validation")
	}
}

func TestValidateRejectsBadConfig(t *testing.T) {
	cases := map[string]func(*Config){
		"empty name":    func(c *Config) { c.Service.Name = "" },
		"bad env":       func(c *Config) { c.Service.Env = "production" },
		"bad port":      func(c *Config) { c.HTTP.Port = 70000 },
		"bad level":     func(c *Config) { c.Log.Level = "verbose" },
		"bad format":    func(c *Config) { c.Log.Format = "xml" },
		"limiter burst": func(c *Config) { c.Limiter.RPS = 10; c.Limiter.Burst = 0 },
		"zero body":     func(c *Config) { c.HTTP.MaxBodyBytes = 0 },
		"sampling": func(c *Config) {
			c.Log.Sampling.Enabled = true
			c.Log.Sampling.Thereafter = 0
		},
		"sampling tick": func(c *Config) {
			c.Log.Sampling.Enabled = true
			c.Log.Sampling.Tick = 0
		},
		"negative handler timeout": func(c *Config) { c.HTTP.HandlerTimeout = Duration(-1) },
		"negative cors max age":    func(c *Config) { c.HTTP.CORS.MaxAge = Duration(-1) },
		"incomplete http tls":      func(c *Config) { c.HTTP.TLS = TLSConfig{CertFile: "cert.pem"} },
		"incomplete grpc tls":      func(c *Config) { c.GRPC.TLS = TLSConfig{KeyFile: "key.pem"} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := Default()
			mutate(cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatalf("expected a validation error for %s", name)
			}
		})
	}
}

func TestRedisValidation(t *testing.T) {
	cases := map[string]func(*RedisConfig){
		"single without addr":   func(c *RedisConfig) { c.Mode = "single"; c.Addr = "" },
		"cluster without addrs": func(c *RedisConfig) { c.Mode = "cluster" },
		"sentinel without master": func(c *RedisConfig) {
			c.Mode = "sentinel"
			c.Addrs = []string{"127.0.0.1:26379"}
		},
		"unknown mode": func(c *RedisConfig) { c.Mode = "ring" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			redis := Default().Storage.Redis
			redis.Enabled = true
			mutate(&redis)
			if err := redis.Validate(); err == nil {
				t.Fatalf("expected a validation error for %s", name)
			}
		})
	}

	valid := Default().Storage.Redis
	valid.Enabled = true
	if err := valid.Validate(); err != nil {
		t.Fatalf("default redis config must be valid: %v", err)
	}
}

func TestPostgresValidation(t *testing.T) {
	cases := map[string]func(*PostgresConfig){
		"missing dsn": func(c *PostgresConfig) { c.DSN = "" },
		"idle above open": func(c *PostgresConfig) {
			c.DSN = "postgres://localhost/app"
			c.MaxOpenConns = 2
			c.MaxIdleConns = 4
		},
		"zero ping timeout": func(c *PostgresConfig) {
			c.DSN = "postgres://localhost/app"
			c.PingTimeout = 0
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			pg := Default().Storage.Postgres
			pg.Enabled = true
			mutate(&pg)
			if err := pg.Validate(); err == nil {
				t.Fatalf("expected a validation error for %s", name)
			}
		})
	}

	valid := Default().Storage.Postgres
	valid.Enabled = true
	valid.DSN = "postgres://localhost/app"
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid postgres config rejected: %v", err)
	}
}

func TestDurationUnmarshalTOML(t *testing.T) {
	tests := []struct {
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{raw: `d = "5s"`, want: 5 * time.Second},
		{raw: `d = 2`, want: 2 * time.Second},
		{raw: `d = 1.5`, want: 1500 * time.Millisecond},
		{raw: `d = "1h30m"`, want: 90 * time.Minute},
		{raw: `d = "bogus"`, wantErr: true},
	}
	for _, tt := range tests {
		var out struct {
			D Duration `toml:"d"`
		}
		err := toml.Unmarshal([]byte(tt.raw), &out)
		if tt.wantErr {
			if err == nil {
				t.Fatalf("Duration(%s): expected an error", tt.raw)
			}
			continue
		}
		if err != nil {
			t.Fatalf("Duration(%s): %v", tt.raw, err)
		}
		if out.D.D() != tt.want {
			t.Fatalf("Duration(%s) = %s, want %s", tt.raw, out.D, tt.want)
		}
	}
}

func TestRedacted(t *testing.T) {
	cfg := Default()
	cfg.Auth.Enabled = true
	cfg.Auth.APIKeys = []string{"key-1", "key-2"}
	cfg.Auth.JWT.Secret = "0123456789abcdef"
	cfg.Storage.Redis.Password = "redis-secret"
	cfg.Storage.Postgres.DSN = "postgres://user:pass@localhost/app"

	redacted := cfg.Redacted()
	if redacted.Auth.JWT.Secret != "***" || redacted.Storage.Redis.Password != "***" {
		t.Fatalf("secrets not redacted: %+v", redacted.Auth.JWT.Secret)
	}
	if redacted.Storage.Postgres.DSN != "***" {
		t.Fatalf("dsn not redacted: %q", redacted.Storage.Postgres.DSN)
	}
	for _, key := range redacted.Auth.APIKeys {
		if key != "***" {
			t.Fatalf("api key not redacted: %q", key)
		}
	}
	// The original must stay untouched.
	if cfg.Auth.JWT.Secret != "0123456789abcdef" || cfg.Auth.APIKeys[0] != "key-1" {
		t.Fatal("Redacted modified the original config")
	}
}

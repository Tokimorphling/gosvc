package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultIsValid(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatalf("default config must be valid: %v", err)
	}
}

func TestLoadFileMergesDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{
		"service": {"name": "svc", "env": "prod"},
		"http": {"port": 1234, "readTimeout": "3s"},
		"log": {"level": "debug", "format": "text"}
	}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
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

func TestEnvOverrides(t *testing.T) {
	t.Setenv("GOSVC_SERVICE_NAME", "from-env")
	t.Setenv("GOSVC_HTTP_ADDR", "127.0.0.1:9999")
	t.Setenv("GOSVC_LOG_LEVEL", "warn")

	cfg, err := Load("")
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

func TestInvalidEnvAddr(t *testing.T) {
	t.Setenv("GOSVC_HTTP_ADDR", "not-an-address")
	if _, err := Load(""); err == nil {
		t.Fatal("expected an error for a malformed GOSVC_HTTP_ADDR")
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

func TestDurationUnmarshal(t *testing.T) {
	tests := []struct {
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{raw: `"5s"`, want: 5 * time.Second},
		{raw: `2`, want: 2 * time.Second},
		{raw: `null`, want: 0},
		{raw: `"bogus"`, wantErr: true},
		{raw: `{}`, wantErr: true},
	}
	for _, tt := range tests {
		var d Duration
		err := json.Unmarshal([]byte(tt.raw), &d)
		if tt.wantErr {
			if err == nil {
				t.Fatalf("Duration(%s): expected an error", tt.raw)
			}
			continue
		}
		if err != nil {
			t.Fatalf("Duration(%s): %v", tt.raw, err)
		}
		if d.D() != tt.want {
			t.Fatalf("Duration(%s) = %s, want %s", tt.raw, d, tt.want)
		}
	}
}

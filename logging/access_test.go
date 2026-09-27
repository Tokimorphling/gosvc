package logging

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Tokimorphling/gosvc/config"
)

func TestAccessLoggerSeparateSink(t *testing.T) {
	dir := t.TempDir()
	appLog := filepath.Join(dir, "app.log")
	accessLog := filepath.Join(dir, "access.log")

	cfg := config.Default().Log
	cfg.Level = "info"
	cfg.Output = "file"
	cfg.File.Path = appLog
	cfg.Access.Enabled = true
	cfg.Access.Output = "file"
	cfg.Access.File.Path = accessLog

	handle, err := New(cfg, "svc", "dev", "v1")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	handle.Logger().Info("app line")
	access := handle.Access()
	if access == nil {
		t.Fatal("access logger must be enabled")
	}
	access.Info("access line", "route", "/x")

	appRaw := readLogFile(t, appLog)
	accessRaw := readLogFile(t, accessLog)

	if !strings.Contains(appRaw, "app line") || strings.Contains(appRaw, "access line") {
		t.Fatalf("application log must only contain application lines: %s", appRaw)
	}
	if !strings.Contains(accessRaw, "access line") || strings.Contains(accessRaw, "app line") {
		t.Fatalf("access log must only contain access lines: %s", accessRaw)
	}
	if !strings.Contains(accessRaw, `"log_type":"access"`) {
		t.Fatalf("access lines must be tagged with log_type: %s", accessRaw)
	}
}

func TestAccessLoggerDisabled(t *testing.T) {
	cfg := config.Default().Log

	handle, err := New(cfg, "svc", "dev", "v1")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if handle.Access() != nil {
		t.Fatal("access logger must be nil when the access sink is disabled")
	}
}

func TestAccessLoggerReloadSwapsSink(t *testing.T) {
	dir := t.TempDir()
	appLog := filepath.Join(dir, "app.log")
	accessA := filepath.Join(dir, "access-a.log")
	accessB := filepath.Join(dir, "access-b.log")

	cfg := config.Default().Log
	cfg.Output = "file"
	cfg.File.Path = appLog
	cfg.Access.Enabled = true
	cfg.Access.Output = "file"
	cfg.Access.File.Path = accessA

	handle, err := New(cfg, "svc", "dev", "v1")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	handle.Access().Info("first")

	next := cfg
	next.Access.File.Path = accessB
	if err := handle.Reload(next); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	handle.Access().Info("second")

	if raw := readLogFile(t, accessA); !strings.Contains(raw, "first") || strings.Contains(raw, "second") {
		t.Fatalf("old access sink = %s", raw)
	}
	if raw := readLogFile(t, accessB); !strings.Contains(raw, "second") {
		t.Fatalf("new access sink = %s", raw)
	}
}

func readLogFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}

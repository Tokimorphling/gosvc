package logging

import (
	"log/slog"
	"testing"

	"github.com/Tokimorphling/gosvc/config"
)

func TestReloadFailureKeepsPreviousLevel(t *testing.T) {
	cfg := config.Default().Log
	handle, err := New(cfg, "svc", "dev", "v1")
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	previous := handle.Level().Level()
	cfg.Level = "debug"
	cfg.Output = "file"
	cfg.File.Path = ""
	if err := handle.Reload(cfg); err == nil {
		t.Fatal("expected reload failure")
	}
	if got := handle.Level().Level(); got != previous || got != slog.LevelInfo {
		t.Fatalf("level changed on failed reload: %s", got)
	}
}

// Package logging builds the application's slog logger.
package logging

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"example.com/gosvc/internal/config"
)

// New builds a slog logger from config and returns it together with the
// LevelVar so callers can adjust the level at runtime.
func New(cfg config.LogConfig) (*slog.Logger, *slog.LevelVar, error) {
	level := new(slog.LevelVar)
	if err := SetLevel(level, cfg.Level); err != nil {
		return nil, nil, err
	}

	opts := &slog.HandlerOptions{Level: level, AddSource: cfg.AddSource}

	var handler slog.Handler
	switch strings.ToLower(cfg.Format) {
	case "json":
		handler = slog.NewJSONHandler(os.Stdout, opts)
	case "text":
		handler = slog.NewTextHandler(os.Stdout, opts)
	default:
		return nil, nil, fmt.Errorf("unsupported log format %q", cfg.Format)
	}

	return slog.New(handler), level, nil
}

// SetLevel applies a textual level ("debug", "info", "warn", "error").
func SetLevel(level *slog.LevelVar, s string) error {
	switch strings.ToLower(s) {
	case "debug":
		level.Set(slog.LevelDebug)
	case "info":
		level.Set(slog.LevelInfo)
	case "warn":
		level.Set(slog.LevelWarn)
	case "error":
		level.Set(slog.LevelError)
	default:
		return fmt.Errorf("unsupported log level %q", s)
	}
	return nil
}

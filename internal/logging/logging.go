// Package logging builds the application's slog logger on top of the logx
// handlers (terminal/JSON/logfmt) and optionally mirrors records to a rotating
// file.
//
// Recommended production setup: JSON on stdout (collected by the platform) and,
// when running on bare metal, an additional JSON file with rotation.
package logging

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"

	"example.com/gosvc/internal/config"
	"example.com/gosvc/internal/logx"
)

// New builds the logger from config. env selects the default format when
// cfg.Format is "auto" (terminal for dev, JSON otherwise).
func New(cfg config.LogConfig, service, env, serviceVersion string) (*slog.Logger, *slog.LevelVar, error) {
	level := new(slog.LevelVar)
	if err := SetLevel(level, cfg.Level); err != nil {
		return nil, nil, err
	}

	format, err := logx.NormalizeFormat(cfg.Format)
	if err != nil {
		return nil, nil, err
	}
	if format == "auto" {
		if env == "dev" {
			format = "terminal"
		} else {
			format = "json"
		}
	}

	output := strings.ToLower(strings.TrimSpace(cfg.Output))
	if output == "" {
		output = "stdout"
	}

	var handlers []slog.Handler
	switch output {
	case "stdout", "both":
		handler, err := newConsoleHandler(format, cfg, level)
		if err != nil {
			return nil, nil, err
		}
		handlers = append(handlers, handler)
	}
	switch output {
	case "file", "both":
		handler, err := newFileHandler(cfg.File, level)
		if err != nil {
			return nil, nil, err
		}
		handlers = append(handlers, handler)
	}
	switch output {
	case "stdout", "file", "both":
	default:
		return nil, nil, fmt.Errorf("unsupported log output %q", cfg.Output)
	}

	var handler slog.Handler
	if len(handlers) == 1 {
		handler = handlers[0]
	} else {
		handler = newMultiHandler(handlers...)
	}

	logger := slog.New(handler).With(
		slog.String("service", service),
		slog.String("env", env),
		slog.String("version", serviceVersion),
	)
	return logger, level, nil
}

// SetLevel applies a textual level ("debug", "info", "warn", "error", ...).
func SetLevel(level *slog.LevelVar, s string) error {
	parsed, err := logx.ParseLevel(s)
	if err != nil {
		return err
	}
	level.Set(parsed)
	return nil
}

// LevelName returns the current level name.
func LevelName(level *slog.LevelVar) string {
	if level == nil {
		return ""
	}
	return logx.LevelString(level.Level())
}

func newConsoleHandler(format string, cfg config.LogConfig, level *slog.LevelVar) (slog.Handler, error) {
	switch format {
	case "terminal":
		return logx.NewTerminalHandlerWithLevel(os.Stdout, level, colorEnabled(cfg.Color)), nil
	case "json":
		return logx.JSONHandlerWithLevel(os.Stdout, level), nil
	case "logfmt":
		return logx.LogfmtHandlerWithLevel(os.Stdout, level), nil
	default:
		return nil, fmt.Errorf("unsupported log format %q", format)
	}
}

// newFileHandler always writes structured JSON with source information so the
// file stays machine parseable regardless of the console format.
func newFileHandler(cfg config.FileLogConfig, level *slog.LevelVar) (slog.Handler, error) {
	path := strings.TrimSpace(cfg.Path)
	if path == "" {
		return nil, fmt.Errorf("log.file.path must be set when log.output is file or both")
	}

	writer := &lumberjack.Logger{
		Filename:   path,
		MaxSize:    cfg.MaxSizeMB, // megabytes
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxAgeDays, // days
		Compress:   cfg.Compress,
	}
	return slog.NewJSONHandler(writer, &slog.HandlerOptions{Level: level, AddSource: true}), nil
}

func colorEnabled(mode string) bool {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "always":
		return true
	case "never":
		return false
	default:
		info, err := os.Stdout.Stat()
		if err != nil {
			return false
		}
		return info.Mode()&os.ModeCharDevice != 0
	}
}

// Package logging is the assembly layer of the logging stack: it reads
// config.LogConfig, picks the console format (terminal/JSON/logfmt from
// internal/slogx), chooses the sinks (stdout, rotating file, or both) and
// returns a Handle carrying the logger plus its runtime controls.
//
// The split keeps responsibilities clear: internal/slogx decides how one line
// looks, internal/logging decides where lines go, at which level and with which
// fixed fields. Application code only imports this package.
//
// Call slog.SetDefault(handle.Logger()) at startup so that context-scoped
// loggers (logging.FromContext) share the same handler chain, including
// sampling.
//
// Recommended production setup: JSON on stdout (collected by the platform) and,
// when running on bare metal, an additional JSON file with rotation.
package logging

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"

	"gopkg.in/natefinch/lumberjack.v2"

	"example.com/gosvc/internal/config"
	"example.com/gosvc/internal/slogx"
)

// Handle bundles the application logger with its runtime controls (dynamic
// level, sampling stats and hot reload).
type Handle struct {
	logger  *slog.Logger
	level   *slog.LevelVar
	swap    *slogx.SwapHandler
	service string
	env     string
	version string

	mu      sync.RWMutex
	sampler *slogx.SamplingHandler
}

// New builds the logger from config. env selects the default format when
// cfg.Format is "auto" (terminal for dev, JSON otherwise).
func New(cfg config.LogConfig, service, env, serviceVersion string) (*Handle, error) {
	h := &Handle{
		level:   new(slog.LevelVar),
		service: service,
		env:     env,
		version: serviceVersion,
	}
	if err := SetLevel(h.level, cfg.Level); err != nil {
		return nil, err
	}

	handler, sampler, err := buildHandler(cfg, env, h.level, service, serviceVersion)
	if err != nil {
		return nil, err
	}

	h.sampler = sampler
	h.swap = slogx.NewSwapHandler(handler)
	h.logger = slog.New(h.swap)
	return h, nil
}

// Logger returns the root logger. The returned logger survives Reload calls.
func (h *Handle) Logger() *slog.Logger { return h.logger }

// Level exposes the dynamic level for runtime changes.
func (h *Handle) Level() *slog.LevelVar { return h.level }

// SamplingStats returns sampler counters, or nil when sampling is disabled.
func (h *Handle) SamplingStats() *slogx.SamplingStats {
	h.mu.RLock()
	sampler := h.sampler
	h.mu.RUnlock()
	if sampler == nil {
		return nil
	}
	stats := sampler.Stats()
	return &stats
}

// Reload rebuilds the handler chain from cfg. Level, format, sinks, rotation
// and sampling all take effect without restarting the process.
func (h *Handle) Reload(cfg config.LogConfig) error {
	if err := SetLevel(h.level, cfg.Level); err != nil {
		return err
	}

	handler, sampler, err := buildHandler(cfg, h.env, h.level, h.service, h.version)
	if err != nil {
		return err
	}

	h.mu.Lock()
	h.sampler = sampler
	h.mu.Unlock()
	h.swap.Swap(handler)
	return nil
}

// SetLevel applies a textual level ("trace", "debug", "info", "warn", "error").
func SetLevel(level *slog.LevelVar, s string) error {
	parsed, err := slogx.ParseLevel(s)
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
	return slogx.LevelString(level.Level())
}

func buildHandler(cfg config.LogConfig, env string, level *slog.LevelVar, service, version string) (slog.Handler, *slogx.SamplingHandler, error) {
	format, err := slogx.NormalizeFormat(cfg.Format)
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

	// Base attributes are baked into the handler so the root logger can be
	// swapped on reload without losing them.
	handler = handler.WithAttrs([]slog.Attr{
		slog.String("service", service),
		slog.String("env", env),
		slog.String("version", version),
	})

	var sampler *slogx.SamplingHandler
	if cfg.Sampling.Enabled {
		sampler = slogx.NewSamplingHandler(handler, slogx.SamplingOptions{
			Initial:     cfg.Sampling.Initial,
			Thereafter:  cfg.Sampling.Thereafter,
			Tick:        cfg.Sampling.Tick.D(),
			ExemptLevel: slog.LevelWarn,
		})
		handler = sampler
	}
	return handler, sampler, nil
}

func newConsoleHandler(format string, cfg config.LogConfig, level *slog.LevelVar) (slog.Handler, error) {
	switch format {
	case "terminal":
		return slogx.NewTerminalHandlerWithLevel(os.Stdout, level, colorEnabled(cfg.Color)), nil
	case "json":
		return slogx.JSONHandlerWithLevel(os.Stdout, level), nil
	case "logfmt":
		return slogx.LogfmtHandlerWithLevel(os.Stdout, level), nil
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

// Package logging is the assembly layer of the logging stack: it reads
// config.LogConfig, picks the console format (terminal/JSON/logfmt from the
// slogx package), chooses the sinks (stdout, rotating file, or both) and
// returns a Handle carrying the loggers plus their runtime controls.
//
// Two log streams are supported:
//
//   - the application logger (Handle.Logger), used by everything by default;
//   - an optional dedicated access logger (Handle.Access) for request logs, so
//     access logs can go to their own sink, format and level. When it is
//     disabled, access logs share the application logger.
//
// The split keeps responsibilities clear: slogx decides how one line looks,
// logging decides where lines go, at which level and with which fixed fields.
// Application code only imports this package.
//
// Call slog.SetDefault(handle.Logger()) at startup so that context-scoped
// loggers (logging.FromContext) share the same handler chain, including
// sampling.
//
// Recommended production setup: JSON on stdout (collected by the platform) and,
// when running on bare metal, an additional JSON file with rotation.
package logging

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/Tokimorphling/gosvc/config"
	"github.com/Tokimorphling/gosvc/slogx"
)

// Handle bundles the application logger with its runtime controls (dynamic
// level, sampling stats and hot reload).
type Handle struct {
	logger *slog.Logger
	swap   *slogx.SwapHandler

	level   *slog.LevelVar
	service string
	env     string
	version string

	mu           sync.RWMutex
	sampler      *slogx.SamplingHandler
	closers      []io.Closer
	access       *slog.Logger
	accessSwap   *slogx.SwapHandler
	accessLevel  *slog.LevelVar
	accessOnInit bool
}

// New builds the loggers from config. env selects the default format when
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

	handler, sampler, closer, err := buildMainHandler(cfg, env, h.level, service, serviceVersion)
	if err != nil {
		return nil, err
	}
	h.sampler = sampler
	h.closers = append(h.closers, closer)
	h.swap = slogx.NewSwapHandler(handler)
	h.logger = slog.New(h.swap)

	// Whether an access sink exists is fixed at startup; its format, output and
	// level are hot reloadable. Enabling or disabling it requires a restart.
	h.accessOnInit = cfg.Access.Enabled
	if cfg.Access.Enabled {
		accessLevel := new(slog.LevelVar)
		if err := SetLevel(accessLevel, cfg.Access.Level); err != nil {
			closeIfPresent(closer)
			return nil, fmt.Errorf("log.access.level: %w", err)
		}
		accessHandler, accessCloser, err := buildAccessHandler(cfg.Access, accessLevel, service, env, serviceVersion)
		if err != nil {
			closeIfPresent(closer)
			return nil, err
		}
		h.closers = append(h.closers, accessCloser)
		h.accessLevel = accessLevel
		h.accessSwap = slogx.NewSwapHandler(accessHandler)
		h.access = slog.New(h.accessSwap)
	}

	return h, nil
}

// Logger returns the root application logger. The returned logger survives
// Reload calls.
func (h *Handle) Logger() *slog.Logger { return h.logger }

// Access returns the dedicated access logger, or nil when access logs share the
// application logger.
func (h *Handle) Access() *slog.Logger {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.access
}

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

// Reload rebuilds the handler chains from cfg. Level, format, sinks, rotation
// and sampling all take effect without restarting the process. The existence of
// the access sink is not toggled here. File sinks replaced by the reload are
// closed so their file descriptors do not accumulate.
func (h *Handle) Reload(cfg config.LogConfig) error {
	var nextLevel slog.LevelVar
	if err := SetLevel(&nextLevel, cfg.Level); err != nil {
		return err
	}
	var nextAccessLevel slog.LevelVar
	if h.accessOnInit {
		if err := SetLevel(&nextAccessLevel, cfg.Access.Level); err != nil {
			return fmt.Errorf("log.access.level: %w", err)
		}
	}

	handler, sampler, closer, err := buildMainHandler(cfg, h.env, h.level, h.service, h.version)
	if err != nil {
		return err
	}

	var accessHandler slog.Handler
	var accessCloser io.Closer
	if h.accessOnInit {
		accessLevel := h.accessLevel
		if accessLevel == nil {
			accessLevel = h.level
		}
		accessHandler, accessCloser, err = buildAccessHandler(cfg.Access, accessLevel, h.service, h.env, h.version)
		if err != nil {
			closeIfPresent(closer)
			return err
		}
	}

	h.mu.Lock()
	h.level.Set(nextLevel.Level())
	if h.accessLevel != nil {
		h.accessLevel.Set(nextAccessLevel.Level())
	}
	h.sampler = sampler
	prevClosers := h.closers
	h.closers = []io.Closer{closer, accessCloser}
	h.swap.Swap(handler)

	if h.accessOnInit && accessHandler != nil && h.accessSwap != nil {
		h.accessSwap.Swap(accessHandler)
	}
	h.mu.Unlock()

	// Close the file sinks the reload replaced so their file descriptors do
	// not accumulate. Request-scoped loggers derived before the swap may
	// still write a few lines; a closed lumberjack writer reopens its file on
	// demand, so those writes stay correct and their descriptors are bounded
	// by the request lifetime.
	for _, prev := range prevClosers {
		if prev != nil {
			_ = prev.Close()
		}
	}
	return nil
}

// Close releases file sinks owned by this handle. It is safe to call more
// than once; an externally supplied handle remains owned by its caller.
func (h *Handle) Close() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	closers := h.closers
	h.closers = nil
	h.mu.Unlock()
	var errs []error
	for _, closer := range closers {
		if closer != nil {
			errs = append(errs, closer.Close())
		}
	}
	return errors.Join(errs...)
}

func closeIfPresent(closer io.Closer) {
	if closer != nil {
		_ = closer.Close()
	}
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

func baseAttrs(service, env, version string) []slog.Attr {
	return []slog.Attr{
		slog.String("service", service),
		slog.String("env", env),
		slog.String("version", version),
	}
}

func buildMainHandler(cfg config.LogConfig, env string, level *slog.LevelVar, service, version string) (slog.Handler, *slogx.SamplingHandler, io.Closer, error) {
	format, err := slogx.NormalizeFormat(cfg.Format)
	if err != nil {
		return nil, nil, nil, err
	}
	if format == "auto" {
		if env == "dev" {
			format = "terminal"
		} else {
			format = "json"
		}
	}

	handler, closer, err := buildSinks(format, cfg.Output, cfg.Color, cfg.File, level, cfg.AddSource)
	if err != nil {
		return nil, nil, nil, err
	}
	handler = handler.WithAttrs(baseAttrs(service, env, version))

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
	return handler, sampler, closer, nil
}

func buildAccessHandler(cfg config.AccessLogConfig, level *slog.LevelVar, service, env, version string) (slog.Handler, io.Closer, error) {
	format, err := slogx.NormalizeFormat(cfg.Format)
	if err != nil {
		return nil, nil, fmt.Errorf("log.access.format: %w", err)
	}
	if format == "auto" {
		format = "json"
	}

	handler, closer, err := buildSinks(format, cfg.Output, cfg.Color, cfg.File, level, false)
	if err != nil {
		return nil, nil, fmt.Errorf("log.access: %w", err)
	}

	attrs := append(baseAttrs(service, env, version), slog.String("log_type", "access"))
	return handler.WithAttrs(attrs), closer, nil
}

// buildSinks assembles the console and/or rotating file handlers. The returned
// closer owns the rotating file sink, or is nil when writing to stdout only.
func buildSinks(format, output, color string, fileCfg config.FileLogConfig, level *slog.LevelVar, addSource bool) (slog.Handler, io.Closer, error) {
	output = strings.ToLower(strings.TrimSpace(output))
	if output == "" {
		output = "stdout"
	}

	switch output {
	case "stdout":
		handler, err := newConsoleHandler(format, color, level)
		return handler, nil, err
	case "file":
		return newFileHandler(fileCfg, level, addSource)
	case "both":
		console, err := newConsoleHandler(format, color, level)
		if err != nil {
			return nil, nil, err
		}
		file, closer, err := newFileHandler(fileCfg, level, addSource)
		if err != nil {
			return nil, nil, err
		}
		return newMultiHandler(console, file), closer, nil
	default:
		return nil, nil, fmt.Errorf("unsupported log output %q", output)
	}
}

func newConsoleHandler(format, color string, level *slog.LevelVar) (slog.Handler, error) {
	switch format {
	case "terminal":
		return slogx.NewTerminalHandlerWithLevel(os.Stdout, level, colorEnabled(color)), nil
	case "json":
		return slogx.JSONHandlerWithLevel(os.Stdout, level), nil
	case "logfmt":
		return slogx.LogfmtHandlerWithLevel(os.Stdout, level), nil
	default:
		return nil, fmt.Errorf("unsupported log format %q", format)
	}
}

// newFileHandler always writes structured JSON with source information so the
// file stays machine parseable regardless of the console format. The returned
// closer owns the rotating writer.
func newFileHandler(cfg config.FileLogConfig, level *slog.LevelVar, addSource bool) (slog.Handler, io.Closer, error) {
	path := strings.TrimSpace(cfg.Path)
	if path == "" {
		return nil, nil, fmt.Errorf("log file path must be set when the output includes a file")
	}

	writer := &lumberjack.Logger{
		Filename:   path,
		MaxSize:    cfg.MaxSizeMB, // megabytes
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxAgeDays, // days
		Compress:   cfg.Compress,
	}
	return slog.NewJSONHandler(writer, &slog.HandlerOptions{Level: level, AddSource: addSource}), writer, nil
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

// Package slogx is the presentation layer of the logging stack: slog handlers
// and formatters (terminal/JSON/logfmt) ported from the not-only-mining-pool
// project, which itself is a slog port of go-ethereum's logger. It keeps the
// geth-style human readable output: aligned levels, colored severity, caller
// column and padded key=value pairs.
//
// It depends only on the standard library and knows nothing about this
// service's configuration; sink selection, rotation and level wiring live in
// internal/logging. That makes the package safe to copy into other services.
package slogx

import (
	"fmt"
	"log/slog"
	"math"
	"strings"
)

const (
	// levelMaxVerbosity lets every record through.
	levelMaxVerbosity slog.Level = math.MinInt

	// LevelTrace is more verbose than slog's debug level.
	LevelTrace slog.Level = -8

	LevelDebug = slog.LevelDebug
	LevelInfo  = slog.LevelInfo
	LevelWarn  = slog.LevelWarn
	LevelError = slog.LevelError
	// LevelCrit marks fatal conditions. Unlike the original project it never
	// exits the process by itself.
	LevelCrit slog.Level = 12
)

// Attribute keys used by the terminal handler for caller information.
const (
	callerKey = "caller"
	fileKey   = "file"
	lineKey   = "line"
)

// LevelAlignedString returns a 5-character level name for terminal output.
func LevelAlignedString(l slog.Level) string {
	switch l {
	case LevelTrace:
		return "TRACE"
	case slog.LevelDebug:
		return "DEBUG"
	case slog.LevelInfo:
		return "INFO "
	case slog.LevelWarn:
		return "WARN "
	case slog.LevelError:
		return "ERROR"
	case LevelCrit:
		return "CRIT "
	default:
		return "?????"
	}
}

// LevelString returns the lowercase level name.
func LevelString(l slog.Level) string {
	switch l {
	case LevelTrace:
		return "trace"
	case slog.LevelDebug:
		return "debug"
	case slog.LevelInfo:
		return "info"
	case slog.LevelWarn:
		return "warn"
	case slog.LevelError:
		return "error"
	case LevelCrit:
		return "crit"
	default:
		return fmt.Sprintf("level(%d)", int(l))
	}
}

// ParseLevel converts a textual level into a slog.Level.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "trace":
		return LevelTrace, nil
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	case "crit", "critical", "fatal":
		return LevelCrit, nil
	default:
		return 0, fmt.Errorf("unsupported log level %q", s)
	}
}

// NormalizeFormat maps format aliases onto canonical names:
// auto, terminal, json or logfmt.
func NormalizeFormat(s string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "auto":
		return "auto", nil
	case "terminal", "console", "pretty":
		return "terminal", nil
	case "json":
		return "json", nil
	case "logfmt", "text":
		return "logfmt", nil
	default:
		return "", fmt.Errorf("unsupported log format %q", s)
	}
}

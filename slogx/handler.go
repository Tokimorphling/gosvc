package slogx

import (
	"context"
	"io"
	"log/slog"
	"sync"
)

// discardHandler is a no-op handler.
type discardHandler struct{}

// DiscardHandler returns a handler that drops every record.
func DiscardHandler() slog.Handler { return discardHandler{} }

func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) WithGroup(string) slog.Handler             { return discardHandler{} }
func (discardHandler) WithAttrs([]slog.Attr) slog.Handler        { return discardHandler{} }

// TerminalHandler renders records for humans on a terminal:
//
//	[LEVEL] [TIME] caller - message key=value key=value
//
// Attribute values are aligned in columns and levels are colorized.
type TerminalHandler struct {
	mu       *sync.Mutex
	wr       io.Writer
	lvl      slog.Leveler
	useColor bool
	attrs    []slog.Attr
	groups   []string

	// fieldPadding caches the widest value seen per key so columns line up.
	fieldPadding map[string]int

	buf []byte
}

// NewTerminalHandler returns a colored terminal handler at maximum verbosity.
func NewTerminalHandler(wr io.Writer, useColor bool) *TerminalHandler {
	return NewTerminalHandlerWithLevel(wr, levelMaxVerbosity, useColor)
}

// NewTerminalHandlerWithLevel returns a terminal handler filtered by lvl.
// lvl is a slog.Leveler so a *slog.LevelVar enables runtime level changes.
func NewTerminalHandlerWithLevel(wr io.Writer, lvl slog.Leveler, useColor bool) *TerminalHandler {
	return &TerminalHandler{
		mu:           new(sync.Mutex),
		wr:           wr,
		lvl:          lvl,
		useColor:     useColor,
		fieldPadding: make(map[string]int),
	}
}

// Enabled implements slog.Handler.
func (h *TerminalHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.lvl.Level()
}

// Handle implements slog.Handler.
func (h *TerminalHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	buf := h.format(h.buf, r, h.useColor)
	_, _ = h.wr.Write(buf)
	h.buf = buf[:0]
	return nil
}

// WithAttrs implements slog.Handler.
func (h *TerminalHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &TerminalHandler{
		mu:           h.mu,
		wr:           h.wr,
		lvl:          h.lvl,
		useColor:     h.useColor,
		attrs:        append(append([]slog.Attr{}, h.attrs...), attrs...),
		groups:       append([]string{}, h.groups...),
		fieldPadding: make(map[string]int),
	}
}

// WithGroup implements slog.Handler by prefixing attribute keys with the group
// name, which is what users expect from flat terminal output.
func (h *TerminalHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &TerminalHandler{
		mu:           h.mu,
		wr:           h.wr,
		lvl:          h.lvl,
		useColor:     h.useColor,
		attrs:        append([]slog.Attr{}, h.attrs...),
		groups:       append(append([]string{}, h.groups...), name),
		fieldPadding: make(map[string]int),
	}
}

// ResetFieldPadding zeroes the column widths.
func (h *TerminalHandler) ResetFieldPadding() {
	h.mu.Lock()
	h.fieldPadding = make(map[string]int)
	h.mu.Unlock()
}

// JSONHandler prints records as JSON with geth-style short keys.
func JSONHandler(wr io.Writer) slog.Handler {
	return JSONHandlerWithLevel(wr, levelMaxVerbosity)
}

// JSONHandlerWithLevel is JSONHandler filtered by lvl.
func JSONHandlerWithLevel(wr io.Writer, lvl slog.Leveler) slog.Handler {
	return slog.NewJSONHandler(wr, &slog.HandlerOptions{
		ReplaceAttr: builtinReplaceJSON,
		Level:       lvl,
	})
}

// LogfmtHandler prints records in logfmt format.
func LogfmtHandler(wr io.Writer) slog.Handler {
	return LogfmtHandlerWithLevel(wr, levelMaxVerbosity)
}

// LogfmtHandlerWithLevel is LogfmtHandler filtered by lvl.
func LogfmtHandlerWithLevel(wr io.Writer, lvl slog.Leveler) slog.Handler {
	return slog.NewTextHandler(wr, &slog.HandlerOptions{
		ReplaceAttr: builtinReplaceLogfmt,
		Level:       lvl,
	})
}

func builtinReplaceLogfmt(_ []string, attr slog.Attr) slog.Attr {
	return builtinReplace(attr, true)
}

func builtinReplaceJSON(_ []string, attr slog.Attr) slog.Attr {
	return builtinReplace(attr, false)
}

func builtinReplace(attr slog.Attr, logfmt bool) slog.Attr {
	switch attr.Key {
	case slog.TimeKey:
		if attr.Value.Kind() == slog.KindTime {
			tm := attr.Value.Time().UTC()
			if logfmt {
				return slog.String("t", tm.Format(timeFormat))
			}
			return slog.Attr{Key: "t", Value: slog.TimeValue(tm)}
		}
	case slog.LevelKey:
		if l, ok := attr.Value.Any().(slog.Level); ok {
			return slog.String("lvl", LevelString(l))
		}
	}
	return attr
}

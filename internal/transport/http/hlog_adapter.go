package http

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/cloudwego/hertz/pkg/common/hlog"

	"example.com/gosvc/internal/logging"
)

// hlogAdapter routes Hertz internal logs into the application's slog logger so
// every line shares the same format, level and output.
type hlogAdapter struct {
	base  *slog.Logger
	level *slog.LevelVar
}

func newHlogAdapter(base *slog.Logger, level *slog.LevelVar) hlog.FullLogger {
	if base == nil {
		base = slog.Default()
	}
	if level == nil {
		level = new(slog.LevelVar)
	}
	return &hlogAdapter{base: base, level: level}
}

func (a *hlogAdapter) logger(ctx context.Context) *slog.Logger {
	if ctx != nil {
		return logging.FromContext(ctx)
	}
	return a.base
}

// Logger interface.

func (a *hlogAdapter) Trace(v ...any) { a.logger(nil).Debug(fmt.Sprint(v...), "component", "hertz") }
func (a *hlogAdapter) Debug(v ...any) { a.logger(nil).Debug(fmt.Sprint(v...), "component", "hertz") }
func (a *hlogAdapter) Info(v ...any)  { a.logger(nil).Info(fmt.Sprint(v...), "component", "hertz") }
func (a *hlogAdapter) Notice(v ...any) {
	a.logger(nil).Info(fmt.Sprint(v...), "component", "hertz")
}
func (a *hlogAdapter) Warn(v ...any)  { a.logger(nil).Warn(fmt.Sprint(v...), "component", "hertz") }
func (a *hlogAdapter) Error(v ...any) { a.logger(nil).Error(fmt.Sprint(v...), "component", "hertz") }
func (a *hlogAdapter) Fatal(v ...any) { a.logger(nil).Error(fmt.Sprint(v...), "component", "hertz") }

// FormatLogger interface.

func (a *hlogAdapter) Tracef(format string, v ...any) {
	a.logger(nil).Debug(fmt.Sprintf(format, v...), "component", "hertz")
}
func (a *hlogAdapter) Debugf(format string, v ...any) {
	a.logger(nil).Debug(fmt.Sprintf(format, v...), "component", "hertz")
}
func (a *hlogAdapter) Infof(format string, v ...any) {
	a.logger(nil).Info(fmt.Sprintf(format, v...), "component", "hertz")
}
func (a *hlogAdapter) Noticef(format string, v ...any) {
	a.logger(nil).Info(fmt.Sprintf(format, v...), "component", "hertz")
}
func (a *hlogAdapter) Warnf(format string, v ...any) {
	a.logger(nil).Warn(fmt.Sprintf(format, v...), "component", "hertz")
}
func (a *hlogAdapter) Errorf(format string, v ...any) {
	a.logger(nil).Error(fmt.Sprintf(format, v...), "component", "hertz")
}
func (a *hlogAdapter) Fatalf(format string, v ...any) {
	a.logger(nil).Error(fmt.Sprintf(format, v...), "component", "hertz")
}

// CtxLogger interface.

func (a *hlogAdapter) CtxTracef(ctx context.Context, format string, v ...any) {
	a.logger(ctx).Debug(fmt.Sprintf(format, v...), "component", "hertz")
}
func (a *hlogAdapter) CtxDebugf(ctx context.Context, format string, v ...any) {
	a.logger(ctx).Debug(fmt.Sprintf(format, v...), "component", "hertz")
}
func (a *hlogAdapter) CtxInfof(ctx context.Context, format string, v ...any) {
	a.logger(ctx).Info(fmt.Sprintf(format, v...), "component", "hertz")
}
func (a *hlogAdapter) CtxNoticef(ctx context.Context, format string, v ...any) {
	a.logger(ctx).Info(fmt.Sprintf(format, v...), "component", "hertz")
}
func (a *hlogAdapter) CtxWarnf(ctx context.Context, format string, v ...any) {
	a.logger(ctx).Warn(fmt.Sprintf(format, v...), "component", "hertz")
}
func (a *hlogAdapter) CtxErrorf(ctx context.Context, format string, v ...any) {
	a.logger(ctx).Error(fmt.Sprintf(format, v...), "component", "hertz")
}
func (a *hlogAdapter) CtxFatalf(ctx context.Context, format string, v ...any) {
	a.logger(ctx).Error(fmt.Sprintf(format, v...), "component", "hertz")
}

// Control interface.

func (a *hlogAdapter) SetLevel(level hlog.Level) {
	switch level {
	case hlog.LevelTrace, hlog.LevelDebug:
		a.level.Set(slog.LevelDebug)
	case hlog.LevelInfo, hlog.LevelNotice:
		a.level.Set(slog.LevelInfo)
	case hlog.LevelWarn:
		a.level.Set(slog.LevelWarn)
	case hlog.LevelError, hlog.LevelFatal:
		a.level.Set(slog.LevelError)
	}
}

// SetOutput is a no-op: output is owned by the slog handler.
func (a *hlogAdapter) SetOutput(io.Writer) {}

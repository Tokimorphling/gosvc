package slogx

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestTerminalHandlerFormat(t *testing.T) {
	var buf bytes.Buffer
	handler := NewTerminalHandlerWithLevel(&buf, LevelDebug, false)
	logger := slog.New(handler).With("service", "test")

	logger.WithGroup("g").Info("hello", "n", 1)

	out := buf.String()
	for _, want := range []string{"INFO", "hello", "service=test", "g.n=1", "format_test.go:"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output %q does not contain %q", out, want)
		}
	}
}

func TestTerminalHandlerLevelFilter(t *testing.T) {
	var buf bytes.Buffer
	handler := NewTerminalHandlerWithLevel(&buf, LevelWarn, false)
	logger := slog.New(handler)

	logger.Info("hidden")
	if buf.Len() != 0 {
		t.Fatalf("info record must be filtered, got %q", buf.String())
	}

	logger.Warn("shown")
	if !strings.Contains(buf.String(), "shown") {
		t.Fatalf("warn record must be emitted, got %q", buf.String())
	}
}

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"trace": LevelTrace,
		"debug": slog.LevelDebug,
		"INFO":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
		"crit":  LevelCrit,
	}
	for input, want := range cases {
		got, err := ParseLevel(input)
		if err != nil {
			t.Fatalf("ParseLevel(%q): %v", input, err)
		}
		if got != want {
			t.Fatalf("ParseLevel(%q) = %v, want %v", input, got, want)
		}
	}
	if _, err := ParseLevel("verbose"); err == nil {
		t.Fatal("expected an error for an unknown level")
	}
}

func TestNormalizeFormat(t *testing.T) {
	cases := map[string]string{
		"":         "auto",
		"auto":     "auto",
		"terminal": "terminal",
		"console":  "terminal",
		"json":     "json",
		"text":     "logfmt",
		"logfmt":   "logfmt",
	}
	for input, want := range cases {
		got, err := NormalizeFormat(input)
		if err != nil {
			t.Fatalf("NormalizeFormat(%q): %v", input, err)
		}
		if got != want {
			t.Fatalf("NormalizeFormat(%q) = %q, want %q", input, got, want)
		}
	}
	if _, err := NormalizeFormat("xml"); err == nil {
		t.Fatal("expected an error for an unknown format")
	}
}

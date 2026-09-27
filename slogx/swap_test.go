package slogx

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestSwapHandlerSwapsInner(t *testing.T) {
	var first, second bytes.Buffer
	swap := NewSwapHandler(slog.NewTextHandler(&first, nil))
	logger := slog.New(swap)

	logger.Info("one")
	swap.Swap(slog.NewTextHandler(&second, nil))
	logger.Info("two")

	if !strings.Contains(first.String(), "one") || strings.Contains(first.String(), "two") {
		t.Fatalf("first handler output = %q", first.String())
	}
	if !strings.Contains(second.String(), "two") {
		t.Fatalf("second handler output = %q", second.String())
	}
}

func TestSwapHandlerIgnoresNil(t *testing.T) {
	var buf bytes.Buffer
	swap := NewSwapHandler(slog.NewTextHandler(&buf, nil))
	swap.Swap(nil)

	slog.New(swap).Info("still works")
	if !strings.Contains(buf.String(), "still works") {
		t.Fatalf("output = %q", buf.String())
	}
}

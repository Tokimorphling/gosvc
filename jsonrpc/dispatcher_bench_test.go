package jsonrpc

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func BenchmarkDispatcher(b *testing.B) {
	for _, observed := range []bool{false, true} {
		name := "Unobserved"
		if observed {
			name = "Observed"
		}
		b.Run(name, func(b *testing.B) {
			d := NewDispatcher()
			d.Register("echo", func(context.Context, json.RawMessage) (any, error) { return "ok", nil })
			if observed {
				d.SetObserver(func(string, int, time.Duration) {})
			}
			ctx := context.Background()
			if _, err := d.Invoke(ctx, "echo", nil); err != nil {
				b.Fatal(err)
			}
			b.Run("Invoke", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := d.Invoke(ctx, "echo", nil); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("Parallel", func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if _, err := d.Invoke(ctx, "echo", nil); err != nil {
							b.Error(err)
						}
					}
				})
			})
			b.Run("Serve", func(b *testing.B) {
				body := []byte(`{"jsonrpc":"2.0","id":1,"method":"echo"}`)
				b.ReportAllocs()
				for b.Loop() {
					if _, ok := d.Serve(ctx, body); !ok {
						b.Fatal("no response")
					}
				}
			})
		})
	}
}

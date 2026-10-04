package health

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkReadiness(b *testing.B) {
	for _, n := range []int{0, 8} {
		b.Run(fmt.Sprintf("Checks%d", n), func(b *testing.B) {
			r := &Ready{}
			r.Set(true)
			for i := range n {
				r.AddCheck(fmt.Sprint(i), func(context.Context) error { return nil })
			}
			b.ReportAllocs()
			for b.Loop() {
				if ok, _ := r.Check(context.Background()); !ok {
					b.Fatal("not ready")
				}
			}
		})
	}
}

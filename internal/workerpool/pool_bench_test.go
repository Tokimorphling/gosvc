package workerpool

import (
	"fmt"
	"sync"
	"testing"
)

// The worker is parked while each backlog is filled, then drains a full queue.
// ns/task includes admission, scheduling and synchronization, with no business IO.
func BenchmarkPoolBacklog(b *testing.B) {
	for _, size := range []int{64, 1024} {
		for _, serial := range []bool{false, true} {
			b.Run(fmt.Sprintf("Size%d/Serial%t", size, serial), func(b *testing.B) {
				pool := New[*int](1, size)
				defer pool.Stop()
				var wg sync.WaitGroup
				work := wg.Done
				key := new(int)
				b.ReportAllocs()
				for b.Loop() {
					started, release := make(chan struct{}), make(chan struct{})
					if err := pool.Submit(func() { close(started); <-release }); err != nil {
						b.Fatal(err)
					}
					<-started
					wg.Add(size)
					for range size {
						var err error
						if serial {
							err = pool.SubmitSerial(key, work)
						} else {
							err = pool.Submit(work)
						}
						if err != nil {
							close(release)
							b.Fatal(err)
						}
					}
					close(release)
					wg.Wait()
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/task")
			})
		}
	}
}

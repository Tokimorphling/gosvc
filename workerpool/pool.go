// Package workerpool implements a bounded goroutine pool.
//
// It exists so that transports which run on an event loop (netpoll) can hand
// CPU-heavy work to a bounded number of goroutines with explicit backpressure
// instead of spawning one goroutine per request.
package workerpool

import (
	"errors"
	"sync"
)

var (
	// ErrFull is returned when the queue is saturated.
	ErrFull = errors.New("worker pool queue is full")
	// ErrClosed is returned after Stop.
	ErrClosed = errors.New("worker pool is closed")
)

// Pool runs submitted tasks on a fixed number of workers.
type Pool struct {
	tasks   chan func()
	wg      sync.WaitGroup
	mu      sync.RWMutex
	closed  bool
	onPanic func(recovered any)
}

// New starts workers goroutines consuming a queue of queueSize tasks.
func New(workers, queueSize int) *Pool {
	if workers < 1 {
		workers = 1
	}
	if queueSize < 1 {
		queueSize = 1
	}

	p := &Pool{tasks: make(chan func(), queueSize)}
	for i := 0; i < workers; i++ {
		p.wg.Go(func() {
			for task := range p.tasks {
				p.run(task)
			}
		})
	}
	return p
}

// SetPanicHandler installs a callback invoked when a task panics. The worker
// survives panics either way; the default is to swallow them.
func (p *Pool) SetPanicHandler(fn func(recovered any)) {
	p.mu.Lock()
	p.onPanic = fn
	p.mu.Unlock()
}

func (p *Pool) run(task func()) {
	defer func() {
		if r := recover(); r != nil {
			p.mu.RLock()
			handler := p.onPanic
			p.mu.RUnlock()
			if handler != nil {
				handler(r)
			}
		}
	}()
	task()
}

// Submit enqueues a task, returning ErrFull when the queue is saturated or
// ErrClosed after Stop.
func (p *Pool) Submit(task func()) error {
	if task == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrClosed
	}
	select {
	case p.tasks <- task:
		return nil
	default:
		return ErrFull
	}
}

// Stop closes the queue, drains pending tasks and waits for the workers.
func (p *Pool) Stop() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.tasks)
	p.mu.Unlock()
	p.wg.Wait()
}

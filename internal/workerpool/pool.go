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
type Pool[K comparable] struct {
	mu      sync.Mutex
	ready   *sync.Cond
	tasks   scheduler[K]
	wg      sync.WaitGroup
	done    chan struct{}
	closed  bool
	onPanic func(recovered any)
}

// New starts workers goroutines consuming a queue of queueSize tasks.
func New[K comparable](workers, queueSize int) *Pool[K] {
	if workers < 1 {
		workers = 1
	}
	if queueSize < 1 {
		queueSize = 1
	}

	p := &Pool[K]{tasks: newScheduler[K](queueSize), done: make(chan struct{})}
	p.ready = sync.NewCond(&p.mu)
	for range workers {
		p.wg.Go(p.work)
	}
	return p
}

// SetPanicHandler installs a callback invoked when a task panics. The worker
// survives panics either way; the default is to swallow them.
func (p *Pool[K]) SetPanicHandler(fn func(recovered any)) {
	p.mu.Lock()
	p.onPanic = fn
	p.mu.Unlock()
}

func (p *Pool[K]) run(task func()) {
	defer func() {
		if r := recover(); r != nil {
			p.mu.Lock()
			handler := p.onPanic
			p.mu.Unlock()
			if handler != nil {
				handler(r)
			}
		}
	}()
	task()
}

func (p *Pool[K]) work() {
	for {
		p.mu.Lock()
		task, ok := p.tasks.pop()
		for !ok {
			if p.closed && p.tasks.pending == 0 {
				p.mu.Unlock()
				return
			}
			p.ready.Wait()
			task, ok = p.tasks.pop()
		}
		p.mu.Unlock()

		p.run(task.run)
		p.mu.Lock()
		if p.tasks.complete(task) {
			p.ready.Signal()
		}
		if p.closed && p.tasks.pending == 0 {
			p.ready.Broadcast()
		}
		p.mu.Unlock()
	}
}

// Submit enqueues a task, returning ErrFull when the queue is saturated or
// ErrClosed after Stop.
func (p *Pool[K]) Submit(task func()) error {
	var key K
	return p.submit(key, false, task)
}

// SubmitSerial queues task in arrival order for key. While a task with that
// key runs, workers skip its successors and can execute other connections'
// tasks. Keys are checked by the type system; their zero value is valid.
// Pending tasks share the pool's
// normal queue capacity.
func (p *Pool[K]) SubmitSerial(key K, task func()) error {
	return p.submit(key, true, task)
}

func (p *Pool[K]) submit(key K, serial bool, task func()) error {
	if task == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosed
	}
	if !p.tasks.push(task, key, serial) {
		return ErrFull
	}
	if p.tasks.ready.head != 0 {
		p.ready.Signal()
	}
	return nil
}

// Stop closes the queue, drains pending tasks and waits for the workers.
func (p *Pool[K]) Stop() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		<-p.done
		return
	}
	p.closed = true
	p.ready.Broadcast()
	p.mu.Unlock()
	p.wg.Wait()
	close(p.done)
}

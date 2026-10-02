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
	mu       sync.Mutex
	ready    *sync.Cond
	tasks    []queuedTask
	active   map[any]struct{}
	capacity int
	wg       sync.WaitGroup
	done     chan struct{}
	closed   bool
	onPanic  func(recovered any)
}

type queuedTask struct {
	run func()
	key any
}

// New starts workers goroutines consuming a queue of queueSize tasks.
func New(workers, queueSize int) *Pool {
	if workers < 1 {
		workers = 1
	}
	if queueSize < 1 {
		queueSize = 1
	}

	p := &Pool{capacity: queueSize, active: make(map[any]struct{}), done: make(chan struct{})}
	p.ready = sync.NewCond(&p.mu)
	for range workers {
		p.wg.Go(p.work)
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

func (p *Pool) work() {
	for {
		p.mu.Lock()
		var task queuedTask
		for {
			if p.closed && len(p.tasks) == 0 {
				p.mu.Unlock()
				return
			}
			index := -1
			for i, candidate := range p.tasks {
				if candidate.key == nil {
					index = i
					break
				}
				if _, busy := p.active[candidate.key]; !busy {
					index = i
					break
				}
			}
			if index >= 0 {
				task = p.tasks[index]
				copy(p.tasks[index:], p.tasks[index+1:])
				p.tasks[len(p.tasks)-1] = queuedTask{}
				p.tasks = p.tasks[:len(p.tasks)-1]
				if task.key != nil {
					p.active[task.key] = struct{}{}
				}
				break
			}
			p.ready.Wait()
		}
		p.mu.Unlock()

		p.run(task.run)
		if task.key != nil {
			p.mu.Lock()
			delete(p.active, task.key)
			p.ready.Broadcast()
			p.mu.Unlock()
		}
	}
}

// Submit enqueues a task, returning ErrFull when the queue is saturated or
// ErrClosed after Stop.
func (p *Pool) Submit(task func()) error {
	return p.submit(nil, task)
}

// SubmitSerial queues task in arrival order for key. While a task with that
// key runs, workers skip its successors and can execute other connections'
// tasks. key must be comparable and non-nil. Pending tasks share the pool's
// normal queue capacity.
func (p *Pool) SubmitSerial(key any, task func()) error {
	return p.submit(key, task)
}

func (p *Pool) submit(key any, task func()) error {
	if task == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosed
	}
	if len(p.tasks) >= p.capacity {
		return ErrFull
	}
	p.tasks = append(p.tasks, queuedTask{run: task, key: key})
	p.ready.Signal()
	return nil
}

// Stop closes the queue, drains pending tasks and waits for the workers.
func (p *Pool) Stop() {
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

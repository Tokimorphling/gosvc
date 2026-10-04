package jsonrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
)

// Dispatcher routes JSON-RPC calls through an immutable method table. Writes
// publish a new table; requests load one snapshot without taking a registry lock.
// In-flight calls finish against their original snapshot after a reconfiguration.
type Dispatcher struct {
	mu    sync.Mutex // writers only; never held while invoking application code
	table atomic.Pointer[dispatchTable]
}

type dispatchTable struct {
	methods     map[string]*methodEntry
	middlewares []Middleware
	perMethod   map[string][]Middleware
	observer    Observer
	maxBatch    int
}

// Entries can be shared across table versions when their middleware has not
// changed. OnceValue compiles each chain lazily, outside the writer lock, and
// consistently re-panics if an application middleware constructor panics.
type methodEntry struct {
	base    HandlerFunc
	resolve func() HandlerFunc
}

func newMethodEntry(base HandlerFunc, global, local []Middleware) *methodEntry {
	e := &methodEntry{base: base}
	if len(global)+len(local) != 0 {
		e.resolve = sync.OnceValue(func() HandlerFunc {
			handler := base
			for i := len(local) - 1; i >= 0; i-- {
				handler = local[i](handler)
			}
			for i := len(global) - 1; i >= 0; i-- {
				handler = global[i](handler)
			}
			return handler
		})
	}
	return e
}

func (e *methodEntry) handler() HandlerFunc {
	if e == nil {
		return nil
	}
	if e.resolve != nil {
		return e.resolve()
	}
	return e.base
}

var emptyTable = dispatchTable{maxBatch: defaultMaxBatch}

func (d *Dispatcher) load() *dispatchTable {
	if table := d.table.Load(); table != nil {
		return table
	}
	return &emptyTable
}

// NewDispatcher creates an empty dispatcher. Its zero value is also usable.
func NewDispatcher() *Dispatcher { return &Dispatcher{} }

// Register binds a handler and panics on duplicates to catch wiring mistakes.
// TryRegister is the error-returning alternative for runtime registration.
func (d *Dispatcher) Register(method string, handler HandlerFunc) {
	if err := d.TryRegister(method, handler); err != nil {
		panic(err)
	}
}

func (d *Dispatcher) TryRegister(method string, handler HandlerFunc) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	current := d.load()
	if _, exists := current.methods[method]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateMethod, method)
	}
	next := *current
	next.methods = maps.Clone(current.methods)
	if next.methods == nil {
		next.methods = make(map[string]*methodEntry)
	}
	next.methods[method] = newMethodEntry(handler, next.middlewares, next.perMethod[method])
	d.table.Store(&next)
	return nil
}

// RegisterTyped decodes named params as Req and publishes the handler's Resp.
// Type arguments are inferred from func(context.Context, Req) (Resp, error).
func (d *Dispatcher) RegisterTyped[Req, Resp any](method string, handler func(context.Context, Req) (Resp, error)) {
	d.Register(method, func(ctx context.Context, params json.RawMessage) (any, error) {
		var req Req
		if err := DecodeParams(params, &req); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	})
}

// SetObserver replaces the observer for subsequent calls.
func (d *Dispatcher) SetObserver(observer Observer) {
	d.mu.Lock()
	defer d.mu.Unlock()
	next := *d.load()
	next.observer = observer
	d.table.Store(&next)
}

// SetObserverIfAbsent preserves an observer installed by the application.
func (d *Dispatcher) SetObserverIfAbsent(observer Observer) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	next := *d.load()
	if next.observer != nil {
		return false
	}
	next.observer = observer
	d.table.Store(&next)
	return true
}

// SetMaxBatch limits batch size; values <= 0 restore the default.
func (d *Dispatcher) SetMaxBatch(n int) {
	if n <= 0 {
		n = defaultMaxBatch
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	next := *d.load()
	next.maxBatch = n
	d.table.Store(&next)
}

// Use appends global middleware, outermost first. New chains are built lazily
// on their first call; unchanged in-flight calls keep their original chain.
func (d *Dispatcher) Use(mw ...Middleware) {
	if len(mw) == 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	next := *d.load()
	next.middlewares = append(slices.Clone(next.middlewares), mw...)
	next.methods = maps.Clone(next.methods)
	for name, entry := range next.methods {
		next.methods[name] = newMethodEntry(entry.base, next.middlewares, next.perMethod[name])
	}
	d.table.Store(&next)
}

// UseFor adds middleware inside the global chain for one method. Other
// methods retain their compiled chains, including across new registrations.
func (d *Dispatcher) UseFor(method string, mw Middleware) {
	if mw == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	next := *d.load()
	next.perMethod = maps.Clone(next.perMethod)
	if next.perMethod == nil {
		next.perMethod = make(map[string][]Middleware)
	}
	next.perMethod[method] = append(slices.Clone(next.perMethod[method]), mw)
	if entry := next.methods[method]; entry != nil {
		next.methods = maps.Clone(next.methods)
		next.methods[method] = newMethodEntry(entry.base, next.middlewares, next.perMethod[method])
	}
	d.table.Store(&next)
}

// Methods returns an independent, sorted list of registered method names.
func (d *Dispatcher) Methods() []string {
	table := d.load()
	names := make([]string, 0, len(table.methods))
	for name := range table.methods {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

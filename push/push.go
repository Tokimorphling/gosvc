// Package push provides the in-process fan-out layer for server-originated
// events, so applications stop rewriting the same glue: a subscriber
// registry, per-subscriber delivery with backpressure, dead-sink pruning and
// shutdown draining.
//
// The pieces:
//
//   - Sink is one pushable destination, adapted per transport: a TCP
//     Session (*tcp.Session.AsSink), an SSE stream
//     (*httptransport.SSEStream.AsSink), or any gRPC server stream
//     (StreamSink, structurally typed). Done() fires when the destination is
//     gone, Close() terminates it, and Send is the delivery call.
//   - Broker[T] fans events out: Publish never blocks and never misses the
//     other subscribers — every subscription owns a bounded queue drained by
//     its own pump goroutine, so one slow client delays only itself.
//   - SinkFromContext resolves the current transport's sink inside a
//     dispatched handler ("events.subscribe"), when there is one: HTTP /rpc
//     has no push capability and reports none.
//
// Backpressure is uniform: a full subscription queue applies the configured
// policy — Drop (count and continue) or Disconnect (terminate the slow
// subscriber; it reconnects and resubscribes). Delivery and drops are counted
// on gosvc_broker_* metrics when a *observability.Metrics is wired. A sink can
// also return ErrDropped to discard one event without ending its subscription.
//
// Broker[T] is generic over the payload type for compile-time safety at the
// Publish/Subscribe boundary; delivery boxes the payload once per
// subscription.
package push

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/Tokimorphling/gosvc/state"
)

// Sink is one pushable destination.
type Sink interface {
	// Send delivers one event to the destination. It may block briefly (an
	// SSE write under the HTTP write timeout); broker pumps absorb that. A
	// sink may return ErrDropped to discard this event without ending its
	// subscription.
	Send(event string, data any) error
	// Done is closed when the destination is gone: the broker prunes the
	// subscription without the application doing anything.
	Done() <-chan struct{}
	// Close terminates the destination, best-effort; used by the Disconnect
	// policy for slow consumers.
	Close() error
	// ID identifies the sink for logs and error messages.
	ID() string
}

// Observer receives broker metrics without coupling fan-out to a monitoring
// backend. observability.Metrics implements it. Methods must be concurrency
// safe, non-blocking, and must not reenter the broker. Nil disables metrics.
type Observer interface {
	SetBrokerSubscribers(name string, count int)
	ObserveBrokerDelivered(name string)
	ObserveBrokerDropped(name, reason string)
}

// ErrDropped tells the broker that a sink discarded one event but remains
// usable. The broker counts the event as dropped and continues its pump.
var ErrDropped = errors.New("push: event dropped")

// Policy selects what happens when a subscriber's queue is full.
type Policy int

const (
	// Drop discards the event for that subscriber and counts it on
	// gosvc_broker_dropped_total{reason="queue_full"}.
	Drop Policy = iota
	// Disconnect terminates the slow subscriber (Sink.Close) so it can
	// reconnect and resubscribe.
	Disconnect
)

type message[T any] struct {
	event string
	data  T
}

// Broker fans events of type T out to subscribed sinks.
type Broker[T any] struct {
	name string

	mu      sync.RWMutex
	subs    map[*Subscription[T]]struct{}
	count   int
	stopped bool

	queueSize int
	policy    Policy
	metrics   state.Snapshot[Observer]
}

// The option plumbing is declared once and accepted by every Broker[T]
// instantiation.
type brokerConfig struct {
	name      string
	queueSize int
	policy    Policy
	metrics   Observer
}

func newBrokerConfig(name string, opts ...BrokerOption) brokerConfig {
	c := brokerConfig{name: name, queueSize: 256}
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

// BrokerOption customises a Broker. The functional options are declared once
// and accepted by every Broker[T] instantiation.
type BrokerOption func(*brokerConfig)

// WithQueueSize sets the per-subscriber queue bound (default 256).
func WithQueueSize(n int) BrokerOption {
	return func(c *brokerConfig) {
		if n > 0 {
			c.queueSize = n
		}
	}
}

// WithPolicy selects the slow-subscriber policy (default Drop).
func WithPolicy(p Policy) BrokerOption {
	return func(c *brokerConfig) { c.policy = p }
}

// WithMetrics wires the delivery/drop counters.
func WithMetrics(m Observer) BrokerOption {
	return func(c *brokerConfig) { c.metrics = m }
}

// NewBroker creates a fan-out broker for events of type T. name labels its
// metrics.
func NewBroker[T any](name string, opts ...BrokerOption) *Broker[T] {
	c := newBrokerConfig(name, opts...)
	b := &Broker[T]{
		name:      c.name,
		queueSize: c.queueSize,
		policy:    c.policy,
	}
	b.metrics.Store(c.metrics)
	return b
}

// SetMetrics wires the delivery/drop counters after construction, when the
// metrics bundle only becomes available later in startup. It is safe to call
// while events are being delivered and initializes the new subscriber gauge
// to the current count.
func (b *Broker[T]) SetMetrics(m Observer) {
	b.mu.Lock()
	b.metrics.Store(m)
	if m != nil {
		m.SetBrokerSubscribers(b.name, b.count)
	}
	b.mu.Unlock()
}

// Name returns the broker's metric name.
func (b *Broker[T]) Name() string { return b.name }

// Subscribe registers sink and starts its delivery pump. The returned
// subscription is the handle for Unsubscribe; it is also closed
// automatically when the sink dies. After Shutdown, it returns an already
// completed subscription without registering the sink.
func (b *Broker[T]) Subscribe(sink Sink) *Subscription[T] {
	sub := &Subscription[T]{
		broker: b,
		sink:   sink,
		queue:  make(chan message[T], b.queueSize),
		done:   make(chan struct{}),
	}
	b.mu.Lock()
	if b.stopped {
		sub.closeQueue()
		close(sub.done)
		b.mu.Unlock()
		return sub
	}
	if b.subs == nil {
		b.subs = make(map[*Subscription[T]]struct{})
	}
	b.subs[sub] = struct{}{}
	b.count++
	b.reportSubscribers()
	b.mu.Unlock()
	go sub.pump()
	return sub
}

// Publish fans an event out to every live subscriber. It never blocks on a
// subscriber; a full queue applies the policy. The return value counts the
// subscriptions that accepted the event into their queues.
func (b *Broker[T]) Publish(event string, payload T) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.stopped {
		return 0
	}
	accepted := 0
	for sub := range b.subs {
		if sub.closed.Load() {
			continue
		}
		select {
		case sub.queue <- message[T]{event: event, data: payload}:
			accepted++
		default:
			b.overflow(sub)
		}
	}
	return accepted
}

// overflow applies the policy for one full queue. It requires b.mu (read or
// write).
func (b *Broker[T]) overflow(sub *Subscription[T]) {
	b.observeDropped("queue_full")
	if b.policy == Disconnect && sub.closed.CompareAndSwap(false, true) {
		// Closing the sink fires its Done, which stops the pump; the close
		// itself may block, so never wait for it here.
		go func() { _ = sub.sink.Close() }()
	}
}

// Len returns the number of registered subscriptions. During Shutdown it may
// include subscriptions whose pumps are still draining queued events.
func (b *Broker[T]) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.count
}

// Shutdown closes every subscription; each pump drains what is still queued
// and then exits. Publish after Shutdown delivers nothing.
func (b *Broker[T]) Shutdown() {
	b.mu.Lock()
	b.stopped = true
	for sub := range b.subs {
		sub.closeQueue()
	}
	b.mu.Unlock()
}

func (b *Broker[T]) remove(sub *Subscription[T]) {
	b.mu.Lock()
	if _, ok := b.subs[sub]; ok {
		delete(b.subs, sub)
		b.count--
		b.reportSubscribers()
	}
	b.mu.Unlock()
}

func (b *Broker[T]) reportSubscribers() {
	if m := b.metrics.Load(); m != nil {
		m.SetBrokerSubscribers(b.name, b.count)
	}
}

func (b *Broker[T]) observeDelivered() {
	if m := b.metrics.Load(); m != nil {
		m.ObserveBrokerDelivered(b.name)
	}
}
func (b *Broker[T]) observeDropped(reason string) {
	if m := b.metrics.Load(); m != nil {
		m.ObserveBrokerDropped(b.name, reason)
	}
}

// Subscription is the handle of one subscriber. Unsubscribe is idempotent;
// Done is closed after the pump exits (unsubscribed, sink gone or shutdown
// drain finished), or immediately if Subscribe was called after Shutdown.
type Subscription[T any] struct {
	broker *Broker[T]
	sink   Sink
	queue  chan message[T]
	done   chan struct{}
	closed atomic.Bool
}

// Unsubscribe stops the subscription. Queued events are drained (delivered)
// before the pump exits.
func (s *Subscription[T]) Unsubscribe() {
	s.broker.mu.Lock()
	s.closeQueue()
	s.broker.mu.Unlock()
}

// Done is closed once the delivery pump has exited, or immediately when the
// subscription was rejected because the broker was shut down.
func (s *Subscription[T]) Done() <-chan struct{} { return s.done }

// closeQueue marks the subscription closed and closes its queue. It requires
// the broker write lock, which excludes every Publish, so no send can race
// the close.
func (s *Subscription[T]) closeQueue() {
	if s.closed.CompareAndSwap(false, true) {
		close(s.queue)
	}
}

// pump delivers queued events until the queue is closed and drained, the
// sink reports it is done, or a delivery fails with an error other than
// ErrDropped.
func (s *Subscription[T]) pump() {
	defer close(s.done)
	defer s.broker.remove(s)

	for {
		select {
		case msg, ok := <-s.queue:
			// A closed channel yields its buffered messages first, so the
			// shutdown drain happens naturally.
			if !ok {
				return
			}
			if err := s.sink.Send(msg.event, msg.data); err != nil {
				if errors.Is(err, ErrDropped) {
					s.broker.observeDropped("dropped")
					continue
				}
				s.broker.observeDropped("sink_error")
				return
			}
			s.broker.observeDelivered()
		case <-s.sink.Done():
			s.broker.observeDropped("closed")
			return
		}
	}
}

type sinkKey struct{}

// WithSink stores the current transport's push sink in ctx, for handlers
// implementing "subscribe" style methods. Transports with no push
// capability (HTTP /rpc) never set it.
func WithSink(ctx context.Context, sink Sink) context.Context {
	return context.WithValue(ctx, sinkKey{}, sink)
}

// SinkFromContext returns the push sink of the transport serving ctx, if it
// has one. TCP requests always carry one; HTTP /rpc does not.
func SinkFromContext(ctx context.Context) (Sink, bool) {
	if ctx == nil {
		return nil, false
	}
	if sink, ok := ctx.Value(sinkKey{}).(Sink); ok && sink != nil {
		return sink, true
	}
	return nil, false
}

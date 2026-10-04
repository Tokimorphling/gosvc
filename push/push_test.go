package push

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tokimorphling/gosvc/observability"
)

// chanSink records every delivered event without blocking.
type chanSink struct {
	id   string
	send chan string
	done chan struct{}
}

func newChanSink(id string) *chanSink {
	return &chanSink{id: id, send: make(chan string, 64), done: make(chan struct{})}
}

func (s *chanSink) Send(event string, _ any) error {
	s.send <- event
	return nil
}

func (s *chanSink) Done() <-chan struct{} { return s.done }
func (s *chanSink) Close() error {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	return nil
}
func (s *chanSink) ID() string { return s.id }

func (s *chanSink) received() []string {
	var events []string
	for {
		select {
		case event := <-s.send:
			events = append(events, event)
		default:
			return events
		}
	}
}

// droppedEventSink discards one event. A later event must still reach the
// same subscription.
type droppedEventSink struct{ *chanSink }

func (s *droppedEventSink) Send(event string, data any) error {
	if event == "discard" {
		return fmt.Errorf("destination queue full: %w", ErrDropped)
	}
	return s.chanSink.Send(event, data)
}

type countingSink struct {
	delivered atomic.Int64
	done      chan struct{}
	closeOnce sync.Once
}

func (s *countingSink) Send(string, any) error {
	s.delivered.Add(1)
	return nil
}
func (s *countingSink) Done() <-chan struct{} { return s.done }
func (s *countingSink) Close() error {
	s.closeOnce.Do(func() { close(s.done) })
	return nil
}
func (s *countingSink) ID() string { return "counting" }

type testObserver struct {
	subscribers atomic.Int64
	delivered   atomic.Int64
	dropped     atomic.Int64
}

func (o *testObserver) SetBrokerSubscribers(_ string, n int) { o.subscribers.Store(int64(n)) }
func (o *testObserver) ObserveBrokerDelivered(string)        { o.delivered.Add(1) }
func (o *testObserver) ObserveBrokerDropped(string, string)  { o.dropped.Add(1) }

func TestBrokerAcceptsIndependentObserverAndCanDisableIt(t *testing.T) {
	observer := &testObserver{}
	broker := NewBroker[int]("custom", WithMetrics(observer))
	sink := newChanSink("custom")
	deliver := func() {
		t.Helper()
		sub := broker.Subscribe(sink)
		if broker.Publish("event", 42) != 1 {
			t.Fatal("event not accepted")
		}
		sub.Unsubscribe()
		select {
		case <-sub.Done():
		case <-time.After(time.Second):
			t.Fatal("delivery did not drain")
		}
	}
	deliver()
	if observer.delivered.Load() != 1 || observer.subscribers.Load() != 0 {
		t.Fatal("independent observer missed lifecycle events")
	}
	broker.SetMetrics(nil)
	deliver()
	if observer.delivered.Load() != 1 {
		t.Fatal("disabled observer still received events")
	}
	broker.Shutdown()
}

// blockingSink parks in Send until released; used to force queue overflow.
type blockingSink struct {
	id      string
	gate    chan struct{}
	deliver atomic.Int64
	done    chan struct{}
}

func newBlockingSink(id string) *blockingSink {
	return &blockingSink{id: id, gate: make(chan struct{}), done: make(chan struct{})}
}

func (s *blockingSink) Send(event string, _ any) error {
	<-s.gate
	s.deliver.Add(1)
	return nil
}

func (s *blockingSink) release() { close(s.gate) }

func (s *blockingSink) Done() <-chan struct{} { return s.done }
func (s *blockingSink) Close() error {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	return nil
}
func (s *blockingSink) ID() string { return s.id }

func TestBrokerFanOut(t *testing.T) {
	b := NewBroker[struct{}]("test")
	a, c := newChanSink("a"), newChanSink("c")

	b.Subscribe(a)
	b.Subscribe(c)

	if got := b.Publish("tick", struct{}{}); got != 2 {
		t.Fatalf("accepted = %d, want 2", got)
	}
	waitFor(t, func() bool { return len(a.received()) == 1 && len(c.received()) == 1 })

	if b.Len() != 2 {
		t.Fatalf("subscribers = %d, want 2", b.Len())
	}
}

func TestBrokerDropPolicy(t *testing.T) {
	block := newBlockingSink("block")
	b := NewBroker[struct{}]("test", WithQueueSize(1))
	b.Subscribe(block)

	accepted := 0
	for i := 0; i < 10; i++ {
		accepted += b.Publish("tick", struct{}{})
	}
	if accepted == 10 {
		t.Fatal("a blocked sink with a 1-slot queue must force drops")
	}

	// Releasing the sink lets the pump deliver exactly what was accepted —
	// nothing that was dropped ever reappears.
	block.release()
	waitFor(t, func() bool { return block.deliver.Load() == int64(accepted) })
}

func TestBrokerSinkDropKeepsSubscription(t *testing.T) {
	metrics := observability.New("push-test")
	b := NewBroker[struct{}]("test", WithMetrics(metrics))
	sink := &droppedEventSink{newChanSink("drop")}
	sub := b.Subscribe(sink)

	if got := b.Publish("discard", struct{}{}); got != 1 {
		t.Fatalf("accepted = %d, want 1", got)
	}
	if got := b.Publish("keep", struct{}{}); got != 1 {
		t.Fatalf("accepted = %d, want 1", got)
	}
	waitFor(t, func() bool {
		return len(sink.send) == 1 &&
			brokerMetricValue(t, metrics, "gosvc_broker_dropped_total", map[string]string{"broker": "test", "reason": "dropped"}) == 1 &&
			brokerMetricValue(t, metrics, "gosvc_broker_delivered_total", map[string]string{"broker": "test"}) == 1
	})
	if got := sink.received(); len(got) != 1 || got[0] != "keep" {
		t.Fatalf("received = %v, want only the kept event", got)
	}
	select {
	case <-sub.Done():
		t.Fatal("a dropped event must not end the subscription")
	default:
	}
	if b.Len() != 1 {
		t.Fatalf("subscribers = %d, want 1 after a sink drop", b.Len())
	}
	if got := brokerMetricValue(t, metrics, "gosvc_broker_dropped_total", map[string]string{"broker": "test", "reason": "sink_error"}); got != 0 {
		t.Fatalf("sink_error drops = %g, want 0", got)
	}

	sub.Unsubscribe()
	<-sub.Done()
}

func TestBrokerDisconnectPolicy(t *testing.T) {
	block := newBlockingSink("block")
	b := NewBroker[struct{}]("test", WithQueueSize(1), WithPolicy(Disconnect))
	sub := b.Subscribe(block)

	for i := 0; i < 10; i++ {
		b.Publish("tick", struct{}{})
	}
	select {
	case <-block.done:
	case <-time.After(3 * time.Second):
		t.Fatal("the Disconnect policy must close the slow sink")
	}

	block.release() // unblock the in-flight Send so the pump can exit
	select {
	case <-sub.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("the pump must exit after the sink was disconnected")
	}
	if b.Len() != 0 {
		t.Fatalf("subscribers = %d, want the disconnected one pruned", b.Len())
	}
}

func TestBrokerPrunesDeadSinks(t *testing.T) {
	b := NewBroker[struct{}]("test")
	dead := newChanSink("dead")
	sub := b.Subscribe(dead)

	dead.Close() // the connection ends...
	<-sub.Done() // ...the broker notices and prunes
	if b.Len() != 0 {
		t.Fatalf("subscribers = %d, want 0", b.Len())
	}

	alive := newChanSink("alive")
	b.Subscribe(alive)
	if got := b.Publish("tick", struct{}{}); got != 1 {
		t.Fatalf("accepted = %d, want 1", got)
	}
}

func TestBrokerUnsubscribeDrains(t *testing.T) {
	b := NewBroker[struct{}]("test")
	sink := newChanSink("s")
	sub := b.Subscribe(sink)

	b.Publish("tick", struct{}{})
	b.Publish("tock", struct{}{})
	sub.Unsubscribe()

	<-sub.Done()
	// Everything queued before Unsubscribe is delivered (a closed channel
	// yields its buffer first), nothing after.
	if got := len(sink.received()); got != 2 {
		t.Fatalf("received = %d, want the queued events drained", got)
	}
	if got := b.Publish("tick", struct{}{}); got != 0 {
		t.Fatalf("accepted = %d, want 0 after Unsubscribe", got)
	}
	if b.Len() != 0 {
		t.Fatalf("subscribers = %d, want 0", b.Len())
	}
}

func TestBrokerShutdownDrainsAndStops(t *testing.T) {
	b := NewBroker[struct{}]("test")
	sink := newChanSink("s")
	sub := b.Subscribe(sink)

	b.Publish("tick", struct{}{})
	b.Shutdown()

	select {
	case <-sub.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("the pump must exit after Shutdown")
	}
	if got := len(sink.received()); got != 1 {
		t.Fatalf("received = %d, want the queued event drained", got)
	}
	if got := b.Publish("tick", struct{}{}); got != 0 {
		t.Fatalf("accepted = %d, want 0 after Shutdown", got)
	}

	lateSink := newChanSink("late")
	late := b.Subscribe(lateSink)
	select {
	case <-late.Done():
	default:
		t.Fatal("a subscription created after Shutdown must already be done")
	}
	late.Unsubscribe()
	late.Unsubscribe()
	b.Shutdown()
	if got := b.Publish("tick", struct{}{}); got != 0 {
		t.Fatalf("accepted = %d, want 0 after a late Subscribe", got)
	}
	if got := b.Len(); got != 0 {
		t.Fatalf("subscribers = %d, want 0 after Shutdown", got)
	}
	if got := len(lateSink.send); got != 0 {
		t.Fatalf("late sink received %d events, want 0", got)
	}
}

func TestSetMetricsDuringDelivery(t *testing.T) {
	const events = 300
	b := NewBroker[int]("metered", WithQueueSize(events))
	sink := &countingSink{done: make(chan struct{})}
	sub := b.Subscribe(sink)
	first := observability.New("push-test")
	second := observability.New("push-test")
	b.SetMetrics(first)
	if got := brokerMetricValue(t, first, "gosvc_broker_subscribers", map[string]string{"broker": "metered"}); got != 1 {
		t.Fatalf("new subscriber gauge = %g, want 1", got)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < events; i++ {
			if i%2 == 0 {
				b.SetMetrics(second)
			} else {
				b.SetMetrics(first)
			}
		}
	}()
	for i := 0; i < events; i++ {
		if got := b.Publish("tick", i); got != 1 {
			t.Fatalf("accepted = %d, want 1", got)
		}
	}
	wg.Wait()
	waitFor(t, func() bool { return sink.delivered.Load() == events })
	sub.Unsubscribe()
	<-sub.Done()
	b.SetMetrics(second)
	if got := brokerMetricValue(t, second, "gosvc_broker_subscribers", map[string]string{"broker": "metered"}); got != 0 {
		t.Fatalf("subscriber gauge after removal = %g, want 0", got)
	}
}

func TestSinkFromContext(t *testing.T) {
	//nolint:staticcheck // deliberately probing the nil-context path
	if _, ok := SinkFromContext(nil); ok {
		t.Fatal("nil context must not carry a sink")
	}
	if _, ok := SinkFromContext(context.Background()); ok {
		t.Fatal("an unannotated context must not carry a sink")
	}

	sink := newChanSink("s")
	ctx := WithSink(context.Background(), sink)
	got, ok := SinkFromContext(ctx)
	if !ok || got.ID() != "s" {
		t.Fatalf("got %v, %v", got, ok)
	}
}

func TestStreamSinkIsStructural(t *testing.T) {
	var s Sink = StreamSink{Stream: fakeServerStream{}, Ident: "greeter.Watch"}
	if s.ID() != "greeter.Watch" {
		t.Fatalf("ID = %q", s.ID())
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
}

type fakeServerStream struct{}

func (f fakeServerStream) Context() context.Context { return context.Background() }
func (f fakeServerStream) SendMsg(any) error        { return nil }

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func brokerMetricValue(t *testing.T, metrics *observability.Metrics, family string, labels map[string]string) float64 {
	t.Helper()
	families, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, candidate := range families {
		if candidate.GetName() != family {
			continue
		}
		for _, metric := range candidate.Metric {
			matched := true
			for key, want := range labels {
				found := false
				for _, label := range metric.Label {
					if label.GetName() == key && label.GetValue() == want {
						found = true
						break
					}
				}
				if !found {
					matched = false
					break
				}
			}
			if matched {
				if metric.Counter != nil {
					return metric.Counter.GetValue()
				}
				if metric.Gauge != nil {
					return metric.Gauge.GetValue()
				}
				t.Fatalf("metric %s has neither counter nor gauge", family)
			}
		}
	}
	return 0
}

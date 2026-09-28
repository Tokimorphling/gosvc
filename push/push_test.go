package push

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
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

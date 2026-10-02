package tcp

import (
	"strings"
	"testing"
	"time"

	"github.com/Tokimorphling/gosvc/config"
	"github.com/Tokimorphling/gosvc/observability"
	"github.com/Tokimorphling/gosvc/push"
)

// Both public TCP sink paths must treat a full session queue as one dropped
// notification, not as the end of the broker subscription.
func TestBrokerSubscriptionSurvivesTCPNotifyDrop(t *testing.T) {
	for _, tc := range []struct {
		name string
		sink func(*Session, *connState) push.Sink
	}{
		{"session", func(s *Session, _ *connState) push.Sink { return s.AsSink() }},
		{"context", func(_ *Session, state *connState) push.Sink { return connSink{state: state} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metrics := observability.New("tcp-sink-test")
			server := &Server{
				cfg:     config.TCPConfig{NotifyPolicy: "drop"},
				metrics: metrics,
			}
			state := &connState{server: server, remote: "client"}
			session := &Session{
				server: server,
				state:  state,
				queue:  make(chan []byte, 1),
				done:   make(chan struct{}),
			}
			state.session = session
			session.queue <- []byte("already queued")

			broker := push.NewBroker[int]("tcp-sink-test", push.WithMetrics(metrics))
			sub := broker.Subscribe(tc.sink(session, state))
			defer func() {
				sub.Unsubscribe()
				<-sub.Done()
			}()

			if got := broker.Publish("events.tick", 1); got != 1 {
				t.Fatalf("first Publish accepted %d, want 1", got)
			}
			deadline := time.Now().Add(3 * time.Second)
			for brokerDropCount(t, metrics) == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if got := brokerDropCount(t, metrics); got != 1 {
				t.Fatalf("broker dropped count = %v, want 1", got)
			}
			select {
			case <-sub.Done():
				t.Fatal("the dropped notification ended the subscription")
			default:
			}
			if got := broker.Len(); got != 1 {
				t.Fatalf("broker subscribers = %d, want 1", got)
			}

			<-session.queue
			if got := broker.Publish("events.tick", 2); got != 1 {
				t.Fatalf("second Publish accepted %d, want 1", got)
			}
			select {
			case frame := <-session.queue:
				if !strings.Contains(string(frame), `"params":2`) {
					t.Fatalf("queued frame = %q, want the later event", frame)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("later event was not queued after a drop")
			}
		})
	}
}

func brokerDropCount(t *testing.T, metrics *observability.Metrics) float64 {
	t.Helper()
	families, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "gosvc_broker_dropped_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "reason" && label.GetValue() == "dropped" {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

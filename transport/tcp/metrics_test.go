package tcp

import (
	"strings"
	"testing"
	"time"

	"github.com/Tokimorphling/gosvc/health"
	"github.com/Tokimorphling/gosvc/observability"
)

// tcpMetricValue reads one metric family from the given registry. It mirrors
// the helper in the root package tests.
func tcpMetricValue(t *testing.T, m *observability.Metrics, family string, labels map[string]string) float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, candidate := range families {
		if candidate.GetName() != family {
			continue
		}
		for _, metric := range candidate.GetMetric() {
			matched := true
			for key, want := range labels {
				found := false
				for _, label := range metric.GetLabel() {
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
				if metric.GetCounter() != nil {
					return metric.GetCounter().GetValue()
				}
				if metric.GetGauge() != nil {
					return metric.GetGauge().GetValue()
				}
				t.Fatalf("metric %s has neither counter nor gauge", family)
			}
		}
	}
	return 0
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestTCPConnectionGaugeTracksClients(t *testing.T) {
	metrics := observability.New("tcp-test")
	h := startHarness(t, func(o *Options) { o.Metrics = metrics })
	defer h.stop()

	if got := tcpMetricValue(t, metrics, "gosvc_tcp_connections", nil); got != 0 {
		t.Fatalf("initial connections gauge = %g, want 0", got)
	}

	first, _ := dial(t, h.addr)
	defer first.Close()
	waitFor(t, func() bool { return tcpMetricValue(t, metrics, "gosvc_tcp_connections", nil) == 1 })

	second, _ := dial(t, h.addr)
	waitFor(t, func() bool { return tcpMetricValue(t, metrics, "gosvc_tcp_connections", nil) == 2 })

	// Closing a connection decrements the gauge exactly once, even though
	// netpoll may re-run the close-callback chain.
	_ = second.Close()
	waitFor(t, func() bool { return tcpMetricValue(t, metrics, "gosvc_tcp_connections", nil) == 1 })
}

func TestTCPRejectMetrics(t *testing.T) {
	t.Run("not ready", func(t *testing.T) {
		metrics := observability.New("tcp-test")
		// The zero-value Ready reports not ready, which makes the server
		// answer once and close before dispatch.
		h := startHarness(t, func(o *Options) {
			o.Metrics = metrics
			o.Ready = &health.Ready{}
		})
		defer h.stop()

		conn, reader := dial(t, h.addr)
		defer conn.Close()
		// The not-ready path answers once input arrives: without bytes to
		// read the event loop has nothing to reject.
		send(t, conn, `{"jsonrpc":"2.0","method":"echo","id":1}`)
		line, err := reader.ReadString('\n')
		if err != nil || !strings.Contains(line, "service not ready") {
			t.Fatalf("not-ready response = %q, %v", line, err)
		}
		if _, err := reader.ReadByte(); err == nil {
			t.Fatal("not-ready connection was not closed")
		}
		if got := tcpMetricValue(t, metrics, "gosvc_tcp_rejects_total", map[string]string{"reason": "not_ready"}); got != 1 {
			t.Fatalf("not_ready rejects = %g, want 1", got)
		}
	})

	t.Run("frame too large", func(t *testing.T) {
		metrics := observability.New("tcp-test")
		h := startHarness(t, func(o *Options) {
			o.Metrics = metrics
			o.Config.TCP.MaxFrameBytes = 64
		})
		defer h.stop()

		big, bigReader := dial(t, h.addr)
		defer big.Close()
		for _, chunk := range []string{strings.Repeat("x", 32), strings.Repeat("x", 32), "x"} {
			if _, err := big.Write([]byte(chunk)); err != nil {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		line, err := bigReader.ReadString('\n')
		if err != nil || !strings.Contains(line, "frame too large") {
			t.Fatalf("oversize response = %q, %v", line, err)
		}
		if got := tcpMetricValue(t, metrics, "gosvc_tcp_rejects_total", map[string]string{"reason": "too_large"}); got != 1 {
			t.Fatalf("too_large rejects = %g, want 1", got)
		}
	})
}

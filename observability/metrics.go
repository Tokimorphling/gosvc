// Package observability provides Prometheus metrics for all transports.
package observability

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/Tokimorphling/gosvc/jsonrpc"
)

const namespace = "gosvc"

// Metrics owns a private Prometheus registry plus the transport metrics.
type Metrics struct {
	registry *prometheus.Registry

	httpRequests  *prometheus.CounterVec
	httpDuration  *prometheus.HistogramVec
	rpcRequests   *prometheus.CounterVec
	rpcDuration   *prometheus.HistogramVec
	grpcRequests  *prometheus.CounterVec
	grpcDuration  *prometheus.HistogramVec
	notifySent    *prometheus.CounterVec
	notifyDropped *prometheus.CounterVec

	tcpConnections *prometheus.GaugeVec
	tcpRejects     *prometheus.CounterVec

	brokerSubscribers *prometheus.GaugeVec
	brokerDelivered   *prometheus.CounterVec
	brokerDropped     *prometheus.CounterVec
}

// New builds the registry and registers all collectors.
func New(service string) *Metrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	labels := prometheus.Labels{"service": service}

	m := &Metrics{
		registry: registry,
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "http_requests_total",
			Help: "Total number of HTTP requests.", ConstLabels: labels,
		}, []string{"method", "route", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "http_request_duration_seconds",
			Help: "HTTP request latency in seconds.", ConstLabels: labels,
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
		rpcRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "jsonrpc_requests_total",
			Help: "Total number of JSON-RPC requests.", ConstLabels: labels,
		}, []string{"method", "code"}),
		rpcDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "jsonrpc_request_duration_seconds",
			Help: "JSON-RPC request latency in seconds.", ConstLabels: labels,
			Buckets: prometheus.DefBuckets,
		}, []string{"method"}),
		grpcRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "grpc_requests_total",
			Help: "Total number of gRPC requests.", ConstLabels: labels,
		}, []string{"method", "code"}),
		grpcDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "grpc_request_duration_seconds",
			Help: "gRPC request latency in seconds.", ConstLabels: labels,
			Buckets: prometheus.DefBuckets,
		}, []string{"method"}),
		notifySent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "notify_sent_total",
			Help: "Total number of outbound notifications accepted for delivery.", ConstLabels: labels,
		}, []string{"transport", "codec"}),
		notifyDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "notify_dropped_total",
			Help: "Total number of outbound notifications dropped (queue full or session closed).", ConstLabels: labels,
		}, []string{"transport", "codec", "reason"}),
		tcpConnections: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "tcp_connections",
			Help: "Current number of live TCP connections.", ConstLabels: labels,
		}, nil),
		tcpRejects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "tcp_rejects_total",
			Help: "Total number of TCP connections rejected before dispatch (not ready, rate limited, frame too large) plus handler-panic internal errors.", ConstLabels: labels,
		}, []string{"reason"}),
		brokerSubscribers: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "broker_subscribers",
			Help: "Current number of live broker subscribers.", ConstLabels: labels,
		}, []string{"broker"}),
		brokerDelivered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "broker_delivered_total",
			Help: "Total number of broker events handed to a subscriber sink.", ConstLabels: labels,
		}, []string{"broker"}),
		brokerDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "broker_dropped_total",
			Help: "Total number of broker events dropped (queue full, sink drop, sink error or closed).", ConstLabels: labels,
		}, []string{"broker", "reason"}),
	}

	registry.MustRegister(
		m.httpRequests, m.httpDuration,
		m.rpcRequests, m.rpcDuration,
		m.grpcRequests, m.grpcDuration,
		m.notifySent, m.notifyDropped,
		m.tcpConnections, m.tcpRejects,
		m.brokerSubscribers, m.brokerDelivered, m.brokerDropped,
	)
	return m
}

// Registry exposes the private registry for the /metrics endpoint.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// ObserveHTTP records one HTTP request.
func (m *Metrics) ObserveHTTP(method, route string, status int, d time.Duration) {
	m.httpRequests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
	m.httpDuration.WithLabelValues(method, route).Observe(d.Seconds())
}

// ObserveJSONRPC records one JSON-RPC call.
func (m *Metrics) ObserveJSONRPC(method string, code int, d time.Duration) {
	// Invalid and unregistered requests can contain arbitrary client-supplied
	// method names. Keep those values out of Prometheus labels even if this
	// method is called without a Dispatcher observer.
	if code == jsonrpc.CodeParseError || code == jsonrpc.CodeInvalidRequest || code == jsonrpc.CodeMethodNotFound {
		method = jsonrpc.UnknownMethodLabel
	}
	m.rpcRequests.WithLabelValues(method, strconv.Itoa(code)).Inc()
	m.rpcDuration.WithLabelValues(method).Observe(d.Seconds())
}

// ObserveGRPC records one gRPC call.
func (m *Metrics) ObserveGRPC(method, code string, d time.Duration) {
	m.grpcRequests.WithLabelValues(method, code).Inc()
	m.grpcDuration.WithLabelValues(method).Observe(d.Seconds())
}

// ObserveNotifySent records one outbound notification accepted for delivery
// (it is queued for the client; actual delivery is asynchronous). codec
// identifies the wire dialect on TCP transports. A nil Metrics records
// nothing.
func (m *Metrics) ObserveNotifySent(transport, codec string) {
	if m == nil {
		return
	}
	m.notifySent.WithLabelValues(transport, codec).Inc()
}

// ObserveNotifyDropped records one outbound notification that was dropped:
// reason is "queue_full" (slow consumer) or "closed" (session gone) on TCP,
// "write_error" on SSE. codec identifies the wire dialect. A nil Metrics
// records nothing.
func (m *Metrics) ObserveNotifyDropped(transport, codec, reason string) {
	if m == nil {
		return
	}
	m.notifyDropped.WithLabelValues(transport, codec, reason).Inc()
}

// SetTCPConnections sets the gauge of currently live TCP connections. A nil
// Metrics records nothing.
func (m *Metrics) SetTCPConnections(n int) {
	if m == nil {
		return
	}
	m.tcpConnections.WithLabelValues().Set(float64(n))
}

// ObserveTCPReject records one TCP connection rejected before dispatch or one
// internal error answered to a panicking handler: reason is "not_ready",
// "busy", "too_large" or "internal". A nil Metrics records nothing.
func (m *Metrics) ObserveTCPReject(reason string) {
	if m == nil {
		return
	}
	m.tcpRejects.WithLabelValues(reason).Inc()
}

// SetBrokerSubscribers sets the live subscriber gauge of one broker. A nil
// Metrics records nothing.
func (m *Metrics) SetBrokerSubscribers(broker string, n int) {
	if m == nil {
		return
	}
	m.brokerSubscribers.WithLabelValues(broker).Set(float64(n))
}

// ObserveBrokerDelivered records one event handed to a subscriber sink. A
// nil Metrics records nothing.
func (m *Metrics) ObserveBrokerDelivered(broker string) {
	if m == nil {
		return
	}
	m.brokerDelivered.WithLabelValues(broker).Inc()
}

// ObserveBrokerDropped records one dropped broker event. reason is
// "queue_full", "dropped", "sink_error" or "closed". A nil Metrics records
// nothing.
func (m *Metrics) ObserveBrokerDropped(broker, reason string) {
	if m == nil {
		return
	}
	m.brokerDropped.WithLabelValues(broker, reason).Inc()
}

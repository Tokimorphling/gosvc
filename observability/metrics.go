// Package observability provides Prometheus metrics for all transports.
package observability

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
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
	}

	registry.MustRegister(
		m.httpRequests, m.httpDuration,
		m.rpcRequests, m.rpcDuration,
		m.grpcRequests, m.grpcDuration,
		m.notifySent, m.notifyDropped,
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

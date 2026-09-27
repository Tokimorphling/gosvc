package observability

import (
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"
)

// RegisterDBPool registers connection pool gauges for a database/sql pool under
// the given pool name.
func (m *Metrics) RegisterDBPool(name string, db *sql.DB) error {
	if db == nil {
		return nil
	}
	return m.RegisterDBPoolProvider(name, func() *sql.DB { return db })
}

// RegisterDBPoolProvider registers pool metrics for a provider that may return
// a different pool over time, for example after a configuration reload. The
// provider may return nil, in which case the collector reports nothing.
func (m *Metrics) RegisterDBPoolProvider(name string, provider func() *sql.DB) error {
	if m == nil || provider == nil {
		return nil
	}
	return m.registry.Register(newDBPoolCollector(name, provider))
}

// dbPoolCollector reads sql.DBStats on every scrape, so no background polling
// is required.
type dbPoolCollector struct {
	provider func() *sql.DB

	open    *prometheus.Desc
	inUse   *prometheus.Desc
	idle    *prometheus.Desc
	waits   *prometheus.Desc
	maxOpen *prometheus.Desc
}

func newDBPoolCollector(name string, provider func() *sql.DB) *dbPoolCollector {
	labels := prometheus.Labels{"pool": name}
	return &dbPoolCollector{
		provider: provider,
		open:     prometheus.NewDesc(namespace+"_db_pool_open_connections", "Open connections in the pool.", nil, labels),
		inUse:    prometheus.NewDesc(namespace+"_db_pool_in_use_connections", "Connections currently in use.", nil, labels),
		idle:     prometheus.NewDesc(namespace+"_db_pool_idle_connections", "Idle connections in the pool.", nil, labels),
		waits:    prometheus.NewDesc(namespace+"_db_pool_wait_total", "Total waits for a connection.", nil, labels),
		maxOpen:  prometheus.NewDesc(namespace+"_db_pool_max_open_connections", "Configured maximum open connections.", nil, labels),
	}
}

// Describe implements prometheus.Collector.
func (c *dbPoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.open
	ch <- c.inUse
	ch <- c.idle
	ch <- c.waits
	ch <- c.maxOpen
}

// Collect implements prometheus.Collector.
func (c *dbPoolCollector) Collect(ch chan<- prometheus.Metric) {
	db := c.provider()
	if db == nil {
		return
	}
	stats := db.Stats()
	ch <- prometheus.MustNewConstMetric(c.open, prometheus.GaugeValue, float64(stats.OpenConnections))
	ch <- prometheus.MustNewConstMetric(c.inUse, prometheus.GaugeValue, float64(stats.InUse))
	ch <- prometheus.MustNewConstMetric(c.idle, prometheus.GaugeValue, float64(stats.Idle))
	ch <- prometheus.MustNewConstMetric(c.waits, prometheus.CounterValue, float64(stats.WaitCount))
	ch <- prometheus.MustNewConstMetric(c.maxOpen, prometheus.GaugeValue, float64(stats.MaxOpenConnections))
}

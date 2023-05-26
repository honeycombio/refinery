package metrics

import (
	"sync"

	"github.com/honeycombio/refinery/config"
)

// MultiMetrics is a metrics provider that sends metrics to zero or more other
// metrics providers.
//
// For efficency reasons, if there is only one metrics provider specified, that
// provider will be used directly instead of going through MultiMetrics.
//
// It implements and intercepts the Store method since the children don't need
// to know about it, and also records the values that Get returns. Even if there
// are no metrics providers configured, this allows us to use the metrics
// package to store values that can be retrieved later.
type MultiMetrics struct {
	children []Metrics
	// values keeps a map of all the non-histogram metrics and their current
	// value so that we can retrieve them with Get()
	values map[string]float64
	lock   sync.RWMutex
}

func NewMultiMetrics(c config.Config) Metrics {
	m := &MultiMetrics{
		values: make(map[string]float64),
	}
	if c.GetLegacyMetricsConfig().Enabled {
		m.children = append(m.children, &LegacyMetrics{})
	}

	if c.GetPrometheusMetricsConfig().Enabled {
		m.children = append(m.children, &PromMetrics{})
	}

	// if there's only one child, return it directly instead of going through
	// MultiMetrics
	if len(m.children) == 1 {
		return m.children[0]
	}
	return m
}

func (m *MultiMetrics) Register(name string, metricType string) {
	for _, ch := range m.children {
		ch.Register(name, metricType)
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	m.values[name] = 0
}

func (m *MultiMetrics) Increment(name string) { // for counters
	for _, ch := range m.children {
		ch.Increment(name)
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	m.values[name]++
}

func (m *MultiMetrics) Gauge(name string, val interface{}) { // for gauges
	for _, ch := range m.children {
		ch.Gauge(name, val)
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	m.values[name] = ConvertNumeric(val)
}

func (m *MultiMetrics) Count(name string, n interface{}) { // for counters
	for _, ch := range m.children {
		ch.Count(name, n)
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	m.values[name] += ConvertNumeric(n)
}

func (m *MultiMetrics) Histogram(name string, obs interface{}) { // for histogram
	for _, ch := range m.children {
		ch.Histogram(name, obs)
	}
}

func (m *MultiMetrics) Up(name string) { // for updown
	for _, ch := range m.children {
		ch.Up(name)
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	m.values[name]++
}

func (m *MultiMetrics) Down(name string) { // for updown
	for _, ch := range m.children {
		ch.Down(name)
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	m.values[name]--
}

func (m *MultiMetrics) Get(name string) (float64, bool) { // for reading back a counter or a gauge
	m.lock.Lock()
	defer m.lock.Unlock()
	v, ok := m.values[name]
	return v, ok
}

func (m *MultiMetrics) Store(name string, val float64) { // for storing a rarely-changing value not sent as a metric
	m.lock.Lock()
	defer m.lock.Unlock()
	m.values[name] = val
}

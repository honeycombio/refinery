package metrics

import "github.com/honeycombio/refinery/config"

type MultiMetrics struct {
	children map[string]Metrics
}

func NewMultiMetrics(c config.Config) *MultiMetrics {
	m := &MultiMetrics{}
	if cfg, err := c.GetLegacyMetricsConfig(); err == nil {
		if cfg.Enabled {
			m.children["honeycomb"] = &LegacyMetrics{}
		}
	}
	if cfg, err := c.GetPrometheusMetricsConfig(); err == nil {
		if cfg.Enabled {
			m.children["prometheus"] = &PromMetrics{}
		}
	}
	return &MultiMetrics{}
}

func (m *MultiMetrics) Register(name string, metricType string) {
	for _, ch := range m.children {
		ch.Register(name, metricType)
	}
}

func (m *MultiMetrics) Increment(name string) { // for counters
	for _, ch := range m.children {
		ch.Increment(name)
	}
}

func (m *MultiMetrics) Gauge(name string, val interface{}) { // for gauges
	for _, ch := range m.children {
		ch.Gauge(name, val)
	}
}

func (m *MultiMetrics) Count(name string, n interface{}) { // for counters
	for _, ch := range m.children {
		ch.Count(name, n)
	}
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
}

func (m *MultiMetrics) Down(name string) { // for updown
	for _, ch := range m.children {
		ch.Down(name)
	}
}

func (m *MultiMetrics) Get(name string) (float64, bool) { // for reading back a counter or a gauge
	// if there's a honeycomb metrics provider, use it
	if ch, ok := m.children["honeycomb"]; ok {
		return ch.Get(name)
	}
	// otherwise just use the first one we find
	for _, ch := range m.children {
		return ch.Get(name)
	}
	return 0, false
}

func (m *MultiMetrics) Store(name string, val float64) { // for storing a rarely-changing value not sent as a metric
	for _, ch := range m.children {
		ch.Store(name, val)
	}
}

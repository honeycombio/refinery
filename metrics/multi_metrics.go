package metrics

import (
	"sync"

	"github.com/honeycombio/refinery/config"
)

var _ Metrics = (*MultiMetrics)(nil)

// MultiMetrics is a metrics provider that sends metrics to at least one
// underlying metrics provider (StoreMetrics). It can be configured to send
// metrics to multiple providers at once.
//
// It also stores the values saved with Store(), counters, gauges, and updown counters,
// which can then be retrieved with Get(). This is for use with StressRelief and OpAMP agent. It
// does not track histograms, which are reset after each scrape.
type MultiMetrics struct {
	Config      config.Config  `inject:""`
	PromMetrics MetricsBackend `inject:"promMetrics"`
	OTelMetrics MetricsBackend `inject:"otelMetrics"`
	children    []MetricsBackend
	values      map[string]float64
	lock        sync.RWMutex
}

func NewMultiMetrics() *MultiMetrics {
	return &MultiMetrics{
		children: []MetricsBackend{},
		values:   make(map[string]float64),
	}
}

func (m *MultiMetrics) Start() error {
	// I really hate having to do it this way, but
	// the injector can't handle configurable items, so
	// we need to inject everything and then build the
	// array of children conditionally.
	if m.Config.GetPrometheusMetricsConfig().Enabled {
		m.AddChild(m.PromMetrics)
	}

	if m.Config.GetOTelMetricsConfig().Enabled {
		m.AddChild(m.OTelMetrics)
	}

	return nil
}

func (m *MultiMetrics) AddChild(met MetricsBackend) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.children = append(m.children, met)
}

// This is not safe for concurrent use!
func (m *MultiMetrics) Children() []MetricsBackend {
	return m.children
}

func (m *MultiMetrics) Register(metadata Metadata) {
	for _, ch := range m.children {
		ch.Register(metadata)
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	m.values[metadata.Name] = 0
}

func (m *MultiMetrics) Increment(name string) { // for counters
	for _, ch := range m.children {
		ch.Increment(name)
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	m.values[name]++
}

func (m *MultiMetrics) Gauge(name string, val float64) { // for gauges
	for _, ch := range m.children {
		ch.Gauge(name, val)
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	m.values[name] = val
}

func (m *MultiMetrics) Count(name string, n int64) { // for counters
	for _, ch := range m.children {
		ch.Count(name, n)
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	m.values[name] += float64(n)
}

func (m *MultiMetrics) Histogram(name string, obs float64) { // for histogram
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

func (m *MultiMetrics) Get(name string) (float64, bool) { // for reading back a value
	m.lock.RLock()
	defer m.lock.RUnlock()
	val, ok := m.values[name]
	return val, ok
}

func (m *MultiMetrics) Store(name string, val float64) { // for storing a rarely-changing value not sent as a metric
	m.lock.Lock()
	defer m.lock.Unlock()
	m.values[name] = val
}

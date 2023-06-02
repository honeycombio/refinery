package metrics

import "sync"

// MultiMetrics is a metrics provider that sends metrics to at least one
// underlying metrics provider (StoreMetrics). It can be configured to send
// metrics to multiple providers at once.
//
// It also stores the values saved with Store(), gauges, and updown counters,
// which can then be retrieved with Get(). This is for use with StressRelief. It
// does not track histograms or counters, which are reset after each scrape.
type MultiMetrics struct {
	children []Metrics
	values   map[string]float64
	lock     sync.RWMutex
}

func NewMultiMetrics() *MultiMetrics {
	return &MultiMetrics{
		children: []Metrics{},
		values:   make(map[string]float64),
	}
}

// This is not safe for concurrent use!
func (m *MultiMetrics) AddChild(met Metrics) {
	m.children = append(m.children, met)
}

// This is not safe for concurrent use!
func (m *MultiMetrics) Children() []Metrics {
	return m.children
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

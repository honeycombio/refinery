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
	Mlist  *MetricsList `inject:"metricslist"`
	values map[string]float64
	lock   sync.RWMutex
}

func (m *MultiMetrics) Start() {
	m.values = make(map[string]float64)
}

func (m *MultiMetrics) Register(name string, metricType string) {
	for _, ch := range m.Mlist.Children {
		ch.Register(name, metricType)
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	m.values[name] = 0
}

func (m *MultiMetrics) Increment(name string) { // for counters
	for _, ch := range m.Mlist.Children {
		ch.Increment(name)
	}
}

func (m *MultiMetrics) Gauge(name string, val interface{}) { // for gauges
	for _, ch := range m.Mlist.Children {
		ch.Gauge(name, val)
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	m.values[name] = ConvertNumeric(val)
}

func (m *MultiMetrics) Count(name string, n interface{}) { // for counters
	for _, ch := range m.Mlist.Children {
		ch.Count(name, n)
	}
}

func (m *MultiMetrics) Histogram(name string, obs interface{}) { // for histogram
	for _, ch := range m.Mlist.Children {
		ch.Histogram(name, obs)
	}
}

func (m *MultiMetrics) Up(name string) { // for updown
	for _, ch := range m.Mlist.Children {
		ch.Up(name)
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	m.values[name]++
}

func (m *MultiMetrics) Down(name string) { // for updown
	for _, ch := range m.Mlist.Children {
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

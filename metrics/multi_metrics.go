package metrics

// MultiMetrics is a metrics provider that sends metrics to at least one
// underlying metrics provider (StoreMetrics). It can be configured to send
// metrics to multiple providers at once.
// It always starts with a StoreMetrics as the first provider, so that
// Get and Store can depend on it.
type MultiMetrics struct {
	children []Metrics
}

func NewMultiMetrics() *MultiMetrics {
	return &MultiMetrics{
		children: []Metrics{&StoreMetrics{}},
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

func (m *MultiMetrics) Get(name string) (float64, bool) { // for reading back a value
	return m.children[0].Get(name) // this is the StoreMetrics
}

func (m *MultiMetrics) Store(name string, val float64) { // for storing a rarely-changing value not sent as a metric
	m.children[0].Store(name, val) // this is the StoreMetrics
}

package metrics

import (
	"fmt"
	"sync"

	"github.com/facebookgo/startstop"
)

var _ Metrics = &MockMetrics{}

var _ startstop.Starter = &MockMetrics{}
var _ startstop.Stopper = &MockMetrics{}

// MockMetrics collects metrics that were registered and changed to allow tests to
// verify expected behavior
type MockMetrics struct {
	Registrations     map[string]string
	CounterIncrements map[string]int
	GaugeRecords      map[string]float64
	Histograms        map[string][]float64
	UpdownIncrements  map[string]int
	Constants         map[string]float64

	lock sync.Mutex
}

// Start initializes all metrics or resets all metrics to zero
func (m *MockMetrics) Start() error {
	m.Registrations = make(map[string]string)
	m.CounterIncrements = make(map[string]int)
	m.GaugeRecords = make(map[string]float64)
	m.Histograms = make(map[string][]float64)
	m.UpdownIncrements = make(map[string]int)
	m.Constants = make(map[string]float64)

	return nil
}

func (m *MockMetrics) Stop() error {
	return nil
}

func (m *MockMetrics) Register(name string, metricType string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.Registrations[name] = metricType
	switch metricType {
	case "counter":
		m.CounterIncrements[name] = 0
	case "gauge":
		m.GaugeRecords[name] = 0
	case "histogram":
		m.Histograms[name] = make([]float64, 0)
	case "updown":
		m.UpdownIncrements[name] = 0
	case "constant":
		m.Constants[name] = 0
	default:
		panic(fmt.Sprintf("unknown metric type %s", metricType))
	}
}
func (m *MockMetrics) Increment(name string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.CounterIncrements[name] += 1
}
func (m *MockMetrics) Gauge(name string, val interface{}) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.GaugeRecords[name] = ConvertNumeric(val)
}
func (m *MockMetrics) Count(name string, val interface{}) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.CounterIncrements[name] += int(ConvertNumeric(val))
}
func (m *MockMetrics) Histogram(name string, val interface{}) {
	m.lock.Lock()
	defer m.lock.Unlock()

	_, ok := m.Histograms[name]
	if !ok {
		m.Histograms[name] = make([]float64, 0)
	}
	m.Histograms[name] = append(m.Histograms[name], ConvertNumeric(val))
}
func (m *MockMetrics) Up(name string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.CounterIncrements[name]++
}
func (m *MockMetrics) Down(name string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.CounterIncrements[name]--
}

func (m *MockMetrics) Get(name string) (float64, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if v, ok := m.CounterIncrements[name]; ok {
		return float64(v), true
	}
	if v, ok := m.GaugeRecords[name]; ok {
		return v, true
	}
	if v, ok := m.Constants[name]; ok {
		return v, true
	}
	// return the last value in the histogram
	if v, ok := m.Histograms[name]; ok {
		return v[len(v)-1], true
	}
	return 0, false
}

func (m *MockMetrics) Store(name string, val float64) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.Constants[name] = val
}

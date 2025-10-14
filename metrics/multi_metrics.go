package metrics

import (
	"math"
	"sync"
	"sync/atomic"

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
//
// To minimize lock contention, this implementation uses sync.Map for lock-free
// concurrent access to metric values. atomic.Uint64 and atomic.Int64 types are used
// for lock-free concurrent updates.
type MultiMetrics struct {
	Config      config.Config  `inject:""`
	PromMetrics MetricsBackend `inject:"promMetrics"`
	OTelMetrics MetricsBackend `inject:"otelMetrics"`
	children    []MetricsBackend

	// Use sync.Map with atomic types for lock-free concurrent access
	counters sync.Map // map[string]*atomic.Uint64
	gauges   sync.Map // map[string]*atomic.Uint64 (stores float64 bits)
	updowns  sync.Map // map[string]*atomic.Int64 (can be negative)
	stores   sync.Map // map[string]*atomic.Uint64 (stores float64 bits)

	// Track metric types for proper Get() routing
	metricTypes sync.Map // map[string]MetricType
}

func NewMultiMetrics() *MultiMetrics {
	return &MultiMetrics{
		children: []MetricsBackend{},
		// sync.Map doesn't need initialization
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
	// Note: This is only called during initialization (Start method),
	// so we don't need a lock here as there's no concurrent access yet.
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

	// Track the metric type for proper routing in Get()
	m.metricTypes.Store(metadata.Name, metadata.Type)

	// Initialize the value in the appropriate map based on metric type
	switch metadata.Type {
	case Counter:
		m.counters.Store(metadata.Name, &atomic.Uint64{})
	case Gauge:
		// Initialize gauge with zero value (stored as float64 bits)
		val := &atomic.Uint64{}
		val.Store(math.Float64bits(0))
		m.gauges.Store(metadata.Name, val)
	case UpDown:
		m.updowns.Store(metadata.Name, &atomic.Int64{})
	case Histogram:
		// Histograms are not stored, so nothing to do
	}
}

func (m *MultiMetrics) Increment(name string) {
	for _, ch := range m.children {
		ch.Increment(name)
	}
	// Fast path: try Load first (no allocation for registered metrics)
	if val, ok := m.counters.Load(name); ok {
		val.(*atomic.Uint64).Add(1)
		return
	}
	// Slow path: handle unregistered metrics
	val, _ := m.counters.LoadOrStore(name, &atomic.Uint64{})
	val.(*atomic.Uint64).Add(1)
}

func (m *MultiMetrics) Gauge(name string, val float64) {
	for _, ch := range m.children {
		ch.Gauge(name, val)
	}
	bits := math.Float64bits(val)
	// Fast path: try Load first (no allocation for registered metrics)
	if ptr, ok := m.gauges.Load(name); ok {
		ptr.(*atomic.Uint64).Store(bits)
		return
	}
	// Slow path: handle unregistered metrics
	ptr, _ := m.gauges.LoadOrStore(name, &atomic.Uint64{})
	ptr.(*atomic.Uint64).Store(bits)
}

func (m *MultiMetrics) Count(name string, n int64) {
	for _, ch := range m.children {
		ch.Count(name, n)
	}
	// Fast path: try Load first (no allocation for registered metrics)
	if val, ok := m.counters.Load(name); ok {
		val.(*atomic.Uint64).Add(uint64(n))
		return
	}
	// Slow path: handle unregistered metrics
	val, _ := m.counters.LoadOrStore(name, &atomic.Uint64{})
	val.(*atomic.Uint64).Add(uint64(n))
}

func (m *MultiMetrics) Histogram(name string, obs float64) {
	for _, ch := range m.children {
		ch.Histogram(name, obs)
	}
}

func (m *MultiMetrics) Up(name string) {
	for _, ch := range m.children {
		ch.Up(name)
	}
	// Fast path: try Load first (no allocation for registered metrics)
	if val, ok := m.updowns.Load(name); ok {
		val.(*atomic.Int64).Add(1)
		return
	}
	// Slow path: handle unregistered metrics
	val, _ := m.updowns.LoadOrStore(name, &atomic.Int64{})
	val.(*atomic.Int64).Add(1)
}

func (m *MultiMetrics) Down(name string) {
	for _, ch := range m.children {
		ch.Down(name)
	}
	// Fast path: try Load first (no allocation for registered metrics)
	if val, ok := m.updowns.Load(name); ok {
		val.(*atomic.Int64).Add(-1)
		return
	}
	// Slow path: handle unregistered metrics
	val, _ := m.updowns.LoadOrStore(name, &atomic.Int64{})
	val.(*atomic.Int64).Add(-1)
}

func (m *MultiMetrics) Get(name string) (float64, bool) {
	// First check if this is a stored value (not a registered metric)
	if val, ok := m.stores.Load(name); ok {
		bits := val.(*atomic.Uint64).Load()
		return math.Float64frombits(bits), true
	}

	// Then check what type of metric this is
	metricTypeVal, exists := m.metricTypes.Load(name)
	if !exists {
		// This metric was never registered, but might have been used directly
		// Check all maps to find it (for backwards compatibility with unregistered metrics)
		if val, ok := m.counters.Load(name); ok {
			return float64(val.(*atomic.Uint64).Load()), true
		}
		if val, ok := m.gauges.Load(name); ok {
			bits := val.(*atomic.Uint64).Load()
			return math.Float64frombits(bits), true
		}
		if val, ok := m.updowns.Load(name); ok {
			return float64(val.(*atomic.Int64).Load()), true
		}
		return 0, false
	}

	// Look in the appropriate map based on metric type
	switch metricTypeVal.(MetricType) {
	case Counter:
		if val, ok := m.counters.Load(name); ok {
			return float64(val.(*atomic.Uint64).Load()), true
		}
		return 0, false
	case Gauge:
		if val, ok := m.gauges.Load(name); ok {
			bits := val.(*atomic.Uint64).Load()
			return math.Float64frombits(bits), true
		}
		return 0, false
	case UpDown:
		if val, ok := m.updowns.Load(name); ok {
			return float64(val.(*atomic.Int64).Load()), true
		}
		return 0, false
	case Histogram:
		// Histograms are not stored
		return 0, false
	default:
		return 0, false
	}
}

func (m *MultiMetrics) Store(name string, val float64) { // for storing a rarely-changing value not sent as a metric
	bits := math.Float64bits(val)
	// Fast path: try Load first (no allocation for existing stores)
	if ptr, ok := m.stores.Load(name); ok {
		ptr.(*atomic.Uint64).Store(bits)
		return
	}
	// Slow path: first store for this name
	ptr, _ := m.stores.LoadOrStore(name, &atomic.Uint64{})
	ptr.(*atomic.Uint64).Store(bits)
}

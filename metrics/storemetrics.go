package metrics

import "sync"

// StoreMetrics stores the values saved with Store(), gauges, and updown
// counters, which can then be retrieved with Get(). It does not track
// histograms or counters, which are reset after each scrape. This can't know
// about when scraps occur, so we don't do them here.
type StoreMetrics struct {
	// values keeps a map of the metrics and their current value so that we can
	// retrieve them with Get(). Note that for counters, these values will not
	// be reset.
	values map[string]float64
	lock   sync.RWMutex
	M      Metrics `inject:"metrics"`
}

// Start initializes all metrics or resets all metrics to zero
func (s *StoreMetrics) Start() error {
	s.values = make(map[string]float64)
	return nil
}

func (s *StoreMetrics) Register(name string, metricType string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.values[name] = 0
}

func (s *StoreMetrics) Gauge(name string, val interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.values[name] = ConvertNumeric(val)
}

func (s *StoreMetrics) Up(name string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.values[name]++
}

func (s *StoreMetrics) Down(name string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.values[name]--
}

func (s *StoreMetrics) Store(name string, val float64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.values[name] = val
}

func (s *StoreMetrics) Get(name string) (float64, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	val, ok := s.values[name]
	return val, ok
}

// These are no-ops for StoreMetrics
func (s *StoreMetrics) Increment(name string)                  {}
func (s *StoreMetrics) Count(name string, val interface{})     {}
func (s *StoreMetrics) Histogram(name string, obs interface{}) {}

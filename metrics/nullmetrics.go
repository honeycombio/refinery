package metrics

// NullMetrics discards all metrics
type NullMetrics struct{}

// Start initializes all metrics or resets all metrics to zero
func (n *NullMetrics) Start() {}

func (n *NullMetrics) Register(name string, metricType string) {}
func (n *NullMetrics) Increment(name string)                   {}
func (n *NullMetrics) Gauge(name string, val float64)          {}
func (n *NullMetrics) Count(name string, n interface{})        {}
func (n *NullMetrics) Histogram(name string, obs float64)      {}

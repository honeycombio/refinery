package metrics

var _ Metrics = (*NullMetrics)(nil)

// NullMetrics discards all metrics
type NullMetrics struct{}

// Start initializes all metrics or resets all metrics to zero
func (n *NullMetrics) Start() {}

func (n *NullMetrics) Register(metadata Metadata)             {}
func (n *NullMetrics) Increment(name string)                  {}
func (n *NullMetrics) Gauge(name string, val interface{})     {}
func (n *NullMetrics) Count(name string, val interface{})     {}
func (n *NullMetrics) Histogram(name string, obs interface{}) {}
func (n *NullMetrics) Up(name string)                         {}
func (n *NullMetrics) Down(name string)                       {}
func (n *NullMetrics) Store(name string, value float64)       {}
func (n *NullMetrics) Get(name string) (float64, bool)        { return 0, true }

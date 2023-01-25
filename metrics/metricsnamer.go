package metrics

// This wraps a Metrics object and is a Metrics object itself, but adds a prefix
// to all uses of its name. The point is that we can have a singleton Metrics
// object that collects and reports all metrics rather than 3-5 different
// objects with different prefixes. This allows us to query that singleton
// object to get metric information back out.

type MetricsPrefixer struct {
	Metrics Metrics `inject:"metrics"`
	prefix  string
}

func NewMetricsPrefixer(prefix string) *MetricsPrefixer {
	prefixer := &MetricsPrefixer{prefix: prefix}

	if prefix != "" {
		prefixer.prefix = prefix + "_"
	}
	return prefixer
}

func (p *MetricsPrefixer) Start() error {
	return nil
}

func (p *MetricsPrefixer) Register(name string, metricType string) {
	p.Metrics.Register(p.prefix+name, metricType)
}

func (p *MetricsPrefixer) Increment(name string) {
	p.Metrics.Increment(p.prefix + name)
}

func (p *MetricsPrefixer) Gauge(name string, val interface{}) {
	p.Metrics.Gauge(p.prefix+name, val)
}

func (p *MetricsPrefixer) Count(name string, val interface{}) {
	p.Metrics.Count(p.prefix+name, val)
}

func (p *MetricsPrefixer) Histogram(name string, obs interface{}) {
	p.Metrics.Histogram(p.prefix+name, obs)
}

func (p *MetricsPrefixer) Up(name string) {
	p.Metrics.Up(p.prefix + name)
}

func (p *MetricsPrefixer) Down(name string) {
	p.Metrics.Down(p.prefix + name)
}

func (p *MetricsPrefixer) Get(name string) (float64, bool) {
	return p.Metrics.Get(p.prefix + name)
}

func (p *MetricsPrefixer) Store(name string, val float64) {
	p.Metrics.Store(p.prefix+name, val)
}

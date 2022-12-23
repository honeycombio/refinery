package metrics

import (
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
)

type PromMetrics struct {
	Config config.Config `inject:""`
	Logger logger.Logger `inject:""`
	// metrics keeps a record of all the registered metrics so we can increment
	// them by name
	metrics map[string]interface{}
	// values keeps a map of all the non-histogram metrics and their current value
	// so that we can retrieve them with Get()
	values map[string]float64
	lock   sync.RWMutex

	prefix string
}

func (p *PromMetrics) Start() error {
	p.Logger.Debug().Logf("Starting PromMetrics")
	defer func() { p.Logger.Debug().Logf("Finished starting PromMetrics") }()
	pc, err := p.Config.GetPrometheusMetricsConfig()
	if err != nil {
		return err
	}

	p.metrics = make(map[string]interface{})
	p.values = make(map[string]float64)

	muxxer := mux.NewRouter()

	muxxer.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(pc.MetricsListenAddr, muxxer)
	return nil
}

// Register takes a name and a metric type. The type should be one of "counter",
// "gauge", or "histogram"
func (p *PromMetrics) Register(name string, metricType string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	newmet, exists := p.metrics[name]

	// don't attempt to add the metric again as this will cause a panic
	if exists {
		return
	}

	switch metricType {
	case "counter":
		newmet = promauto.NewCounter(prometheus.CounterOpts{
			Name:      name,
			Namespace: p.prefix,
			Help:      name,
		})
	case "gauge", "updown": // updown is a special gauge
		newmet = promauto.NewGauge(prometheus.GaugeOpts{
			Name:      name,
			Namespace: p.prefix,
			Help:      name,
		})
	case "histogram":
		newmet = promauto.NewHistogram(prometheus.HistogramOpts{
			Name:      name,
			Namespace: p.prefix,
			Help:      name,
			// This is an attempt at a usable set of buckets for a wide range of metrics
			// 16 buckets, first upper bound of 1, each following upper bound is 4x the previous
			Buckets: prometheus.ExponentialBuckets(1, 4, 16),
		})
	}

	p.metrics[name] = newmet
	p.values[PrefixMetricName(p.prefix, name)] = 0
}

func (p *PromMetrics) Get(name string) (float64, bool) {
	v, ok := p.values[PrefixMetricName(p.prefix, name)]
	return v, ok
}

func (p *PromMetrics) Increment(name string) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if counterIface, ok := p.metrics[name]; ok {
		if counter, ok := counterIface.(prometheus.Counter); ok {
			counter.Inc()
			p.values[PrefixMetricName(p.prefix, name)]++
		}
	}
}
func (p *PromMetrics) Count(name string, n interface{}) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if counterIface, ok := p.metrics[name]; ok {
		if counter, ok := counterIface.(prometheus.Counter); ok {
			f := ConvertNumeric(n)
			counter.Add(f)
			p.values[PrefixMetricName(p.prefix, name)] += f
		}
	}
}
func (p *PromMetrics) Gauge(name string, val interface{}) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if gaugeIface, ok := p.metrics[name]; ok {
		if gauge, ok := gaugeIface.(prometheus.Gauge); ok {
			f := ConvertNumeric(val)
			gauge.Set(f)
			p.values[PrefixMetricName(p.prefix, name)] = f
		}
	}
}
func (p *PromMetrics) Histogram(name string, obs interface{}) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if histIface, ok := p.metrics[name]; ok {
		if hist, ok := histIface.(prometheus.Histogram); ok {
			hist.Observe(ConvertNumeric(obs))
		}
	}
}
func (p *PromMetrics) Up(name string) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if gaugeIface, ok := p.metrics[name]; ok {
		if gauge, ok := gaugeIface.(prometheus.Gauge); ok {
			gauge.Inc()
			p.values[PrefixMetricName(p.prefix, name)]++
		}
	}
}
func (p *PromMetrics) Down(name string) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if gaugeIface, ok := p.metrics[name]; ok {
		if gauge, ok := gaugeIface.(prometheus.Gauge); ok {
			gauge.Dec()
			p.values[PrefixMetricName(p.prefix, name)]--
		}
	}
}

func (p *PromMetrics) Store(name string, val float64) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	p.values[PrefixMetricName(p.prefix, name)] = val
}

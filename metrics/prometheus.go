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
	lock    sync.RWMutex

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
	case "gauge":
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
}

func (p *PromMetrics) Increment(name string) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if counterIface, ok := p.metrics[name]; ok {
		if counter, ok := counterIface.(prometheus.Counter); ok {
			counter.Inc()
		}
	}
}
func (p *PromMetrics) Count(name string, n interface{}) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if counterIface, ok := p.metrics[name]; ok {
		if counter, ok := counterIface.(prometheus.Counter); ok {
			counter.Add(ConvertNumeric(n))
		}
	}
}
func (p *PromMetrics) Gauge(name string, val interface{}) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if gaugeIface, ok := p.metrics[name]; ok {
		if gauge, ok := gaugeIface.(prometheus.Gauge); ok {
			gauge.Set(ConvertNumeric(val))
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

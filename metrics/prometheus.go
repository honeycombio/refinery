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

var _ MetricsBackend = (*PromMetrics)(nil)

type PromMetrics struct {
	Config config.Config `inject:""`
	Logger logger.Logger `inject:""`
	// metrics keeps a record of all the registered metrics so we can increment
	// them by name
	metrics map[string]interface{}
	lock    sync.RWMutex
}

func (p *PromMetrics) Start() error {
	p.Logger.Debug().Logf("Starting PromMetrics")
	defer func() { p.Logger.Debug().Logf("Finished starting PromMetrics") }()
	pc := p.Config.GetPrometheusMetricsConfig()

	p.lock.Lock()
	defer p.lock.Unlock()

	p.metrics = make(map[string]interface{})

	muxxer := mux.NewRouter()

	muxxer.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(pc.ListenAddr, muxxer)
	return nil
}

// Register takes a name and a metric type. The type should be one of "counter",
// "gauge", or "histogram"
func (p *PromMetrics) Register(metadata Metadata) {
	p.lock.Lock()
	defer p.lock.Unlock()

	newmet, exists := p.metrics[metadata.Name]

	// don't attempt to add the metric again as this will cause a panic
	if exists {
		return
	}

	help := metadata.Description
	if help == "" {
		help = metadata.Name
	}
	switch metadata.Type {
	case Counter:
		newmet = promauto.NewCounter(prometheus.CounterOpts{
			Name: metadata.Name,
			Help: help,
		})
	case Gauge, UpDown: // updown is a special gauge
		newmet = promauto.NewGauge(prometheus.GaugeOpts{
			Name: metadata.Name,
			Help: help,
		})
	case Histogram:
		newmet = promauto.NewHistogram(prometheus.HistogramOpts{
			Name: metadata.Name,
			Help: help,
			// This is an attempt at a usable set of buckets for a wide range of metrics
			// 16 buckets, first upper bound of 1, each following upper bound is 4x the previous
			Buckets: prometheus.ExponentialBuckets(1, 4, 16),
		})
	}

	p.metrics[metadata.Name] = newmet
}

func (p *PromMetrics) Increment(name string) {
	p.lock.RLock()
	counterIface, ok := p.metrics[name]
	p.lock.RUnlock()

	if ok {

		if counter, ok := counterIface.(prometheus.Counter); ok {
			counter.Inc()
		}
	}
}
func (p *PromMetrics) Count(name string, n int64) {
	p.lock.RLock()
	counterIface, ok := p.metrics[name]
	p.lock.RUnlock()

	if ok {
		if counter, ok := counterIface.(prometheus.Counter); ok {
			counter.Add(float64(n))
		}
	}
}
func (p *PromMetrics) Gauge(name string, val float64) {
	p.lock.RLock()
	gaugeIface, ok := p.metrics[name]
	p.lock.RUnlock()

	if ok {
		if gauge, ok := gaugeIface.(prometheus.Gauge); ok {
			gauge.Set(val)
		}
	}
}
func (p *PromMetrics) Histogram(name string, obs float64) {
	p.lock.RLock()
	histIface, ok := p.metrics[name]
	p.lock.RUnlock()

	if ok {
		if hist, ok := histIface.(prometheus.Histogram); ok {
			hist.Observe(obs)
		}
	}
}
func (p *PromMetrics) Up(name string) {
	p.lock.RLock()
	gaugeIface, ok := p.metrics[name]
	p.lock.RUnlock()

	if ok {
		if gauge, ok := gaugeIface.(prometheus.Gauge); ok {
			gauge.Inc()
		}
	}
}

func (p *PromMetrics) Down(name string) {
	p.lock.Lock()
	gaugeIface, ok := p.metrics[name]
	p.lock.Unlock()

	if ok {
		if gauge, ok := gaugeIface.(prometheus.Gauge); ok {
			gauge.Dec()
		}
	}
}

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
	lock    sync.Mutex
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
	var newmet interface{}
	switch metricType {
	case "counter":
		newmet = promauto.NewCounter(prometheus.CounterOpts{
			Name: name,
			Help: name,
		})
	case "gauge":
		newmet = promauto.NewGauge(prometheus.GaugeOpts{
			Name: name,
			Help: name,
		})
	case "histogram":
		newmet = promauto.NewHistogram(prometheus.HistogramOpts{
			Name: name,
			Help: name,
		})
	}

	p.lock.Lock()
	p.metrics[name] = newmet
	p.lock.Unlock()
}

func (p *PromMetrics) IncrementCounter(name string) {
	if counterIface, ok := p.metrics[name]; ok {
		if counter, ok := counterIface.(prometheus.Counter); ok {
			counter.Inc()
		}
	}
}
func (p *PromMetrics) Gauge(name string, val float64) {
	if gaugeIface, ok := p.metrics[name]; ok {
		if gauge, ok := gaugeIface.(prometheus.Gauge); ok {
			gauge.Set(val)
		}
	}
}
func (p *PromMetrics) Histogram(name string, obs float64) {
	if histIface, ok := p.metrics[name]; ok {
		if hist, ok := histIface.(prometheus.Histogram); ok {
			hist.Observe(obs)
		}
	}
}

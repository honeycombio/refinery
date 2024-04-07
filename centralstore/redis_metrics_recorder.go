package centralstore

import (
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/redis"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/jonboulle/clockwork"
)

// RedisMetricsRecorder reads from its injected metrics object and stores its
// most current data in Redis where it can be read and aggregated by a separate
// process. This is intended to allow a custom Kubernetes metrics reporter to
// serve metrics data to Kubernetes for autoscaling purposes. The data we report
// is the same data made available to stress relief (which doesn't include
// histograms).
type RedisMetricsRecorder struct {
	Config      config.Config         `inject:""`
	Logger      logger.Logger         `inject:""`
	Clock       clockwork.Clock       `inject:""`
	Metrics     *metrics.MultiMetrics `inject:"metrics"`
	Version     string                `inject:"version"`
	RedisClient redis.Client          `inject:"redis"`

	//reportingFreq is the interval with which to report statistics
	reportingFreq time.Duration
	prefix        string
	done          chan struct{}
}

func NewRedisReporter(prefix string) *RedisMetricsRecorder {
	return &RedisMetricsRecorder{
		prefix: prefix,
	}
}

func (r *RedisMetricsRecorder) Start() error {
	r.reportingFreq = 30 * time.Second // TODO: make this configurable
	r.done = make(chan struct{})
	go r.monitor()
	return nil
}

func (r *RedisMetricsRecorder) Stop() error {
	close(r.done)
	return nil
}

// We store the metrics in Redis as a hash with the prefix "Refinery_Metrics_" +
// r.prefix, which should be made unique to this instance of refinery. The hash
// is stored with an expiration time of 2 times the reporting frequency. The
// hash will be removed automatically once the refinery instance stops. It is
// expected that the metrics reporter will iterate the prefixes and aggregate them.
func (r *RedisMetricsRecorder) monitor() {
	ticker := r.Clock.NewTicker(r.reportingFreq)
	for {
		select {
		case <-ticker.Chan():
			allmetrics := r.Metrics.GetAll()
			conn := r.RedisClient.Get()
			conn.SetHashTTL("Refinery_Metrics_"+r.prefix, allmetrics, r.reportingFreq*2)
			conn.Close()
		case <-r.done:
			return
		}
	}
}

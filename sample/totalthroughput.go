package sample

import (
	"math/rand"
	"sync"
	"time"

	dynsampler "github.com/honeycombio/dynsampler-go"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

// createDynForTotalThroughputSampler creates a dynsampler for TotalThroughputSampler
func createDynForTotalThroughputSampler(c *config.TotalThroughputSamplerConfig) *dynsampler.TotalThroughput {
	maxKeys := c.MaxKeys
	if maxKeys == 0 {
		maxKeys = 500
	}
	clearFreq := c.ClearFrequency
	if clearFreq == 0 {
		clearFreq = config.Duration(30 * time.Second)
	}
	clusterSize := 1 // Will be updated by SetClusterSize if needed

	dynsampler := &dynsampler.TotalThroughput{
		GoalThroughputPerSec:   c.GoalThroughputPerSec / clusterSize,
		ClearFrequencyDuration: time.Duration(clearFreq),
		MaxKeys:                maxKeys,
	}
	dynsampler.Start()
	return dynsampler
}

type TotalThroughputSampler struct {
	Config  *config.TotalThroughputSamplerConfig
	Logger  logger.Logger
	Metrics metrics.Metrics

	key                      *traceKey
	keyMu                    sync.Mutex
	keyFields, nonRootFields []string

	dynsampler      *dynsampler.TotalThroughput
	metricsRecorder *dynsamplerMetricsRecorder
}

func (d *TotalThroughputSampler) Start() error {
	d.Logger.Debug().Logf("Starting TotalThroughputSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting TotalThroughputSampler") }()

	// If dynsampler is not set (e.g., in tests), create it
	if d.dynsampler == nil {
		d.dynsampler = createDynForTotalThroughputSampler(d.Config)
	}

	d.key = newTraceKey(d.Config.FieldList, d.Config.UseTraceLength)
	d.keyFields, d.nonRootFields = config.GetKeyFields(d.Config.GetSamplingFields())

	// Register statistics this package will produce
	d.metricsRecorder = &dynsamplerMetricsRecorder{
		prefix: "totalthroughput",
		met:    d.Metrics,
	}
	d.metricsRecorder.RegisterMetrics(d.dynsampler)
	return nil
}


func (d *TotalThroughputSampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool, reason string, key string) {
	d.keyMu.Lock()
	key, n := d.key.build(trace)
	d.keyMu.Unlock()

	if n == maxKeyLength {
		d.Logger.Debug().Logf("trace key hit max length of %d, truncating", maxKeyLength)
	}
	count := int(trace.DescendantCount())

	rate = uint(d.dynsampler.GetSampleRateMulti(key, count))
	if rate < 1 { // protect against dynsampler being broken even though it shouldn't be
		rate = 1
	}
	shouldKeep := rand.Intn(int(rate)) == 0
	d.metricsRecorder.RecordMetrics(d.dynsampler, shouldKeep, rate, n)

	d.Logger.Debug().WithFields(map[string]interface{}{
		"sample_key":  key,
		"sample_rate": rate,
		"sample_keep": shouldKeep,
		"trace_id":    trace.TraceID,
		"span_count":  count,
	}).Logf("got sample rate and decision")

	return rate, shouldKeep, "totalthroughput", key
}

func (d *TotalThroughputSampler) GetKeyFields() ([]string, []string) {
	return d.keyFields, d.nonRootFields
}

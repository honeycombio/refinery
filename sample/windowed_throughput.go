package sample

import (
	"math"
	"math/rand"
	"time"

	dynsampler "github.com/honeycombio/dynsampler-go"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

// createDynForWindowedThroughputSampler creates a dynsampler for WindowedThroughputSampler.
// workerCount is the number of concurrent collection workers; the goal throughput is
// divided by workerCount so the aggregate across all workers matches the configured goal.
func createDynForWindowedThroughputSampler(c *config.WindowedThroughputSamplerConfig, workerCount int) *dynsampler.WindowedThroughput {
	maxKeys := c.MaxKeys
	if maxKeys == 0 {
		maxKeys = 500
	}

	dynsamplerInstance := &dynsampler.WindowedThroughput{
		GoalThroughputPerSec:      math.Max(float64(c.GoalThroughputPerSec)/float64(workerCount), 1),
		UpdateFrequencyDuration:   time.Duration(c.UpdateFrequency),
		LookbackFrequencyDuration: time.Duration(c.LookbackFrequency),
		MaxKeys:                   maxKeys,
	}
	dynsamplerInstance.Start()
	return dynsamplerInstance
}

type WindowedThroughputSampler struct {
	Config  *config.WindowedThroughputSamplerConfig
	Logger  logger.Logger
	Metrics metrics.Metrics

	key                      *traceKey
	keyFields, nonRootFields []string

	dynsampler      *dynsampler.WindowedThroughput
	metricsRecorder *dynsamplerMetricsRecorder
}

func (d *WindowedThroughputSampler) Start() error {
	d.Logger.Debug().Logf("Starting WindowedThroughputSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting WindowedThroughputSampler") }()

	// If dynsampler is not set (e.g., in tests), create it with workerCount=1
	if d.dynsampler == nil {
		d.dynsampler = createDynForWindowedThroughputSampler(d.Config, 1)
	}

	d.key = newTraceKey(d.Config.FieldList, d.Config.UseTraceLength)
	d.keyFields, d.nonRootFields = config.GetKeyFields(d.Config.GetSamplingFields())

	// Register statistics this package will produce
	d.metricsRecorder = &dynsamplerMetricsRecorder{
		prefix: "windowedthroughput",
		met:    d.Metrics,
	}
	d.metricsRecorder.RegisterMetrics(d.dynsampler)
	return nil
}

func (d *WindowedThroughputSampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool, reason string, key string) {
	key, n := d.key.build(trace)

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

	return rate, shouldKeep, "windowedthroughput", key
}

func (d *WindowedThroughputSampler) GetKeyFields() ([]string, []string) {
	return d.keyFields, d.nonRootFields
}

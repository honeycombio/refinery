package sample

import (
	"fmt"
	"math/rand"
	"time"

	dynsampler "github.com/honeycombio/dynsampler-go"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

// effectiveWindowedThroughputSamplerConfig fills in the defaults that would
// otherwise be applied when the engine is built. See effectiveDynamicSamplerConfig.
func effectiveWindowedThroughputSamplerConfig(c *config.WindowedThroughputSamplerConfig) config.WindowedThroughputSamplerConfig {
	e := *c
	if e.GoalThroughputPerSec == 0 {
		e.GoalThroughputPerSec = 100
	}
	if e.UpdateFrequency == 0 {
		e.UpdateFrequency = config.Duration(time.Second)
	}
	if e.LookbackFrequency == 0 {
		e.LookbackFrequency = 30 * e.UpdateFrequency
	}
	// The engine floors the lookback to a whole number of update windows, so a
	// 90s and a 91s lookback over a 2s window are the same engine.
	e.LookbackFrequency = e.UpdateFrequency * (e.LookbackFrequency / e.UpdateFrequency)
	if e.MaxKeys == 0 {
		e.MaxKeys = 500
	}
	return e
}

// windowedThroughputSamplerConfigKey is the config half of the shared-dynsampler key.
func windowedThroughputSamplerConfigKey(c *config.WindowedThroughputSamplerConfig) string {
	e := effectiveWindowedThroughputSamplerConfig(c)
	return fmt.Sprintf("UpdateFrequency=%v,LookbackFrequency=%v,GoalThroughputPerSec=%d,UseClusterSize=%t,FieldList=%q,MaxKeys=%d,UseTraceLength=%t",
		time.Duration(e.UpdateFrequency), time.Duration(e.LookbackFrequency), e.GoalThroughputPerSec, e.UseClusterSize, sortedFieldList(e.FieldList), e.MaxKeys, e.UseTraceLength)
}

// createDynForWindowedThroughputSampler creates a dynsampler for WindowedThroughputSampler
func createDynForWindowedThroughputSampler(c *config.WindowedThroughputSamplerConfig) *dynsampler.WindowedThroughput {
	e := effectiveWindowedThroughputSamplerConfig(c)
	clusterSize := 1 // Will be updated by SetClusterSize if needed

	dynsamplerInstance := &dynsampler.WindowedThroughput{
		GoalThroughputPerSec:      float64(e.GoalThroughputPerSec) / float64(clusterSize),
		UpdateFrequencyDuration:   time.Duration(e.UpdateFrequency),
		LookbackFrequencyDuration: time.Duration(e.LookbackFrequency),
		MaxKeys:                   e.MaxKeys,
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

	// If dynsampler is not set (e.g., in tests), create it
	if d.dynsampler == nil {
		d.dynsampler = createDynForWindowedThroughputSampler(d.Config)
	}

	d.key = newTraceKey(d.Config.FieldList, d.Config.UseTraceLength)
	d.keyFields, d.nonRootFields = config.GetKeyFields(d.Config.GetSamplingFields())

	if d.metricsRecorder == nil {
		d.metricsRecorder = &dynsamplerMetricsRecorder{
			prefix: "windowedthroughput",
			met:    d.Metrics,
		}
		d.metricsRecorder.RegisterMetrics(d.dynsampler)
	}
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

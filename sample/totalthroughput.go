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

// effectiveTotalThroughputSamplerConfig fills in the defaults that would
// otherwise be applied when the engine is built. See effectiveDynamicSamplerConfig.
func effectiveTotalThroughputSamplerConfig(c *config.TotalThroughputSamplerConfig) config.TotalThroughputSamplerConfig {
	e := *c
	if e.GoalThroughputPerSec == 0 {
		e.GoalThroughputPerSec = 100
	}
	if e.ClearFrequency == 0 {
		e.ClearFrequency = config.Duration(30 * time.Second)
	}
	if e.MaxKeys == 0 {
		e.MaxKeys = 500
	}
	return e
}

// totalThroughputSamplerConfigKey is the config half of the shared-dynsampler key.
func totalThroughputSamplerConfigKey(c *config.TotalThroughputSamplerConfig) string {
	e := effectiveTotalThroughputSamplerConfig(c)
	return fmt.Sprintf("GoalThroughputPerSec=%d,UseClusterSize=%t,ClearFrequency=%v,FieldList=%q,MaxKeys=%d,UseTraceLength=%t",
		e.GoalThroughputPerSec, e.UseClusterSize, time.Duration(e.ClearFrequency), sortedFieldList(e.FieldList), e.MaxKeys, e.UseTraceLength)
}

// createDynForTotalThroughputSampler creates a dynsampler for TotalThroughputSampler
func createDynForTotalThroughputSampler(c *config.TotalThroughputSamplerConfig) *dynsampler.TotalThroughput {
	e := effectiveTotalThroughputSamplerConfig(c)
	clusterSize := 1 // Will be updated by SetClusterSize if needed

	dynsampler := &dynsampler.TotalThroughput{
		GoalThroughputPerSec:   e.GoalThroughputPerSec / clusterSize,
		ClearFrequencyDuration: time.Duration(e.ClearFrequency),
		MaxKeys:                e.MaxKeys,
	}
	dynsampler.Start()
	return dynsampler
}

type TotalThroughputSampler struct {
	Config  *config.TotalThroughputSamplerConfig
	Logger  logger.Logger
	Metrics metrics.Metrics

	key                      *traceKey
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

	if d.metricsRecorder == nil {
		d.metricsRecorder = &dynsamplerMetricsRecorder{
			prefix: "totalthroughput",
			met:    d.Metrics,
		}
		d.metricsRecorder.RegisterMetrics(d.dynsampler)
	}
	return nil
}

func (d *TotalThroughputSampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool, reason string, key string) {
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

	return rate, shouldKeep, "totalthroughput", key
}

func (d *TotalThroughputSampler) GetKeyFields() ([]string, []string) {
	return d.keyFields, d.nonRootFields
}

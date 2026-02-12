package sample

import (
	"math/rand"
	"time"

	dynsampler "github.com/honeycombio/dynsampler-go"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

// createDynForEMADynamicSampler creates a dynsampler for EMADynamicSampler
func createDynForEMADynamicSampler(c *config.EMADynamicSamplerConfig) *dynsampler.EMASampleRate {
	maxKeys := c.MaxKeys
	if maxKeys == 0 {
		maxKeys = 500
	}

	dynsampler := &dynsampler.EMASampleRate{
		GoalSampleRate:             c.GoalSampleRate,
		AdjustmentIntervalDuration: time.Duration(c.AdjustmentInterval),
		Weight:                     c.Weight,
		AgeOutValue:                c.AgeOutValue,
		BurstDetectionDelay:        c.BurstDetectionDelay,
		BurstMultiple:              c.BurstMultiple,
		MaxKeys:                    maxKeys,
	}
	dynsampler.Start()
	return dynsampler
}

type EMADynamicSampler struct {
	Config  *config.EMADynamicSamplerConfig
	Logger  logger.Logger
	Metrics metrics.Metrics

	key                      *traceKey
	keyFields, nonRootFields []string

	dynsampler      *dynsampler.EMASampleRate
	metricsRecorder *dynsamplerMetricsRecorder
}

func (d *EMADynamicSampler) Start() error {
	d.Logger.Debug().Logf("Starting EMADynamicSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting EMADynamicSampler") }()

	// If dynsampler is not set (e.g., in tests), create it
	if d.dynsampler == nil {
		d.dynsampler = createDynForEMADynamicSampler(d.Config)
	}

	d.keyFields, d.nonRootFields = config.GetKeyFields(d.Config.GetSamplingFields())
	d.key = newTraceKey(d.Config.FieldList, d.Config.UseTraceLength)

	// Register statistics this package will produce
	d.metricsRecorder = &dynsamplerMetricsRecorder{
		prefix: "emadynamic",
		met:    d.Metrics,
	}
	d.metricsRecorder.RegisterMetrics(d.dynsampler)
	return nil
}

func (d *EMADynamicSampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool, reason string, key string) {
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
	d.Logger.Debug().WithFields(map[string]interface{}{
		"sample_key":  key,
		"sample_rate": rate,
		"sample_keep": shouldKeep,
		"trace_id":    trace.TraceID,
		"span_count":  count,
	}).Logf("got sample rate and decision")
	d.metricsRecorder.RecordMetrics(d.dynsampler, shouldKeep, rate, n)

	return rate, shouldKeep, "emadynamic", key
}

func (d *EMADynamicSampler) GetKeyFields() ([]string, []string) {
	return d.keyFields, d.nonRootFields

}

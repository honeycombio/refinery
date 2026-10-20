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

// effectiveEMADynamicSamplerConfig fills in the defaults that would otherwise be
// applied when the engine is built. See effectiveDynamicSamplerConfig.
func effectiveEMADynamicSamplerConfig(c *config.EMADynamicSamplerConfig) config.EMADynamicSamplerConfig {
	e := *c
	if e.GoalSampleRate == 0 {
		e.GoalSampleRate = 10
	}
	if e.AdjustmentInterval == 0 {
		e.AdjustmentInterval = config.Duration(15 * time.Second)
	}
	if e.Weight == 0 {
		e.Weight = 0.5
	}
	// AgeOutValue follows Weight, so it has to be defaulted after it.
	if e.AgeOutValue == 0 {
		e.AgeOutValue = e.Weight
	}
	if e.BurstMultiple == 0 {
		e.BurstMultiple = 2
	}
	if e.BurstDetectionDelay == 0 {
		e.BurstDetectionDelay = 3
	}
	if e.MaxKeys == 0 {
		e.MaxKeys = 500
	}
	return e
}

// emaDynamicSamplerConfigKey is the config half of the shared-dynsampler key.
func emaDynamicSamplerConfigKey(c *config.EMADynamicSamplerConfig) string {
	e := effectiveEMADynamicSamplerConfig(c)
	return fmt.Sprintf("GoalSampleRate=%d,AdjustmentInterval=%v,Weight=%s,AgeOutValue=%s,BurstMultiple=%s,BurstDetectionDelay=%d,FieldList=%q,MaxKeys=%d,UseTraceLength=%t",
		e.GoalSampleRate, time.Duration(e.AdjustmentInterval), formatFloat(e.Weight), formatFloat(e.AgeOutValue), formatFloat(e.BurstMultiple), e.BurstDetectionDelay, sortedFieldList(e.FieldList), e.MaxKeys, e.UseTraceLength)
}

// createDynForEMADynamicSampler creates a dynsampler for EMADynamicSampler
func createDynForEMADynamicSampler(c *config.EMADynamicSamplerConfig) *dynsampler.EMASampleRate {
	e := effectiveEMADynamicSamplerConfig(c)

	dynsampler := &dynsampler.EMASampleRate{
		GoalSampleRate:             e.GoalSampleRate,
		AdjustmentIntervalDuration: time.Duration(e.AdjustmentInterval),
		Weight:                     e.Weight,
		AgeOutValue:                e.AgeOutValue,
		BurstDetectionDelay:        e.BurstDetectionDelay,
		BurstMultiple:              e.BurstMultiple,
		MaxKeys:                    e.MaxKeys,
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

	if d.metricsRecorder == nil {
		d.metricsRecorder = &dynsamplerMetricsRecorder{
			prefix: "emadynamic",
			met:    d.Metrics,
		}
		d.metricsRecorder.RegisterMetrics(d.dynsampler)
	}
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

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

// effectiveDynamicSamplerConfig fills in the defaults that would otherwise be
// applied when the engine is built, some by us and some by dynsampler.Start.
// The engine and the shared-dynsampler key are both derived from the result, so
// a config that omits a field cannot land on a different engine from one that
// writes that field's default.
func effectiveDynamicSamplerConfig(c *config.DynamicSamplerConfig) config.DynamicSamplerConfig {
	e := *c
	if e.SampleRate == 0 {
		e.SampleRate = 10
	}
	if e.ClearFrequency == 0 {
		e.ClearFrequency = config.Duration(30 * time.Second)
	}
	if e.MaxKeys == 0 {
		e.MaxKeys = 500
	}
	return e
}

// dynamicSamplerConfigKey is the config half of the shared-dynsampler key. It
// must name every field effectiveDynamicSamplerConfig feeds to the engine, or
// two differently behaving samplers share one.
func dynamicSamplerConfigKey(c *config.DynamicSamplerConfig) string {
	e := effectiveDynamicSamplerConfig(c)
	return fmt.Sprintf("SampleRate=%d,ClearFrequency=%v,FieldList=%q,MaxKeys=%d,UseTraceLength=%t",
		e.SampleRate, time.Duration(e.ClearFrequency), sortedFieldList(e.FieldList), e.MaxKeys, e.UseTraceLength)
}

// createDynForDynamicSampler creates a dynsampler for DynamicSampler
func createDynForDynamicSampler(c *config.DynamicSamplerConfig) dynsampler.Sampler {
	e := effectiveDynamicSamplerConfig(c)

	dynsamplerInstance := &dynsampler.AvgSampleRate{
		GoalSampleRate:         int(e.SampleRate),
		ClearFrequencyDuration: time.Duration(e.ClearFrequency),
		MaxKeys:                e.MaxKeys,
	}
	dynsamplerInstance.Start()
	return dynsamplerInstance
}

type DynamicSampler struct {
	Config  *config.DynamicSamplerConfig
	Logger  logger.Logger
	Metrics metrics.Metrics

	key                      *traceKey
	keyFields, nonRootFields []string

	dynsampler      dynsampler.Sampler
	metricsRecorder *dynsamplerMetricsRecorder
}

func (d *DynamicSampler) Start() error {
	d.Logger.Debug().Logf("Starting DynamicSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting DynamicSampler") }()

	// If dynsampler is not set (e.g., in tests), create it
	if d.dynsampler == nil {
		d.dynsampler = createDynForDynamicSampler(d.Config)
	}

	d.key = newTraceKey(d.Config.FieldList, d.Config.UseTraceLength)
	d.keyFields, d.nonRootFields = config.GetKeyFields(d.Config.GetSamplingFields())

	if d.metricsRecorder == nil {
		d.metricsRecorder = &dynsamplerMetricsRecorder{
			met:    d.Metrics,
			prefix: "dynamic",
		}
		d.metricsRecorder.RegisterMetrics(d.dynsampler)
	}

	return nil
}

func (d *DynamicSampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool, reason string, key string) {
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
	return rate, shouldKeep, "dynamic", key
}

func (d *DynamicSampler) GetKeyFields() ([]string, []string) {
	return d.keyFields, d.nonRootFields
}

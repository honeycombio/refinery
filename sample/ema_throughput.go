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

// effectiveEMAThroughputSamplerConfig fills in the defaults that would otherwise
// be applied when the engine is built. See effectiveDynamicSamplerConfig.
func effectiveEMAThroughputSamplerConfig(c *config.EMAThroughputSamplerConfig) config.EMAThroughputSamplerConfig {
	e := *c
	if e.GoalThroughputPerSec == 0 {
		e.GoalThroughputPerSec = 100
	}
	if e.InitialSampleRate == 0 {
		e.InitialSampleRate = 10
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

// emaThroughputSamplerConfigKey is the config half of the shared-dynsampler key.
func emaThroughputSamplerConfigKey(c *config.EMAThroughputSamplerConfig) string {
	e := effectiveEMAThroughputSamplerConfig(c)
	return fmt.Sprintf("GoalThroughputPerSec=%d,UseClusterSize=%t,InitialSampleRate=%d,AdjustmentInterval=%v,Weight=%s,AgeOutValue=%s,BurstMultiple=%s,BurstDetectionDelay=%d,FieldList=%q,MaxKeys=%d,UseTraceLength=%t",
		e.GoalThroughputPerSec, e.UseClusterSize, e.InitialSampleRate, time.Duration(e.AdjustmentInterval), formatFloat(e.Weight), formatFloat(e.AgeOutValue), formatFloat(e.BurstMultiple), e.BurstDetectionDelay, sortedFieldList(e.FieldList), e.MaxKeys, e.UseTraceLength)
}

// createDynForEMAThroughputSampler creates a dynsampler for EMAThroughputSampler
func createDynForEMAThroughputSampler(c *config.EMAThroughputSamplerConfig) *dynsampler.EMAThroughput {
	e := effectiveEMAThroughputSamplerConfig(c)
	clusterSize := 1 // Will be updated by SetClusterSize if needed

	dynsamplerInstance := &dynsampler.EMAThroughput{
		GoalThroughputPerSec: e.GoalThroughputPerSec / clusterSize,
		InitialSampleRate:    e.InitialSampleRate,
		AdjustmentInterval:   time.Duration(e.AdjustmentInterval),
		Weight:               e.Weight,
		AgeOutValue:          e.AgeOutValue,
		BurstDetectionDelay:  e.BurstDetectionDelay,
		BurstMultiple:        e.BurstMultiple,
		MaxKeys:              e.MaxKeys,
	}
	dynsamplerInstance.Start()
	return dynsamplerInstance
}

type EMAThroughputSampler struct {
	Config  *config.EMAThroughputSamplerConfig
	Logger  logger.Logger
	Metrics metrics.Metrics

	key                      *traceKey
	keyFields, nonRootFields []string

	dynsampler      *dynsampler.EMAThroughput
	metricsRecorder *dynsamplerMetricsRecorder
}

func (d *EMAThroughputSampler) Start() error {
	d.Logger.Debug().Logf("Starting EMAThroughputSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting EMAThroughputSampler") }()

	// If dynsampler is not set (e.g., in tests), create it
	if d.dynsampler == nil {
		d.dynsampler = createDynForEMAThroughputSampler(d.Config)
	}

	d.key = newTraceKey(d.Config.FieldList, d.Config.UseTraceLength)
	d.keyFields, d.nonRootFields = config.GetKeyFields(d.Config.GetSamplingFields())

	if d.metricsRecorder == nil {
		d.metricsRecorder = &dynsamplerMetricsRecorder{
			prefix: "emathroughput",
			met:    d.Metrics,
		}
		d.metricsRecorder.RegisterMetrics(d.dynsampler)
	}

	return nil
}

func (d *EMAThroughputSampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool, reason string, key string) {
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
	return rate, shouldKeep, "emathroughput", key
}

func (d *EMAThroughputSampler) GetKeyFields() ([]string, []string) {
	return d.keyFields, d.nonRootFields
}

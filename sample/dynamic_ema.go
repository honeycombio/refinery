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

type EMADynamicSampler struct {
	Config  *config.EMADynamicSamplerConfig
	Logger  logger.Logger
	Metrics metrics.Metrics

	goalSampleRate      int
	adjustmentInterval  config.Duration
	weight              float64
	ageOutValue         float64
	burstMultiple       float64
	burstDetectionDelay uint
	maxKeys             int
	prefix              string

	key       *traceKey
	keyFields []string

	dynsampler      *dynsampler.EMASampleRate
	metricsRecorder *dynsamplerMetricsRecorder
}

func (d *EMADynamicSampler) Start() error {
	d.Logger.Debug().Logf("Starting EMADynamicSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting EMADynamicSampler") }()
	d.goalSampleRate = d.Config.GoalSampleRate
	d.adjustmentInterval = d.Config.AdjustmentInterval
	d.weight = d.Config.Weight
	d.ageOutValue = d.Config.AgeOutValue
	d.burstMultiple = d.Config.BurstMultiple
	d.burstDetectionDelay = d.Config.BurstDetectionDelay
	d.key = newTraceKey(d.Config.FieldList, d.Config.UseTraceLength)
	d.maxKeys = d.Config.MaxKeys
	if d.maxKeys == 0 {
		d.maxKeys = 500
	}
	d.prefix = "emadynamic"
	d.keyFields = d.Config.GetSamplingFields()

	// spin up the actual dynamic sampler
	d.dynsampler = &dynsampler.EMASampleRate{
		GoalSampleRate:             d.goalSampleRate,
		AdjustmentIntervalDuration: time.Duration(d.adjustmentInterval),
		Weight:                     d.weight,
		AgeOutValue:                d.ageOutValue,
		BurstDetectionDelay:        d.burstDetectionDelay,
		BurstMultiple:              d.burstMultiple,
		MaxKeys:                    d.maxKeys,
	}
	d.dynsampler.Start()

	// Register statistics this package will produce
	d.metricsRecorder = &dynsamplerMetricsRecorder{
		prefix: d.prefix,
		met:    d.Metrics,
	}

	d.metricsRecorder.RegisterMetrics(d.dynsampler)
	return nil
}

func (d *EMADynamicSampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool, summarize bool, reason string, key string) {
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

	// Handle summarization based on configuration
	summarize = false
	switch d.Config.SummarizeMode {
	case "all":
		summarize = true
	case "dropped":
		summarize = !shouldKeep
	case "kept":
		summarize = shouldKeep
	}
	d.Logger.Debug().WithFields(map[string]interface{}{
		"sample_key":  key,
		"sample_rate": rate,
		"sample_keep": shouldKeep,
		"trace_id":    trace.TraceID,
		"span_count":  count,
		"summarize":   summarize,
	}).Logf("got sample rate and decision")

	d.metricsRecorder.RecordMetrics(d.dynsampler, shouldKeep, rate, n, summarize)

	return rate, shouldKeep, summarize, d.prefix, key
}

func (d *EMADynamicSampler) GetKeyFields() []string {
	return d.keyFields
}

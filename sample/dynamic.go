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

type DynamicSampler struct {
	Config  *config.DynamicSamplerConfig
	Logger  logger.Logger
	Metrics metrics.Metrics

	sampleRate     int64
	clearFrequency config.Duration
	maxKeys        int
	prefix         string

	key       *traceKey
	keyFields []string

	dynsampler      dynsampler.Sampler
	metricsRecorder dynsamplerMetricsRecorder
}

func (d *DynamicSampler) Start() error {
	d.Logger.Debug().Logf("Starting DynamicSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting DynamicSampler") }()
	d.sampleRate = d.Config.SampleRate
	if d.Config.ClearFrequency == 0 {
		d.Config.ClearFrequency = config.Duration(30 * time.Second)
	}
	d.clearFrequency = d.Config.ClearFrequency
	d.key = newTraceKey(d.Config.FieldList, d.Config.UseTraceLength)
	d.maxKeys = d.Config.MaxKeys
	if d.maxKeys == 0 {
		d.maxKeys = 500
	}
	d.keyFields = d.Config.GetSamplingFields()

	d.prefix = "dynamic"
	// spin up the actual dynamic sampler
	d.dynsampler = &dynsampler.AvgSampleRate{
		GoalSampleRate:         int(d.sampleRate),
		ClearFrequencyDuration: time.Duration(d.clearFrequency),
		MaxKeys:                d.maxKeys,
	}
	d.dynsampler.Start()

	// Register statistics from the dynsampler-go package
	d.metricsRecorder = dynsamplerMetricsRecorder{
		met:    d.Metrics,
		prefix: d.prefix,
	}
	d.metricsRecorder.RegisterMetrics(d.dynsampler)

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
	d.metricsRecorder.RecordMetrics(d.dynsampler, shouldKeep, rate)
	return rate, shouldKeep, d.prefix, key
}

func (d *DynamicSampler) GetKeyFields() []string {
	return d.keyFields
}

func (d *DynamicSampler) CountLateSpan() {}

// Make sure it implements Sampler
var _ Sampler = (*DynamicSampler)(nil)
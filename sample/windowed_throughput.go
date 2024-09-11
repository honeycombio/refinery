package sample

import (
	"time"

	dynsampler "github.com/honeycombio/dynsampler-go"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

var _ Sampler = (*WindowedThroughputSampler)(nil)

type WindowedThroughputSampler struct {
	Config  *config.WindowedThroughputSamplerConfig
	Logger  logger.Logger
	Metrics metrics.Metrics

	updatefrequency      config.Duration
	lookbackfrequency    config.Duration
	goalThroughputPerSec int
	clusterSize          int
	useClusterSize       bool
	maxKeys              int
	prefix               string
	lastMetrics          map[string]int64

	key *traceKey

	dynsampler *dynsampler.WindowedThroughput
}

func (d *WindowedThroughputSampler) Start() error {
	d.Logger.Debug().Logf("Starting WindowedThroughputSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting WindowedThroughputSampler") }()
	d.goalThroughputPerSec = d.Config.GoalThroughputPerSec
	d.useClusterSize = d.Config.UseClusterSize
	if d.clusterSize == 0 {
		d.clusterSize = 1
	}
	d.updatefrequency = d.Config.UpdateFrequency
	d.lookbackfrequency = d.Config.LookbackFrequency
	d.key = newTraceKey(d.Config.FieldList, d.Config.UseTraceLength)
	d.maxKeys = d.Config.MaxKeys
	if d.maxKeys == 0 {
		d.maxKeys = 500
	}
	d.prefix = "windowedthroughput_"

	// spin up the actual dynamic sampler
	d.dynsampler = &dynsampler.WindowedThroughput{
		GoalThroughputPerSec:      float64(d.goalThroughputPerSec) / float64(d.clusterSize),
		UpdateFrequencyDuration:   time.Duration(d.updatefrequency),
		LookbackFrequencyDuration: time.Duration(d.lookbackfrequency),
		MaxKeys:                   d.maxKeys,
	}
	d.dynsampler.Start()

	// Register statistics this package will produce
	d.lastMetrics = d.dynsampler.GetMetrics(d.prefix)
	for name := range d.lastMetrics {
		d.Metrics.Register(name, getMetricType(name))
	}
	d.Metrics.Register(d.prefix+"num_dropped", "counter")
	d.Metrics.Register(d.prefix+"num_kept", "counter")
	d.Metrics.Register(d.prefix+"sample_rate", "histogram")

	return nil
}

func (d *WindowedThroughputSampler) SetClusterSize(size int) {
	if d.useClusterSize {
		d.clusterSize = size
		d.dynsampler.GoalThroughputPerSec = float64(d.goalThroughputPerSec) / float64(size)
	}
}

func (d *WindowedThroughputSampler) GetSampleRate(trace *types.Trace) (rate uint, reason string, key string) {
	key = d.key.build(trace)
	count := int(trace.DescendantCount())

	rate = uint(d.dynsampler.GetSampleRateMulti(key, count))
	if rate < 1 { // protect against dynsampler being broken even though it shouldn't be
		rate = 1
	}
	d.Logger.Debug().WithFields(map[string]interface{}{
		"sample_key":  key,
		"sample_rate": rate,
		"trace_id":    trace.TraceID,
		"span_count":  count,
	}).Logf("got sample rate")

	d.Metrics.Histogram(d.prefix+"sample_rate", float64(rate))

	for name, val := range d.dynsampler.GetMetrics(d.prefix) {
		switch getMetricType(name) {
		case "counter":
			delta := val - d.lastMetrics[name]
			d.Metrics.Count(name, delta)
			d.lastMetrics[name] = val
		case "gauge":
			d.Metrics.Gauge(name, val)
		}
	}
	return rate, "Windowedthroughput", key
}

func (d *WindowedThroughputSampler) MakeSamplingDecision(rate uint, trace *types.Trace) bool {
	keep := makeSamplingDecision(rate)
	if keep {
		d.Metrics.Increment(d.prefix + "num_kept")
	} else {
		d.Metrics.Increment(d.prefix + "num_dropped")
	}

	d.Logger.Debug().WithFields(map[string]interface{}{
		"sample_rate": rate,
		"trace_id":    trace.TraceID,
	}).Logf("got sample decision")
	return keep
}

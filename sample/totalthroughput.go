package sample

import (
	"time"

	dynsampler "github.com/honeycombio/dynsampler-go"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

var _ Sampler = (*TotalThroughputSampler)(nil)

type TotalThroughputSampler struct {
	Config  *config.TotalThroughputSamplerConfig
	Logger  logger.Logger
	Metrics metrics.Metrics

	goalThroughputPerSec int
	clusterSize          int
	useClusterSize       bool
	clearFrequency       config.Duration
	maxKeys              int
	prefix               string
	lastMetrics          map[string]int64

	key *traceKey

	dynsampler *dynsampler.TotalThroughput
}

func (d *TotalThroughputSampler) Start() error {
	d.Logger.Debug().Logf("Starting TotalThroughputSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting TotalThroughputSampler") }()
	if d.Config.GoalThroughputPerSec < 1 {
		d.Logger.Debug().Logf("configured sample rate for dynamic sampler was %d; forcing to 100", d.Config.GoalThroughputPerSec)
		d.Config.GoalThroughputPerSec = 100
	}
	d.goalThroughputPerSec = d.Config.GoalThroughputPerSec
	d.useClusterSize = d.Config.UseClusterSize
	if d.clusterSize == 0 {
		d.clusterSize = 1
	}
	if d.Config.ClearFrequency == 0 {
		d.Config.ClearFrequency = config.Duration(30 * time.Second)
	}
	d.clearFrequency = d.Config.ClearFrequency
	d.key = newTraceKey(d.Config.FieldList, d.Config.UseTraceLength)
	d.maxKeys = d.Config.MaxKeys
	if d.maxKeys == 0 {
		d.maxKeys = 500
	}
	d.prefix = "totalthroughput_"

	// spin up the actual dynamic sampler
	d.dynsampler = &dynsampler.TotalThroughput{
		GoalThroughputPerSec:   d.goalThroughputPerSec / d.clusterSize,
		ClearFrequencyDuration: time.Duration(d.clearFrequency),
		MaxKeys:                d.maxKeys,
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

func (d *TotalThroughputSampler) SetClusterSize(size int) {
	if d.useClusterSize {
		d.clusterSize = size
		d.dynsampler.GoalThroughputPerSec = d.goalThroughputPerSec / size
	}
}

func (d *TotalThroughputSampler) GetSampleRate(trace *types.Trace) (rate uint, reason string, key string) {
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
	return rate, "totalthroughput", key
}

func (d *TotalThroughputSampler) MakeSamplingDecision(rate uint, trace *types.Trace) bool {
	shouldKeep := makeSamplingDecision(rate)
	if shouldKeep {
		d.Metrics.Increment(d.prefix + "num_kept")
	} else {
		d.Metrics.Increment(d.prefix + "num_dropped")
	}
	d.Logger.Debug().WithFields(map[string]interface{}{
		"sample_rate": rate,
		"sample_keep": shouldKeep,
		"trace_id":    trace.TraceID,
	}).Logf("got sample decision")
	return shouldKeep
}

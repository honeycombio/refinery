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

type EMAThroughputSampler struct {
	Config  *config.EMAThroughputSamplerConfig
	Logger  logger.Logger
	Metrics metrics.Metrics

	adjustmentInterval   config.Duration
	weight               float64
	initialSampleRate    int
	goalThroughputPerSec int
	clusterSize          int
	useClusterSize       bool
	ageOutValue          float64
	burstMultiple        float64
	burstDetectionDelay  uint
	maxKeys              int
	prefix               string
	lastMetrics          map[string]int64

	key *traceKey

	dynsampler *dynsampler.EMAThroughput
}

func (d *EMAThroughputSampler) Start() error {
	d.Logger.Debug().Logf("Starting EMAThroughputSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting EMAThroughputSampler") }()
	d.initialSampleRate = d.Config.InitialSampleRate
	d.goalThroughputPerSec = d.Config.GoalThroughputPerSec
	d.useClusterSize = d.Config.UseClusterSize
	if d.clusterSize == 0 {
		d.clusterSize = 1
	}
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
	d.prefix = "emathroughput_"

	// spin up the actual dynamic sampler
	d.dynsampler = &dynsampler.EMAThroughput{
		GoalThroughputPerSec: d.goalThroughputPerSec / d.clusterSize,
		InitialSampleRate:    d.initialSampleRate,
		AdjustmentInterval:   time.Duration(d.adjustmentInterval),
		Weight:               d.weight,
		AgeOutValue:          d.ageOutValue,
		BurstDetectionDelay:  d.burstDetectionDelay,
		BurstMultiple:        d.burstMultiple,
		MaxKeys:              d.maxKeys,
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

func (d *EMAThroughputSampler) SetClusterSize(size int) {
	if d.useClusterSize {
		d.clusterSize = size
		d.dynsampler.GoalThroughputPerSec = d.goalThroughputPerSec / size
	}
}

func (d *EMAThroughputSampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool, reason string, key string) {
	key = d.key.build(trace)
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
	if shouldKeep {
		d.Metrics.Increment(d.prefix + "num_kept")
	} else {
		d.Metrics.Increment(d.prefix + "num_dropped")
	}
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
	return rate, shouldKeep, "emathroughput", key
}

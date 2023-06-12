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

type WindowedThroughputSampler struct {
	Config  *config.WindowedThroughputSamplerConfig
	Logger  logger.Logger
	Metrics metrics.Metrics

	updatefrequency      config.Duration
	lookbackfrequency    config.Duration
	goalthroughputpersec int
	maxKeys              int
	prefix               string
	lastMetrics          map[string]int64

	key *traceKey

	dynsampler dynsampler.Sampler
}

func (d *WindowedThroughputSampler) Start() error {
	d.Logger.Debug().Logf("Starting WindowedThroughputSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting WindowedThroughputSampler") }()
	d.goalthroughputpersec = d.Config.GoalThroughputPerSec
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
		GoalThroughputPerSec:      float64(d.goalthroughputpersec),
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

func (d *WindowedThroughputSampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool, reason string, key string) {
	key = d.key.build(trace)
	rate = uint(d.dynsampler.GetSampleRate(key))
	if rate < 1 { // protect against dynsampler being broken even though it shouldn't be
		rate = 1
	}
	shouldKeep := rand.Intn(int(rate)) == 0
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
		case "gauge":
			d.Metrics.Gauge(name, val)
		}
	}
	return rate, shouldKeep, "Windowedthroughput", key
}

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

	// spin up the actual dynamic sampler
	d.dynsampler = &dynsampler.WindowedThroughput{
		GoalThroughputPerSec:      float64(d.goalthroughputpersec),
		UpdateFrequencyDuration:   time.Duration(d.updatefrequency),
		LookbackFrequencyDuration: time.Duration(d.lookbackfrequency),
		MaxKeys:                   d.maxKeys,
	}
	d.dynsampler.Start()

	// Register statistics this package will produce
	d.Metrics.Register("dynsampler_num_dropped", "counter")
	d.Metrics.Register("dynsampler_num_kept", "counter")
	d.Metrics.Register("dynsampler_sample_rate", "histogram")

	return nil
}

func (d *WindowedThroughputSampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool, reason string, key string) {
	key = d.key.build(trace)
	rate = uint(d.dynsampler.GetSampleRate(key))
	if rate < 1 { // protect against dynsampler being broken even though it shouldn't be
		rate = 1
	}
	shouldKeep := rand.Intn(int(rate)) == 0
	d.Logger.Debug().WithFields(map[string]interface{}{
		"sample_key":  key,
		"sample_rate": rate,
		"sample_keep": shouldKeep,
		"trace_id":    trace.TraceID,
	}).Logf("got sample rate and decision")
	if shouldKeep {
		d.Metrics.Increment("dynsampler_num_kept")
	} else {
		d.Metrics.Increment("dynsampler_num_dropped")
	}
	d.Metrics.Histogram("dynsampler_sample_rate", float64(rate))
	return rate, shouldKeep, "Windowedthroughput", key
}

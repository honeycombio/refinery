package sample

import (
	"math/rand"

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

	sampleRate        int64
	clearFrequencySec int64
	configName        string

	key *traceKey

	dynsampler dynsampler.Sampler
}

func (d *DynamicSampler) Start() error {
	d.Logger.Debug().Logf("Starting DynamicSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting DynamicSampler") }()
	d.sampleRate = d.Config.SampleRate
	if d.Config.ClearFrequencySec == 0 {
		d.Config.ClearFrequencySec = 30
	}
	d.clearFrequencySec = d.Config.ClearFrequencySec
	d.key = newTraceKey(d.Config.FieldList, d.Config.UseTraceLength, d.Config.AddSampleRateKeyToTrace, d.Config.AddSampleRateKeyToTraceField)

	// spin up the actual dynamic sampler
	d.dynsampler = &dynsampler.AvgSampleRate{
		GoalSampleRate:    int(d.sampleRate),
		ClearFrequencySec: int(d.clearFrequencySec),
	}
	d.dynsampler.Start()

	// Register stastics this package will produce
	d.Metrics.Register("dynsampler_num_dropped", "counter")
	d.Metrics.Register("dynsampler_num_kept", "counter")
	d.Metrics.Register("dynsampler_sample_rate", "histogram")

	return nil
}

func (d *DynamicSampler) GetSampleRate(trace *types.Trace) (uint, bool) {
	key := d.key.buildAndAdd(trace)
	rate := d.dynsampler.GetSampleRate(key)
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
	return uint(rate), shouldKeep
}

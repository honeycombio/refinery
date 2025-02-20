package sample

import (
	"crypto/sha1"
	"encoding/binary"
	"math"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

// shardingSalt is a random bit to make sure we don't shard the same as any
// other sharding that uses the trace ID (eg deterministic sharding)
const shardingSalt = "5VQ8l2jE5aJLPVqk"

type DeterministicSampler struct {
	Config  *config.DeterministicSamplerConfig
	Logger  logger.Logger
	Metrics metrics.Metrics

	sampleRate int
	upperBound uint32
	prefix     string
}

func (d *DeterministicSampler) Start() error {
	d.Logger.Debug().Logf("Starting DeterministicSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting DeterministicSampler") }()
	d.sampleRate = d.Config.SampleRate
	d.prefix = "deterministic"
	if d.Metrics == nil {
		d.Metrics = &metrics.NullMetrics{}
	}

	for _, metric := range samplerMetrics {
		metric.Name = d.prefix + metric.Name
		d.Metrics.Register(metric)
	}

	// Get the actual upper bound - the largest possible value divided by
	// the sample rate. In the case where the sample rate is 1, this should
	// sample every value.
	d.upperBound = math.MaxUint32 / uint32(d.sampleRate)

	return nil
}

func (d *DeterministicSampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool, reason string, key string, sampler Sampler) {
	if d.sampleRate <= 1 {
		return 1, true, "deterministic/always", "", d
	}
	sum := sha1.Sum([]byte(trace.TraceID + shardingSalt))
	v := binary.BigEndian.Uint32(sum[:4])
	shouldKeep := v <= d.upperBound
	if shouldKeep {
		d.Metrics.Increment(d.prefix + "_num_kept")
	} else {
		d.Metrics.Increment(d.prefix + "_num_dropped")
	}

	return uint(d.sampleRate), shouldKeep, "deterministic/chance", "", d
}

func (d *DeterministicSampler) GetKeyFields() []string {
	return d.Config.GetSamplingFields()
}

func (d *DeterministicSampler) CountLateSpan() {}

// Make sure it implements Sampler
var _ Sampler = (*DeterministicSampler)(nil)
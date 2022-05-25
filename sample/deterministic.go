package sample

import (
	"crypto/sha1"
	"encoding/binary"
	"math"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/types"
)

// shardingSalt is a random bit to make sure we don't shard the same as any
// other sharding that uses the trace ID (eg deterministic sharding)
const shardingSalt = "5VQ8l2jE5aJLPVqk"

type DeterministicSampler struct {
	Config *config.DeterministicSamplerConfig
	Logger logger.Logger

	sampleRate int
	upperBound uint32
}

func (d *DeterministicSampler) Start() error {
	d.Logger.Debug().Logf("Starting DeterministicSampler")
	defer func() { d.Logger.Debug().Logf("Finished starting DeterministicSampler") }()
	d.sampleRate = d.Config.SampleRate

	// Get the actual upper bound - the largest possible value divided by
	// the sample rate. In the case where the sample rate is 1, this should
	// sample every value.
	d.upperBound = math.MaxUint32 / uint32(d.sampleRate)

	return nil
}

func (d *DeterministicSampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool) {
	if d.sampleRate <= 1 {
		return 1, true
	}
	sum := sha1.Sum([]byte(trace.TraceID + shardingSalt))
	v := binary.BigEndian.Uint32(sum[:4])
	return uint(d.sampleRate), v <= d.upperBound
}

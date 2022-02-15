// +build all race

package sample

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/types"
)

// TestInitialization tests that sample rates are consistently returned
func TestInitialization(t *testing.T) {
	ds := &DeterministicSampler{
		Config: &config.DeterministicSamplerConfig{
			SampleRate: 10,
		},
		Logger: &logger.NullLogger{},
	}

	err := ds.Start()
	assert.NoError(t, err, "starting deterministic sampler should not error")

	assert.Equal(t, 10, ds.sampleRate, "upper bound should be correctly calculated")
	assert.Equal(t, uint32(math.MaxUint32/10), ds.upperBound, "upper bound should be correctly calculated")
}

// TestGetSampleRate verifies the same trace ID gets the same response
func TestGetSampleRate(t *testing.T) {
	ds := &DeterministicSampler{
		Config: &config.DeterministicSamplerConfig{
			SampleRate: 10,
		},
		Logger: &logger.NullLogger{},
	}

	tsts := []struct {
		trace   *types.Trace
		sampled bool
	}{
		{&types.Trace{TraceID: "abc123"}, false},
		{&types.Trace{TraceID: "def456"}, true},
		{&types.Trace{TraceID: "ghi789"}, false},
		{&types.Trace{TraceID: "zyx987"}, false},
		{&types.Trace{TraceID: "wvu654"}, false},
		{&types.Trace{TraceID: "tsr321"}, false},
	}
	ds.Start()

	for i, tst := range tsts {
		rate, keep := ds.GetSampleRate(tst.trace)
		assert.Equal(t, uint(10), rate, "sample rate should be fixed")
		assert.Equal(t, tst.sampled, keep, "%d: trace ID %s should be %v", i, tst.trace.TraceID, tst.sampled)
	}

}

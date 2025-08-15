package sample

import (
	"fmt"
	"math"
	"sync"
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
		rate, keep, reason, key := ds.GetSampleRate(tst.trace)
		assert.Equal(t, uint(10), rate, "sample rate should be fixed")
		assert.Equal(t, tst.sampled, keep, "%d: trace ID %s should be %v", i, tst.trace.TraceID, tst.sampled)
		assert.Equal(t, "deterministic/chance", reason)
		assert.Equal(t, "", key)
	}

}

// TestDeterministicSamplerConcurrency tests that GetSampleRate is safe to call concurrently
func TestDeterministicSamplerConcurrency(t *testing.T) {
	ds := &DeterministicSampler{
		Config: &config.DeterministicSamplerConfig{
			SampleRate: 10,
		},
		Logger: &logger.NullLogger{},
	}

	err := ds.Start()
	assert.NoError(t, err)

	// Test traces with known outcomes
	testTraces := []*types.Trace{
		{TraceID: "abc123"}, // false
		{TraceID: "def456"}, // true
		{TraceID: "ghi789"}, // false
	}
	expectedResults := []bool{false, true, false}

	const numGoroutines = 10
	const iterationsPerGoroutine = 100

	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < iterationsPerGoroutine; j++ {
				// Test known traces
				for idx, trace := range testTraces {
					rate, keep, reason, key := ds.GetSampleRate(trace)
					assert.Equal(t, uint(10), rate)
					assert.Equal(t, expectedResults[idx], keep)
					assert.Equal(t, "deterministic/chance", reason)
					assert.Equal(t, "", key)
				}

				// Test random trace for deterministic behavior
				randomTraceID := fmt.Sprintf("random-trace-%d-%d", goroutineID, j)
				randomTrace := &types.Trace{TraceID: randomTraceID}

				rate1, keep1, reason1, key1 := ds.GetSampleRate(randomTrace)
				rate2, keep2, reason2, key2 := ds.GetSampleRate(randomTrace)

				assert.Equal(t, uint(10), rate1)
				assert.Equal(t, "deterministic/chance", reason1)
				assert.Equal(t, "", key1)
				assert.Equal(t, rate1, rate2)
				assert.Equal(t, keep1, keep2)
				assert.Equal(t, reason1, reason2)
				assert.Equal(t, key1, key2)
			}
		}(i)
	}

	wg.Wait()

	// Test sample rate 1 case (always sample)
	dsAlways := &DeterministicSampler{
		Config: &config.DeterministicSamplerConfig{
			SampleRate: 1,
		},
		Logger: &logger.NullLogger{},
	}

	err = dsAlways.Start()
	assert.NoError(t, err)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < 50; j++ {
				trace := &types.Trace{TraceID: fmt.Sprintf("always-trace-%d-%d", goroutineID, j)}
				rate, keep, reason, key := dsAlways.GetSampleRate(trace)

				assert.Equal(t, uint(1), rate)
				assert.True(t, keep)
				assert.Equal(t, "deterministic/always", reason)
				assert.Equal(t, "", key)
			}
		}(i)
	}

	wg.Wait()
}

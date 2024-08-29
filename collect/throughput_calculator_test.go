package collect

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEMAThroughputCalculator(t *testing.T) {
	fakeClock := clockwork.NewFakeClock()

	weight := 0.5
	intervalLength := time.Second
	throughputLimit := 100
	calculator := &EMAThroughputCalculator{
		Clock:           fakeClock,
		Config:          &config.MockConfig{},
		Pubsub:          &pubsub.LocalPubSub{},
		Peer:            &peer.MockPeers{},
		throughputLimit: float64(throughputLimit),
		weight:          weight,
		intervalLength:  intervalLength,
		done:            make(chan struct{}),
		hostID:          "test-host",
		throughputs:     make(map[string]throughputReport),
	}
	calculator.Pubsub.Start()
	defer calculator.Pubsub.Stop()

	calculator.IncrementEventCount(150)

	calculator.updateEMA()
	// check that the EMA was updated correctly
	expectedThroughput := float64(150) / intervalLength.Seconds()
	// starting lastEMA is 0
	expectedEMA := weight*expectedThroughput + (1-weight)*0
	calculator.mut.RLock()
	require.Equal(t, uint(expectedEMA), calculator.lastEMA, "EMA calculation is incorrect")
	require.Equal(t, 0, calculator.eventCount, "event count is not reset after EMA calculation")
	calculator.mut.RUnlock()

	multiplier := calculator.GetSamplingRateMultiplier()
	assert.Equal(t, 1.0, multiplier, "Sampling rate multiplier is incorrect")

	calculator.IncrementEventCount(300)

	calculator.updateEMA()
	newThroughput := float64(300) / intervalLength.Seconds()
	expectedEMA = math.Ceil(weight*newThroughput + (1-weight)*expectedEMA)
	calculator.mut.RLock()
	assert.Equal(t, uint(expectedEMA), calculator.lastEMA, "EMA calculation after second interval is incorrect")
	require.Equal(t, 0, calculator.eventCount, "event count is not reset after EMA calculation")
	calculator.mut.RUnlock()

	multiplier = calculator.GetSamplingRateMultiplier()
	assert.Equal(t, 1.88, multiplier, "Sampling rate multiplier should be 1 when throughput is within the limit")
}

func TestEMAThroughputCalculator_Concurrent(t *testing.T) {
	fakeClock := clockwork.NewFakeClock()

	weight := 0.5
	intervalLength := time.Second
	throughputLimit := 100

	calculator := &EMAThroughputCalculator{
		Clock:           fakeClock,
		Config:          &config.MockConfig{},
		Pubsub:          &pubsub.LocalPubSub{},
		Peer:            &peer.MockPeers{},
		throughputLimit: float64(throughputLimit),
		weight:          weight,
		intervalLength:  intervalLength,
	}
	calculator.Pubsub.Start()
	defer calculator.Pubsub.Stop()
	calculator.Start()
	defer calculator.Stop()

	numGoroutines := 10
	incrementsPerGoroutine := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsPerGoroutine; j++ {
				calculator.IncrementEventCount(1)
			}
			fakeClock.Advance(intervalLength)
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsPerGoroutine; j++ {
				rate := calculator.GetSamplingRateMultiplier()
				assert.GreaterOrEqual(t, rate, 1.0)
			}
		}()
	}
	wg.Wait()
}

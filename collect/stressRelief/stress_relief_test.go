package stressRelief

import (
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/gossip"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/redis"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

// TestStressRelief_Monitor tests that the Stressed method returns the correct value
// for a given metrics data.
func TestStressRelief_Monitor(t *testing.T) {
	// Create a new StressRelief object
	metric := &metrics.MockMetrics{}
	metric.Start()
	defer metric.Stop()
	clock := clockwork.NewFakeClock()

	db := &redis.DefaultClient{
		Config: &config.MockConfig{
			GetRedisHostVal: "localhost:6379",
		},
		Metrics: metric,
	}
	channel := &gossip.GossipRedis{
		Redis: db,
	}
	sr := StressRelief{
		Gossip:          channel,
		Clock:           clock,
		Logger:          &logger.NullLogger{},
		RefineryMetrics: metric,
	}

	require.NoError(t, db.Start())
	defer db.Stop()
	require.NoError(t, channel.Start())
	defer channel.Stop()

	require.NoError(t, sr.Start())
	defer sr.Stop()
	sr.RefineryMetrics.Register("collector_incoming_queue_length", "gauge")

	sr.RefineryMetrics.Store("INCOMING_CAP", 1200)

	cfg := config.StressReliefConfig{
		Mode:                      "monitor",
		ActivationLevel:           80,
		DeactivationLevel:         50,
		SamplingRate:              2,
		MinimumActivationDuration: config.Duration(5 * time.Second),
		MinimumStartupDuration:    config.Duration(time.Second),
	}

	// On startup, the stress relief should not be active
	sr.UpdateFromConfig(cfg)
	require.False(t, sr.Stressed())

	// Test 1
	sr.RefineryMetrics.Gauge("collector_incoming_queue_length", 100)
	require.Eventually(t, func() bool {
		clock.Advance(time.Second * 6)
		return !sr.Stressed()
	}, 2*time.Second, 100*time.Millisecond, "stress relief should be false")

	// Test 2
	// Set activation level to 80 and the current stress level to be more than
	// 80. Check that the Stressed method returns true
	sr.RefineryMetrics.Gauge("collector_incoming_queue_length", 1000)
	require.Eventually(t, func() bool {
		clock.Advance(time.Second * 6)
		return sr.Stressed()
	}, 2*time.Second, 100*time.Millisecond, "stress relief should be true")

	sr.RefineryMetrics.Gauge("collector_incoming_queue_length", 100)
	require.Eventually(t, func() bool {
		clock.Advance(time.Second * 6)
		return !sr.Stressed()
	}, 2*time.Second, 100*time.Millisecond, "stress relief should be false")
}

// TestStressRelief_Sample tests that traces are sampled deterministically
// by traceID.
// The test generates 10000 traceIDs and checks that the sampling rate is
// within 10% of the expected value.
func TestStressRelief_ShouldSampleDeterministically(t *testing.T) {
	traceIDs := make([]string, 0, 100000)
	for i := 0; i < 100; i++ {
		traceIDs = append(traceIDs, strconv.Itoa(rand.Intn(1000000)))
	}

	sr := &StressRelief{
		overallStressLevel: 90,
		activateLevel:      80,
	}

	var sampled int
	var dropped int
	var sampledTraceID string
	var droppedTraceID string
	for _, traceID := range traceIDs {
		if sr.ShouldSampleDeterministically(traceID) {
			sampled++
			if sampledTraceID == "" {
				sampledTraceID = traceID
			}
		} else {
			if droppedTraceID == "" {
				droppedTraceID = traceID
			}
			dropped++
		}
	}

	difference := float64(sampled)/float64(100)*100 - float64(sr.deterministicFraction())
	require.LessOrEqual(t, math.Floor(math.Abs(float64(difference))), float64(10), sampled)

	require.True(t, sr.ShouldSampleDeterministically(sampledTraceID))
	require.False(t, sr.ShouldSampleDeterministically(droppedTraceID))
}

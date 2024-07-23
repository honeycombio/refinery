package collect

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

// TestStressRelief_Monitor tests that the Stressed method returns the correct value
// for a given metrics data.
func TestStressRelief_Monitor(t *testing.T) {
	clock := clockwork.NewFakeClock()
	sr, stop := newStressRelief(t, clock, nil)
	defer stop()
	require.NoError(t, sr.Start())

	sr.RefineryMetrics.Register("collector_incoming_queue_length", "gauge")

	sr.RefineryMetrics.Store("INCOMING_CAP", 1200)

	cfg := config.StressReliefConfig{
		Mode:                      "monitor",
		ActivationLevel:           80,
		DeactivationLevel:         50,
		SamplingRate:              2,
		MinimumActivationDuration: config.Duration(5 * time.Second),
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

// TestStressRelief_Peer tests stress relief activation and deactivation
// based on the stress level of a peer.
func TestStressRelief_Peer(t *testing.T) {
	clock := clockwork.NewFakeClock()
	metric := &metrics.MockMetrics{}
	metric.Start()
	channel := &pubsub.LocalPubSub{
		Metrics: metric,
	}
	require.NoError(t, channel.Start())

	sr, stop := newStressRelief(t, clock, channel)
	defer stop()
	require.NoError(t, sr.Start())

	sr.RefineryMetrics.Register("collector_incoming_queue_length", "gauge")

	sr.RefineryMetrics.Store("INCOMING_CAP", 1200)

	cfg := config.StressReliefConfig{
		Mode:                      "monitor",
		ActivationLevel:           80,
		DeactivationLevel:         65,
		SamplingRate:              2,
		MinimumActivationDuration: config.Duration(5 * time.Second),
	}

	// On startup, the stress relief should not be active
	sr.UpdateFromConfig(cfg)
	require.False(t, sr.Stressed())

	// activate stress relief in one refinery
	sr.RefineryMetrics.Gauge("collector_incoming_queue_length", 965)
	require.Eventually(t, func() bool {
		clock.Advance(time.Second * 1)
		return sr.Stressed()
	}, 2*time.Second, 100*time.Millisecond, "stress relief should be false")

	require.Eventually(t, func() bool {
		// pretend another refinery just started up
		msg := stressReliefMessage{
			level:  90,
			peerID: "peer1",
		}
		require.NoError(t, channel.Publish(context.Background(), stressReliefTopic, msg.String()))
		clock.Advance(time.Second * 1)
		return sr.Stressed()
	}, 2*time.Second, 100*time.Millisecond, "stress relief should be false")

	// now the peer has reported valid stress level
	// it should be taken into account for the overall stress level
	require.Eventually(t, func() bool {
		msg := stressReliefMessage{
			level:  10,
			peerID: "peer1",
		}
		require.NoError(t, channel.Publish(context.Background(), stressReliefTopic, msg.String()))

		clock.Advance(time.Second * 1)
		return !sr.Stressed()
	}, 2*time.Second, 100*time.Millisecond, "stress relief should be false")
}

// TestStressRelief_Sample tests that traces are sampled deterministically
// by traceID.
// The test generates 10000 traceIDs and checks that the sampling rate is
// within 10% of the expected value.
func TestStressRelief_ShouldSampleDeterministically(t *testing.T) {
	traceCount := 10000
	traceIDs := make([]string, 0, traceCount)
	for i := 0; i < traceCount; i++ {
		traceIDs = append(traceIDs, fmt.Sprintf("%016x%016x", rand.Int63(), rand.Int63()))
	}

	sr := &StressRelief{
		overallStressLevel: 90,
		activateLevel:      60,
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

	difference := float64(sampled)/float64(traceCount)*100 - float64(sr.deterministicFraction())
	require.LessOrEqual(t, math.Floor(math.Abs(float64(difference))), float64(10), sampled)

	// make sure that the same traceID always gets the same result
	require.True(t, sr.ShouldSampleDeterministically(sampledTraceID))
	require.False(t, sr.ShouldSampleDeterministically(droppedTraceID))
}

func newStressRelief(t *testing.T, clock clockwork.Clock, channel pubsub.PubSub) (*StressRelief, func()) {
	// Create a new StressRelief object
	metric := &metrics.MockMetrics{}
	metric.Start()

	if clock == nil {
		clock = clockwork.NewRealClock()
	}

	if channel == nil {
		channel = &pubsub.LocalPubSub{
			Metrics: metric,
		}
	}
	require.NoError(t, channel.Start())
	logger := &logger.NullLogger{}
	healthReporter := &health.Health{
		Clock:   clock,
		Metrics: metric,
		Logger:  logger,
	}
	require.NoError(t, healthReporter.Start())

	peer := &peer.MockPeers{}
	require.NoError(t, peer.Start())

	sr := &StressRelief{
		Clock:           clock,
		Logger:          logger,
		RefineryMetrics: metric,
		PubSub:          channel,
		Health:          healthReporter,
		Peer:            peer,
	}

	return sr, func() {
		require.NoError(t, healthReporter.Stop())
		require.NoError(t, peer.Stop())
		require.NoError(t, channel.Stop())
	}
}

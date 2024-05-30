package stressRelief

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/gossip"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/redis"
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

// TestStressRelief_Peer tests stress relief activation and deactivation
// based on the stress level of a peer.
func TestStressRelief_Peer(t *testing.T) {
	clock := clockwork.NewFakeClock()
	channel := &gossip.InMemoryGossip{}

	sr, stop := newStressRelief(t, clock, channel)
	defer stop()
	require.NoError(t, sr.Start())
	defer sr.Stop()

	sr.RefineryMetrics.Register("collector_incoming_queue_length", "gauge")

	sr.RefineryMetrics.Store("INCOMING_CAP", 1200)

	cfg := config.StressReliefConfig{
		Mode:                      "monitor",
		ActivationLevel:           80,
		DeactivationLevel:         65,
		SamplingRate:              2,
		MinimumActivationDuration: config.Duration(5 * time.Second),
		MinimumStartupDuration:    config.Duration(time.Second),
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

	// when a peer just started up, it should not affect the stress level of the
	// cluster overall stress level
	peer2 := &peer.MockPeerStore{
		Identification: "peer2",
		PeerStore: peer.PeerStore{
			Gossip: channel,
			Clock:  clock,
		},
	}
	peer2.Start()
	defer peer2.Stop()

	require.Eventually(t, func() bool {
		// pretend another refinery just started up
		msg := peer.PeerInfo{
			Data: peerMessageToBytes(0),
		}
		require.NoError(t, peer2.PublishPeerInfo(msg))
		clock.Advance(time.Second * 1)
		return sr.Stressed()
	}, 2*time.Second, 100*time.Millisecond, "stress relief should be false")

	// now the peer has reported valid stress level
	// it should be taken into account for the overall stress level
	require.Eventually(t, func() bool {
		msg := peer.PeerInfo{
			Data: peerMessageToBytes(5),
		}
		require.NoError(t, peer2.PublishPeerInfo(msg))

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

func newStressRelief(t *testing.T, clock clockwork.Clock, channel gossip.Gossiper) (*StressRelief, func()) {
	// Create a new StressRelief object
	metric := &metrics.MockMetrics{}
	metric.Start()

	if clock == nil {
		clock = clockwork.NewRealClock()
	}

	db := &redis.DefaultClient{
		Config: &config.MockConfig{
			GetRedisHostVal: "localhost:6379",
		},
		Metrics: metric,
	}
	require.NoError(t, db.Start())

	healthCheck := &health.Health{
		Clock: clock,
	}
	require.NoError(t, healthCheck.Start())
	if channel == nil {
		channel = &gossip.GossipRedis{
			Redis:  db,
			Health: healthCheck,
		}
	}
	peers := &peer.PeerStore{
		Gossip: channel,
		Clock:  clock,
	}
	require.NoError(t, channel.Start())
	require.NoError(t, peers.Start())
	sr := &StressRelief{
		Peer:            peers,
		Clock:           clock,
		Logger:          &logger.NullLogger{},
		RefineryMetrics: metric,
		Health:          healthCheck,
	}

	return sr, func() {
		require.NoError(t, peers.Stop())
		require.NoError(t, channel.Stop())
		require.NoError(t, db.Stop())
		require.NoError(t, metric.Stop())
		require.NoError(t, healthCheck.Stop())
	}
}

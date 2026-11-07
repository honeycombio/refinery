package collect

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/honeycombio/refinery/pubsub/pubsubtest"
)

// TestStressRelief_Monitor tests that the Stressed method returns the correct value
// for a given metrics data.
func TestStressRelief_Monitor(t *testing.T) {
	clock := clockwork.NewFakeClock()
	sr, stop := newStressRelief(t, clock, nil)
	defer stop()
	require.NoError(t, sr.Start())

	sr.RefineryMetrics.Register(metrics.Metadata{
		Name: NUMERATOR_INCOMING_QUEUE,
		Type: metrics.Gauge,
	})

	sr.RefineryMetrics.Store(DENOMINATOR_INCOMING_CAP, 1200)
	sr.Config = &config.MockConfig{
		StressRelief: config.StressReliefConfig{
			Mode:                      "monitor",
			ActivationLevel:           80,
			DeactivationLevel:         50,
			SamplingRate:              2,
			MinimumActivationDuration: config.Duration(5 * time.Second),
		},
	}

	// On startup, the stress relief should not be active
	sr.UpdateFromConfig()
	require.False(t, sr.Stressed())

	// Test 1
	sr.RefineryMetrics.Gauge(NUMERATOR_INCOMING_QUEUE, 100)
	require.Eventually(t, func() bool {
		clock.Advance(time.Second * 6)
		return !sr.Stressed()
	}, 2*time.Second, 100*time.Millisecond, "stress relief should be false")

	// Test 2
	// Set activation level to 80 and the current stress level to be more than
	// 80. Check that the Stressed method returns true
	sr.RefineryMetrics.Gauge(NUMERATOR_INCOMING_QUEUE, 1000)
	require.Eventually(t, func() bool {
		clock.Advance(time.Second * 6)
		return sr.Stressed()
	}, 2*time.Second, 100*time.Millisecond, "stress relief should be true")

	sr.RefineryMetrics.Gauge(NUMERATOR_INCOMING_QUEUE, 100)
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

	sr.RefineryMetrics.Register(metrics.Metadata{
		Name: NUMERATOR_INCOMING_QUEUE,
		Type: metrics.Gauge,
	})

	sr.RefineryMetrics.Store(DENOMINATOR_INCOMING_CAP, 1200)
	sr.Config = &config.MockConfig{
		StressRelief: config.StressReliefConfig{
			Mode:                      "monitor",
			ActivationLevel:           80,
			DeactivationLevel:         65,
			SamplingRate:              2,
			MinimumActivationDuration: config.Duration(5 * time.Second),
		},
	}
	// On startup, the stress relief should not be active
	sr.UpdateFromConfig()
	require.False(t, sr.Stressed())

	// activate stress relief in one refinery
	sr.RefineryMetrics.Gauge(NUMERATOR_INCOMING_QUEUE, 965)
	require.Eventually(t, func() bool {
		clock.Advance(time.Second * 1)
		return sr.Stressed()
	}, 2*time.Second, 100*time.Millisecond, "stress relief should be active")

	require.Eventually(t, func() bool {
		// pretend another refinery just started up
		msg := stressReliefMessage{
			level:  10,
			peerID: "peer1",
		}
		require.NoError(t, channel.Publish(context.Background(), stressReliefTopic, msg.String()))
		clock.Advance(time.Second * 1)
		return sr.Stressed()
	}, 2*time.Second, 100*time.Millisecond, "stress relief should be remain activated")

	// now the peer has reported valid stress level
	// it should be taken into account for the overall stress level
	sr.RefineryMetrics.Gauge(NUMERATOR_INCOMING_QUEUE, 5)
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

// TestStressRelief_OverallStressLevel tests overall stress level calculation across multiple peers
func TestStressRelief_OverallStressLevel(t *testing.T) {
	clock := clockwork.NewFakeClock()
	sr, stop := newStressRelief(t, clock, nil)
	defer stop()

	// disable the automatic stress level recalculation
	sr.disableStressLevelReport = true
	sr.Start()

	sr.RefineryMetrics.Register(metrics.Metadata{
		Name: NUMERATOR_INCOMING_QUEUE,
		Type: metrics.Gauge,
	})

	sr.RefineryMetrics.Store(DENOMINATOR_INCOMING_CAP, 1200)

	sr.Config = &config.MockConfig{
		StressRelief: config.StressReliefConfig{
			Mode:                      "monitor",
			ActivationLevel:           80,
			DeactivationLevel:         65,
			MinimumActivationDuration: config.Duration(5 * time.Second),
		},
	}
	// On startup, the stress relief should not be active
	sr.UpdateFromConfig()
	require.False(t, sr.Stressed())

	// Test 1
	// when a single peer's individual stress level is above the activation level
	// the overall stress level should be above the activation level
	// and the stress relief should be active
	sr.RefineryMetrics.Gauge(NUMERATOR_INCOMING_QUEUE, 965)
	clock.Advance(time.Second * 1)
	sr.stressLevels = make(map[string]stressReport, 100)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("peer%d", i)
		sr.stressLevels[key] = stressReport{
			key:       key,
			level:     10,
			timestamp: sr.Clock.Now(),
		}
	}

	localLevel := sr.Recalc()
	require.Equal(t, localLevel, sr.overallStressLevel)
	require.True(t, sr.stressed)

	// Test 2
	// when a single peer's individual stress level is below the activation level
	// and the rest of the cluster is above the activation level
	// the single peer should remain in stress relief mode
	sr.RefineryMetrics.Gauge(NUMERATOR_INCOMING_QUEUE, 10)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("peer%d", i)
		sr.stressLevels[key] = stressReport{
			key:       key,
			level:     85,
			timestamp: sr.Clock.Now(),
		}
	}
	localLevel = sr.Recalc()
	require.Greater(t, sr.overallStressLevel, localLevel)
	require.True(t, sr.stressed)

	// Test 3
	// Only when both the single peer's individual stress level and the cluster stress
	// level is below the activation level, the stress relief should be deactivated.
	sr.RefineryMetrics.Gauge(NUMERATOR_INCOMING_QUEUE, 10)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("peer%d", i)
		sr.stressLevels[key] = stressReport{
			key:       key,
			level:     1,
			timestamp: sr.Clock.Now(),
		}
	}
	clock.Advance(sr.minDuration * 2)
	localLevel = sr.Recalc()
	assert.Equal(t, sr.overallStressLevel, localLevel)
	assert.False(t, sr.stressed)
}

// TestStressRelief_StaysAliveWhenPublishStalls covers a Redis outage: a stress
// level publish that never returns must not mark stress relief dead and fail
// the liveness check.
func TestStressRelief_StaysAliveWhenPublishStalls(t *testing.T) {
	clock := clockwork.NewFakeClock()
	metric := &metrics.MockMetrics{}
	metric.Start()
	channel := &pubsubtest.OutagePubSub{
		LocalPubSub:       pubsub.LocalPubSub{Metrics: metric},
		HangsPastDeadline: true,
		Blocked:           make(chan struct{}, 1),
	}
	channel.Down()
	sr, stop := newStressRelief(t, clock, channel)
	defer stop()
	defer channel.Up()
	sr.Config = &config.MockConfig{
		StressRelief: config.StressReliefConfig{Mode: "never"}, // never is default, we still publish stress even if there is no action
	}
	sr.UpdateFromConfig()
	require.NoError(t, sr.Start())

	healthReporter := sr.Health.(*health.Health)
	require.Eventually(t, func() bool {
		clock.Advance(100 * time.Millisecond)
		select {
		case <-channel.Blocked:
			return true
		default:
			return false
		}
	}, 2*time.Second, time.Millisecond, "stress relief should publish its level")
	require.True(t, healthReporter.IsAlive())

	// Run the fake clock well past stress relief's 15s health timeout.
	for range 2 * 15 * time.Second / health.TickerTime {
		clock.Advance(health.TickerTime)
		time.Sleep(time.Millisecond)
	}
	assert.True(t, healthReporter.IsAlive(), "a stalled publish should not fail liveness")
}

// registrationAgePeers is a peer.Peers whose registration age the test sets.
type registrationAgePeers struct {
	peer.MockPeers
	age  atomic.Int64 // nanoseconds
	seen atomic.Bool
}

func (p *registrationAgePeers) RegistrationAge() (time.Duration, bool) {
	return time.Duration(p.age.Load()), p.seen.Load()
}

// TestStressRelief_StalePeerRegistration tests what stress relief does when
// the host has not seen its own peer registration for too long: it activates
// in monitor and always modes, labels its sampling decisions with the reason,
// logs when the state changes, and leaves never mode alone.
func TestStressRelief_StalePeerRegistration(t *testing.T) {
	for _, tc := range []struct {
		mode       string
		wantStress bool
		wantReason string
	}{
		{mode: "monitor", wantStress: true, wantReason: "stress_relief/deterministic/RedisUnreachable"},
		{mode: "always", wantStress: true, wantReason: "stress_relief/deterministic/RedisUnreachable"},
		{mode: "never", wantStress: false},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			clock := clockwork.NewFakeClock()
			peers := &registrationAgePeers{}
			sr, stop := newPeerStressRelief(t, clock, tc.mode, peers)
			defer stop()
			logs := &logger.MockLogger{}
			sr.Logger = logs

			// A fresh registration is not stress.
			peers.seen.Store(true)
			peers.age.Store(int64(time.Second))
			sr.Recalc()
			assert.Equal(t, tc.mode == "always", sr.Stressed())

			// A registration older than the limit is.
			peers.age.Store(int64(peerRegistrationMaxAge + time.Second))
			sr.Recalc()
			assert.Equal(t, tc.wantStress, sr.Stressed())
			if tc.wantStress {
				_, _, reason := sr.GetSampleRate("a-trace")
				assert.Equal(t, tc.wantReason, reason)
			}
			assert.True(t, hasWarning(logs, "peers are stale"), "every mode logs the state change")

			// The same state is not logged again on every tick.
			before := len(warnings(logs))
			sr.Recalc()
			assert.Len(t, warnings(logs), before)

			// A fresh registration ends it, after the minimum activation time.
			peers.age.Store(0)
			clock.Advance(6 * time.Second)
			sr.Recalc()
			assert.Equal(t, tc.mode == "always", sr.Stressed())
			assert.True(t, hasWarning(logs, "peers are fresh again"))
		})
	}
}

// TestStressRelief_ActivatesBeforePeerListShrinks tests that during a Redis
// outage a host activates stress relief no later than the first time its peer
// list loses a peer. Peers expire PeerEntryTimeout after their last message, so
// until stress relief is on, the host decides traces as if it had fewer shards
// than the cluster.
func TestStressRelief_ActivatesBeforePeerListShrinks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := clockwork.NewRealClock()
		ps := &pubsubtest.OutagePubSub{}
		require.NoError(t, ps.Start())
		t.Cleanup(ps.Close)

		hosts := make([]*peer.RedisPubsubPeers, 3)
		for i := range hosts {
			h := &health.Health{Clock: clock, Metrics: &metrics.NullMetrics{}, Logger: &logger.NullLogger{}}
			require.NoError(t, h.Start())
			hosts[i] = &peer.RedisPubsubPeers{
				Config: &config.MockConfig{
					GetPeerListenAddrVal: "127.0.0.1:3443",
					RedisIdentifier:      fmt.Sprintf("host%d", i),
					PeerTimeout:          5 * time.Second,
				},
				PubSub:     ps,
				Metrics:    &metrics.NullMetrics{},
				Logger:     &logger.NullLogger{},
				Clock:      clock,
				Health:     h,
				InstanceID: fmt.Sprintf("host%d", i),
				Done:       make(chan struct{}),
			}
			require.NoError(t, hosts[i].Start())
			require.NoError(t, hosts[i].Ready())
			t.Cleanup(func() {
				close(hosts[i].Done)
				require.NoError(t, h.Stop())
			})
		}
		host := hosts[0]

		sr, stop := newPeerStressRelief(t, clockwork.NewFakeClock(), "monitor", host)
		t.Cleanup(stop)

		time.Sleep(10 * time.Second)
		synctest.Wait()
		healthy, err := host.GetPeers()
		require.NoError(t, err)
		require.Len(t, healthy, len(hosts))
		sr.Recalc()
		require.False(t, sr.Stressed(), "a cluster that can reach Redis is not stressed")

		ps.Down()
		t.Cleanup(ps.Up)
		for elapsed := time.Second; elapsed <= 2*peer.PeerEntryTimeout; elapsed += time.Second {
			time.Sleep(time.Second)
			synctest.Wait()
			sr.Recalc()
			current, err := host.GetPeers()
			require.NoError(t, err)
			if len(current) < len(healthy) {
				assert.True(t, sr.Stressed(),
					"the peer list shrank to %d after %s without Redis, before stress relief activated", len(current), elapsed)
				return
			}
		}
		t.Fatal("the peer list never shrank, so this test does not exercise an outage")
	})
}

// TestStressRelief_StaysOnAfterPeersAreFresh tests that in monitor mode stress
// relief stays on for MinimumActivationDuration after the last tick that found
// the peers stale. The peers can be stale for only the tick that activates it,
// or for a long time. Either way, the peer list may still be filling again when
// the registration is fresh, so it must not turn off at once.
func TestStressRelief_StaysOnAfterPeersAreFresh(t *testing.T) {
	for name, staleFor := range map[string]time.Duration{
		"stale for one tick": 0,
		"stale for a while":  10 * time.Second,
	} {
		t.Run(name, func(t *testing.T) {
			clock := clockwork.NewFakeClock()
			peers := &registrationAgePeers{}
			sr, stop := newPeerStressRelief(t, clock, "monitor", peers)
			defer stop()

			peers.seen.Store(true)
			peers.age.Store(int64(peerRegistrationMaxAge + time.Second))
			sr.Recalc()
			require.True(t, sr.Stressed())
			if staleFor > 0 {
				clock.Advance(staleFor)
				sr.Recalc()
				require.True(t, sr.Stressed())
			}

			// The last tick that found the peers stale was just now.
			peers.age.Store(0)
			sr.Recalc()
			assert.True(t, sr.Stressed(), "fresh peers do not end stress relief at once")
			clock.Advance(4 * time.Second)
			sr.Recalc()
			assert.True(t, sr.Stressed(), "still within the minimum activation time")
			clock.Advance(2 * time.Second)
			sr.Recalc()
			assert.False(t, sr.Stressed(), "the minimum activation time has passed")
		})
	}
}

// TestStressRelief_NoRegistrationIsNotStale tests that a host that has not
// yet seen its own registration, and peers that cannot report one, are never
// treated as stale: such a host has no peer list to lose.
func TestStressRelief_NoRegistrationIsNotStale(t *testing.T) {
	for name, peers := range map[string]peer.Peers{
		"not seen yet":   &registrationAgePeers{},
		"no age to give": &peer.MockPeers{},
	} {
		t.Run(name, func(t *testing.T) {
			sr, stop := newPeerStressRelief(t, clockwork.NewFakeClock(), "monitor", peers)
			defer stop()
			sr.Recalc()
			assert.False(t, sr.Stressed())
		})
	}
}

// hasWarning reports whether any warning message starts with prefix.
func hasWarning(l *logger.MockLogger, prefix string) bool {
	for _, w := range warnings(l) {
		if strings.HasPrefix(w, prefix) {
			return true
		}
	}
	return false
}

// warnings returns the warning messages that the logger has seen.
func warnings(l *logger.MockLogger) []string {
	var out []string
	for _, e := range l.Events {
		if w, ok := e.Fields["warn"].(string); ok {
			out = append(out, w)
		}
	}
	return out
}

// newPeerStressRelief returns a started StressRelief in the given mode that
// reads its registration age from peers, and that the test drives with Recalc
// alone. It activates at 80, deactivates at 50 and stays on for at least 5
// seconds of the fake clock. The returned function stops it.
func newPeerStressRelief(t *testing.T, clock clockwork.Clock, mode string, peers peer.Peers) (*StressRelief, func()) {
	sr, stop := newStressRelief(t, clock, nil)
	sr.Peer = peers
	// Call Recalc from the test only, not also from the monitor loop.
	sr.disableStressLevelReport = true
	sr.Config = &config.MockConfig{
		StressRelief: config.StressReliefConfig{
			Mode:                      mode,
			ActivationLevel:           80,
			DeactivationLevel:         50,
			SamplingRate:              2,
			MinimumActivationDuration: config.Duration(5 * time.Second),
		},
	}
	sr.UpdateFromConfig()
	require.NoError(t, sr.Start())
	return sr, stop
}

func newStressRelief(t *testing.T, clock clockwork.Clock, channel pubsub.PubSub) (*StressRelief, func()) {
	// Create a new StressRelief object
	metric := &metrics.MockMetrics{}
	metric.Start()

	if clock == nil {
		clock = clockwork.NewFakeClock()
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
		Done:            make(chan struct{}),
		Logger:          logger,
		RefineryMetrics: metric,
		PubSub:          channel,
		Health:          healthReporter,
		Peer:            peer,
	}

	return sr, func() {
		close(sr.Done)
		require.NoError(t, healthReporter.Stop())
		require.NoError(t, channel.Stop())
	}
}

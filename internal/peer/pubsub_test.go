package peer

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/honeycombio/refinery/pubsub/pubsubtest"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_publicAddr(t *testing.T) {
	cfg := &config.MockConfig{
		GetPeerListenAddrVal:    "127.0.0.1:3443",
		RedisIdentifier:         "somehostname",
		IdentifierInterfaceName: "en0",
	}
	tests := []struct {
		name    string
		c       config.Config
		want    string
		wantErr bool
	}{
		{"basic", cfg, "http://somehostname:3443", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peers := &RedisPubsubPeers{
				Config: tt.c,
				Logger: &logger.NullLogger{},
			}
			got, err := publicAddr(peers.Logger, peers.Config)
			if (err != nil) != tt.wantErr {
				t.Errorf("publicAddr() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("publicAddr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPeerActions(t *testing.T) {
	cmd := newPeerCommand(Register, "foo", "12345bar")
	assert.Equal(t, "Rfoo,12345bar", cmd.marshal())
	assert.Equal(t, "foo", cmd.address)
	assert.Equal(t, "12345bar", cmd.id)
	assert.Equal(t, Register, cmd.action)
	cmd2 := peerCommand{}
	b := cmd2.unmarshal("Ubar,12345foo")
	assert.True(t, b)
	assert.Equal(t, "bar", cmd2.address)
	assert.Equal(t, "12345foo", cmd2.id)
	assert.Equal(t, Unregister, cmd2.action)

	b = cmd2.unmarshal("Rfoo")
	assert.False(t, b)
}

// TestRedisPubsubPeers_RegistrationAge tests that a host reports no age until
// it has seen its own registration come back through the peers channel, and
// that only its own registration resets the age afterwards.
func TestRedisPubsubPeers_RegistrationAge(t *testing.T) {
	clock := clockwork.NewFakeClock()
	ps := &pubsub.LocalPubSub{
		Config: &config.MockConfig{},
	}
	require.NoError(t, ps.Start())
	defer ps.Stop()

	healthRecorder := &health.Health{Clock: clock, Metrics: &metrics.NullMetrics{}, Logger: &logger.NullLogger{}}
	require.NoError(t, healthRecorder.Start())
	defer healthRecorder.Stop()

	peers := &RedisPubsubPeers{
		Config: &config.MockConfig{
			GetPeerListenAddrVal: "127.0.0.1:3443",
			RedisIdentifier:      "somehostname",
		},
		PubSub:     ps,
		Clock:      clock,
		Health:     healthRecorder,
		InstanceID: "me",
		Done:       make(chan struct{}),
	}
	require.NoError(t, peers.Start())

	_, seen := peers.RegistrationAge()
	assert.False(t, seen, "no registration has come back yet")

	peers.listen(context.Background(), newPeerCommand(Register, "http://other:3443", "other").marshal())
	_, seen = peers.RegistrationAge()
	assert.False(t, seen, "another host's registration is not ours")

	peers.listen(context.Background(), newPeerCommand(Register, "http://somehostname:3443", "me").marshal())
	age, seen := peers.RegistrationAge()
	assert.True(t, seen)
	assert.Equal(t, time.Duration(0), age)

	clock.Advance(40 * time.Second)
	age, seen = peers.RegistrationAge()
	assert.True(t, seen, "losing contact must not look like never having had it")
	assert.Equal(t, 40*time.Second, age)

	peers.listen(context.Background(), newPeerCommand(Register, "http://other:3443", "other").marshal())
	age, _ = peers.RegistrationAge()
	assert.Equal(t, 40*time.Second, age, "another host's registration does not reset the age")

	peers.listen(context.Background(), newPeerCommand(Register, "http://somehostname:3443", "me").marshal())
	age, _ = peers.RegistrationAge()
	assert.Equal(t, time.Duration(0), age)
}

// TestRedisPubsubPeers_ReadyAfterFirstRegistration tests that a host is not
// ready until its own registration has come back through the peers channel,
// and that it then stays ready and alive when Redis stops answering.
func TestRedisPubsubPeers_ReadyAfterFirstRegistration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := clockwork.NewRealClock()
		healthRecorder := &health.Health{Clock: clock, Metrics: &metrics.NullMetrics{}, Logger: &logger.NullLogger{}}
		require.NoError(t, healthRecorder.Start())
		t.Cleanup(func() { require.NoError(t, healthRecorder.Stop()) })

		// Add an always-ready component to health to show the awaited readiness later is dependent on peers.
		healthRecorder.Register("im-always-ready", time.Hour)
		healthRecorder.Ready("im-always-ready", true)

		cfg := &config.MockConfig{
			GetPeerListenAddrVal: "127.0.0.1:3443",
			RedisIdentifier:      "somehostname",
			PeerTimeout:          5 * time.Second,
		}
		sadPubSub := &pubsubtest.OutagePubSub{HangsPastDeadline: true}
		require.NoError(t, sadPubSub.Start())
		t.Cleanup(sadPubSub.Close)
		sadPubSub.Down()

		peers := &RedisPubsubPeers{
			Config:     cfg,
			PubSub:     sadPubSub,
			Clock:      clock,
			Health:     healthRecorder,
			InstanceID: "me",
			Done:       make(chan struct{}),
		}
		require.NoError(t, peers.Start())
		require.NoError(t, peers.Ready())
		// The peers' publishes must end before the pubsub closes.
		t.Cleanup(func() {
			sadPubSub.Up()
			close(peers.Done)
			synctest.Wait()
		})

		time.Sleep(10 * time.Second)
		synctest.Wait()
		assert.True(t, healthRecorder.IsAlive(), "a host without a registration is alive")
		assert.False(t, healthRecorder.IsReady(), "a host that has not seen its registration is not ready")

		sadPubSub.Up()
		time.Sleep(10 * time.Second)
		synctest.Wait()
		assert.True(t, healthRecorder.IsReady(), "a host that has seen its registration is ready")

		sadPubSub.Down()
		time.Sleep(time.Minute)
		synctest.Wait()
		assert.True(t, healthRecorder.IsAlive(), "a publish that hangs must not fail liveness")
		assert.True(t, healthRecorder.IsReady(), "a host that has been ready stays ready when Redis stops answering")
	})
}

// TestRedisPubsubPeers_RegistrationAgeGauge tests that peers report how long
// ago the host saw its own registration, in milliseconds, once a second. The
// gauge is absent until the host has seen one registration: a missing value
// does not look like a fresh registration.
func TestRedisPubsubPeers_RegistrationAgeGauge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := clockwork.NewRealClock()
		metric := &metrics.MockMetrics{}
		metric.Start()
		healthRecorder := &health.Health{Clock: clock, Metrics: &metrics.NullMetrics{}, Logger: &logger.NullLogger{}}
		require.NoError(t, healthRecorder.Start())
		t.Cleanup(func() { require.NoError(t, healthRecorder.Stop()) })

		cfg := &config.MockConfig{
			GetPeerListenAddrVal: "127.0.0.1:3443",
			RedisIdentifier:      "somehostname",
			PeerTimeout:          5 * time.Second,
		}
		sadPubSub := &pubsubtest.OutagePubSub{HangsPastDeadline: true}
		require.NoError(t, sadPubSub.Start())
		t.Cleanup(sadPubSub.Close)
		sadPubSub.Down()

		peers := &RedisPubsubPeers{
			Config:     cfg,
			PubSub:     sadPubSub,
			Metrics:    metric,
			Clock:      clock,
			Health:     healthRecorder,
			InstanceID: "me",
			Done:       make(chan struct{}),
		}
		require.NoError(t, peers.Start())
		require.NoError(t, peers.Ready())
		t.Cleanup(func() {
			sadPubSub.Up()
			close(peers.Done)
			synctest.Wait()
		})

		time.Sleep(10 * time.Second)
		synctest.Wait()
		_, found := metric.Get("peer_registration_age_ms")
		assert.False(t, found, "no gauge until the host has seen its registration")

		peers.listen(context.Background(), newPeerCommand(Register, "http://somehostname:3443", "me").marshal())
		time.Sleep(1500 * time.Millisecond)
		synctest.Wait()
		age, found := metric.Get("peer_registration_age_ms")
		require.True(t, found)
		assert.Equal(t, float64(1000), age, "the last report was one second after the registration")

		time.Sleep(4 * time.Second)
		synctest.Wait()
		age, _ = metric.Get("peer_registration_age_ms")
		assert.Equal(t, float64(5000), age, "the age grows while the registration does not come back")

		peers.listen(context.Background(), newPeerCommand(Register, "http://somehostname:3443", "me").marshal())
		time.Sleep(time.Second)
		synctest.Wait()
		age, _ = metric.Get("peer_registration_age_ms")
		assert.Equal(t, float64(500), age, "a new registration resets the age; the next report is half a second after it")
	})
}

// TestRedisPubsubPeers_ConcurrentRegistrations tests that registrations can
// arrive on several goroutines at once, as they do from a pubsub that runs
// each callback on its own goroutine. Run it with the race detector.
func TestRedisPubsubPeers_ConcurrentRegistrations(t *testing.T) {
	ps := &pubsub.LocalPubSub{Config: &config.MockConfig{}}
	require.NoError(t, ps.Start())
	defer ps.Stop()

	clock := clockwork.NewFakeClock()
	healthRecorder := &health.Health{Clock: clock, Metrics: &metrics.NullMetrics{}, Logger: &logger.NullLogger{}}
	require.NoError(t, healthRecorder.Start())
	defer healthRecorder.Stop()

	peers := &RedisPubsubPeers{
		Config: &config.MockConfig{
			GetPeerListenAddrVal: "127.0.0.1:3443",
			RedisIdentifier:      "somehostname",
		},
		PubSub:     ps,
		Clock:      clock,
		Health:     healthRecorder,
		InstanceID: "me",
		Done:       make(chan struct{}),
	}
	require.NoError(t, peers.Start())

	// A registered callback makes the list-change path run for every change.
	peers.RegisterUpdatedPeersCallback(func() {})

	const registrations = 20
	var wg sync.WaitGroup
	for i := range registrations {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("host-%d", i)
			peers.listen(context.Background(), newPeerCommand(Register, "http://"+id+":3443", id).marshal())
		}()
	}
	wg.Wait()

	got, err := peers.GetPeers()
	require.NoError(t, err)
	assert.Len(t, got, registrations+1, "every registration is in the list, plus this host")
}

package peer

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
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

// register sends peers a registration from the host with the given ID, as if
// it came through the subscription.
func register(peers *RedisPubsubPeers, id string) {
	peers.listen(context.Background(), newPeerCommand(Register, "http://"+id+":3443", id).marshal())
}

// registerSelf sends peers their own registration, as if it came back through
// the subscription.
func registerSelf(t *testing.T, peers *RedisPubsubPeers) {
	addr, err := publicAddr(peers.Logger, peers.Config)
	require.NoError(t, err)
	peers.listen(context.Background(), newPeerCommand(Register, addr, peers.InstanceID).marshal())
}

// assertFresh asserts what peers report from PeersAreFresh.
func assertFresh(t *testing.T, peers *RedisPubsubPeers, wantFresh, wantEverFresh bool, msg string) {
	t.Helper()
	fresh, everFresh := peers.PeersAreFresh()
	assert.Equal(t, wantFresh, fresh, "fresh: "+msg)
	assert.Equal(t, wantEverFresh, everFresh, "ever fresh: "+msg)
}

// TestRedisPubsubPeers_PeersAreFresh tests that a host reports its peer list as
// fresh only after it has seen its own registration and peer messages have
// arrived for peersFreshDelay, and that after a long gap the list is fresh
// again at the first peer message.
func TestRedisPubsubPeers_PeersAreFresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The pubsub is down, so the only peer messages are the ones the test sends.
		peers, _, _ := newHungPeers(t, nil, nil)
		assertFresh(t, peers, false, false, "before any peer message")

		register(peers, "a")
		for range 3 {
			time.Sleep(3 * time.Second)
			register(peers, "a")
		}
		assertFresh(t, peers, false, false, "past the delay, but without its own registration")

		time.Sleep(time.Second)
		registerSelf(t, peers)
		assertFresh(t, peers, true, true, "its own registration, past the delay")

		time.Sleep(peerMessageGapLimit)
		assertFresh(t, peers, true, true, "a gap as long as the limit")

		time.Sleep(time.Second)
		assertFresh(t, peers, false, true, "a gap longer than the limit")

		// No entry expired during the gap, so there is nothing to wait for.
		register(peers, "a")
		assertFresh(t, peers, true, true, "the first message after a long gap")
	})
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

// newHungPeers returns started, ready peers on a pubsub whose Publish hangs
// until the test calls Up on it. It also returns the health recorder the peers
// report to. A nil metric or log gets the default of the peers. Call it inside
// a synctest bubble: its cleanup waits for the peers to stop there.
func newHungPeers(t *testing.T, metric metrics.Metrics, log logger.Logger) (*RedisPubsubPeers, *health.Health, *pubsubtest.OutagePubSub) {
	clock := clockwork.NewRealClock()
	healthRecorder := &health.Health{Clock: clock, Metrics: &metrics.NullMetrics{}, Logger: &logger.NullLogger{}}
	require.NoError(t, healthRecorder.Start())
	t.Cleanup(func() { require.NoError(t, healthRecorder.Stop()) })

	sadPubSub := &pubsubtest.OutagePubSub{HangsPastDeadline: true}
	require.NoError(t, sadPubSub.Start())
	t.Cleanup(sadPubSub.Close)
	sadPubSub.Down()

	peers := &RedisPubsubPeers{
		Config: &config.MockConfig{
			GetPeerListenAddrVal: "127.0.0.1:3443",
			RedisIdentifier:      "somehostname",
			PeerTimeout:          5 * time.Second,
		},
		PubSub:     sadPubSub,
		Metrics:    metric,
		Logger:     log,
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
	return peers, healthRecorder, sadPubSub
}

// TestRedisPubsubPeers_ReadyOncePeersAreFresh tests that a host is not ready
// until its peer list has been fresh, and that it then stays ready and alive
// when Redis stops answering.
func TestRedisPubsubPeers_ReadyOncePeersAreFresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, healthRecorder, sadPubSub := newHungPeers(t, nil, nil)

		// Add an always-ready component to health to show the awaited readiness later is dependent on peers.
		healthRecorder.Register("im-always-ready", time.Hour)
		healthRecorder.Ready("im-always-ready", true)

		time.Sleep(10 * time.Second)
		synctest.Wait()
		assert.True(t, healthRecorder.IsAlive(), "a host without a registration is alive")
		assert.False(t, healthRecorder.IsReady(), "a host that has not seen its registration is not ready")

		sadPubSub.Up()
		time.Sleep(5 * time.Second)
		synctest.Wait()
		assert.False(t, healthRecorder.IsReady(), "a host that has seen its registration is not ready before its peers had time to register")

		time.Sleep(10 * time.Second)
		synctest.Wait()
		assert.True(t, healthRecorder.IsReady(), "a host whose peer list is fresh is ready")

		sadPubSub.Down()
		time.Sleep(time.Minute)
		synctest.Wait()
		assert.True(t, healthRecorder.IsAlive(), "a publish that hangs must not fail liveness")
		assert.True(t, healthRecorder.IsReady(), "a host that has been ready stays ready when Redis stops answering")
	})
}

// TestRedisPubsubPeers_WarnsWhenNoRegistrationReturns tests that a host that
// has not seen its own registration 30 seconds after it started says so once,
// and names what can cause it. Such a host stays not ready, and the only other
// sign is the readiness check. Redis that is down, a user that is not allowed
// to subscribe and a server without pub/sub all look the same from here.
func TestRedisPubsubPeers_WarnsWhenNoRegistrationReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &logger.MockLogger{}
		newHungPeers(t, nil, logs)

		time.Sleep(29 * time.Second)
		synctest.Wait()
		assert.Empty(t, warnMessages(logs), "no warning before 30 seconds")

		time.Sleep(2 * time.Second)
		synctest.Wait()
		warns := warnMessages(logs)
		require.Len(t, warns, 1)
		assert.Contains(t, warns[0], "has not seen its own registration")
		assert.Contains(t, warns[0], "PUBLISH and SUBSCRIBE", "the message names what to check")
		assert.Contains(t, warns[0], "credentials", "a wrong AuthCode no longer stops Start, so the message names it")

		time.Sleep(time.Minute)
		synctest.Wait()
		assert.Len(t, warnMessages(logs), 1, "the warning is not repeated")
	})
}

// warnMessages returns the warning messages that the logger has seen.
func warnMessages(l *logger.MockLogger) []string {
	var out []string
	for _, e := range l.GetEvents() {
		if w, ok := e.Fields["warn"].(string); ok {
			out = append(out, w)
		}
	}
	return out
}

// TestRedisPubsubPeers_PeersFreshGauge tests that peers report once a second
// whether the peer list is fresh, from the start of the host, and report
// num_peers before the list is fresh.
func TestRedisPubsubPeers_PeersFreshGauge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		metric := &metrics.MockMetrics{}
		metric.Start()
		peers, _, _ := newHungPeers(t, metric, nil)

		time.Sleep(1500 * time.Millisecond)
		synctest.Wait()
		value, found := metric.Get("peers_fresh")
		require.True(t, found, "the gauge exists before the first peer message")
		assert.Equal(t, float64(0), value)

		registerSelf(t, peers)
		_, found = metric.Get("num_peers")
		assert.True(t, found, "a host whose list is not fresh yet still reports its peer count")
		for range 4 {
			time.Sleep(2 * time.Second)
			registerSelf(t, peers)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		value, _ = metric.Get("peers_fresh")
		assert.Equal(t, float64(1), value, "the list is fresh")

		time.Sleep(peerMessageGapLimit + time.Second)
		synctest.Wait()
		value, _ = metric.Get("peers_fresh")
		assert.Equal(t, float64(0), value, "a long gap makes the list not fresh")
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

	// The list must have been fresh, or checkHash does not run the callbacks.
	for range 3 {
		registerSelf(t, peers)
		clock.Advance(maxRefreshCacheInterval)
	}
	registerSelf(t, peers)
	_, everFresh := peers.PeersAreFresh()
	require.True(t, everFresh)

	var callbacks atomic.Int64
	peers.RegisterUpdatedPeersCallback(func() { callbacks.Add(1) })

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
	assert.Eventually(t, func() bool { return callbacks.Load() > 0 }, time.Second, time.Millisecond, "the registrations changed the list")
}

// newOutagePeers returns started, ready peers named id on ps, a pubsub that
// the test takes down and up. Call it inside a synctest bubble: its cleanup
// waits for the peers to stop there.
func newOutagePeers(t *testing.T, ps *pubsubtest.OutagePubSub, id string) *RedisPubsubPeers {
	clock := clockwork.NewRealClock()
	healthRecorder := &health.Health{Clock: clock, Metrics: &metrics.NullMetrics{}, Logger: &logger.NullLogger{}}
	require.NoError(t, healthRecorder.Start())
	t.Cleanup(func() { require.NoError(t, healthRecorder.Stop()) })

	peers := &RedisPubsubPeers{
		Config: &config.MockConfig{
			GetPeerListenAddrVal: "127.0.0.1:3443",
			RedisIdentifier:      id,
			PeerTimeout:          5 * time.Second,
		},
		PubSub:     ps,
		Clock:      clock,
		Health:     healthRecorder,
		InstanceID: id,
		Done:       make(chan struct{}),
	}
	require.NoError(t, peers.Start())
	require.NoError(t, peers.Ready())
	// The peers' publishes must end before the pubsub closes.
	t.Cleanup(func() {
		ps.Up()
		close(peers.Done)
		synctest.Wait()
	})
	return peers
}

// recordPeerLists registers a callback on peers that records the list
// GetPeers returns each time the peers change. The returned function gives
// the lists recorded so far.
func recordPeerLists(t *testing.T, peers *RedisPubsubPeers) func() [][]string {
	var mut sync.Mutex
	var lists [][]string
	peers.RegisterUpdatedPeersCallback(func() {
		list, err := peers.GetPeers()
		assert.NoError(t, err)
		mut.Lock()
		defer mut.Unlock()
		lists = append(lists, list)
	})
	return func() [][]string {
		mut.Lock()
		defer mut.Unlock()
		return slices.Clone(lists)
	}
}

// TestRedisPubsubPeers_OutageKeepsLivePeers tests that after a Redis outage
// longer than PeerEntryTimeout, the sharder never gets a list without a live
// host: a host that started during the outage is added when it registers, and
// a host that stopped is dropped PeerEntryTimeout of contact time later.
func TestRedisPubsubPeers_OutageKeepsLivePeers(t *testing.T) {
	const outage = PeerEntryTimeout + 500*time.Millisecond
	cluster := []string{"http://host0:3443", "http://host1:3443", "http://host2:3443"}
	for _, tc := range []struct {
		name  string
		stops bool // a host registers before the outage and never again
		joins bool // a host starts during the outage
		want  [][]string
	}{
		{name: "no change", want: [][]string{}},
		{name: "a host stops", stops: true, want: [][]string{cluster}},
		{name: "a host joins", joins: true, want: [][]string{append(slices.Clone(cluster), "http://host3:3443")}},
		{name: "a host stops and a host joins", stops: true, joins: true, want: [][]string{
			append([]string{"http://stopped:3443"}, append(slices.Clone(cluster), "http://host3:3443")...),
			append(slices.Clone(cluster), "http://host3:3443"),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ps := &pubsubtest.OutagePubSub{}
				require.NoError(t, ps.Start())
				t.Cleanup(ps.Close)
				host0 := newOutagePeers(t, ps, "host0")
				newOutagePeers(t, ps, "host1")
				newOutagePeers(t, ps, "host2")
				lists := recordPeerLists(t, host0)

				time.Sleep(15 * time.Second)
				fresh, _ := host0.PeersAreFresh()
				require.True(t, fresh, "the cluster has formed")
				if tc.stops {
					// Only host0 hears this host, so only host0 has to drop it.
					host0.listen(context.Background(), newPeerCommand(Register, "http://stopped:3443", "a-stopped-host").marshal())
				}
				synctest.Wait()
				before := len(lists())

				ps.Down()
				time.Sleep(outage / 2)
				if tc.joins {
					newOutagePeers(t, ps, "host3")
				}
				time.Sleep(outage - outage/2)
				ps.Up()
				time.Sleep(20 * time.Second)
				synctest.Wait()

				got := lists()[before:]
				assert.Equal(t, tc.want, got, "after the outage the sharder sees only the changes to the cluster")
			})
		})
	}
}

// TestRedisPubsubPeers_HealthyChangesAreNotDelayed tests that a host tells
// the sharder about a new peer at once while peer messages arrive as often as
// a healthy cluster sends them: at least once per refresh interval.
func TestRedisPubsubPeers_HealthyChangesAreNotDelayed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The pubsub is down, so the only peer messages are the ones the test sends.
		peers, _, _ := newHungPeers(t, nil, nil)
		lists := recordPeerLists(t, peers)

		registerSelf(t, peers)
		for range 3 {
			register(peers, "a")
			time.Sleep(maxRefreshCacheInterval)
		}
		synctest.Wait()
		before := len(lists())

		register(peers, "b")
		synctest.Wait()
		got := lists()
		require.Len(t, got, before+1, "a new peer changes the list at once")
		assert.Equal(t, []string{"http://a:3443", "http://b:3443"}, got[before])
	})
}

// TestRedisPubsubPeers_StartingHostChangesPeersOnce tests that a starting host
// tells the sharder about its peers once, with every peer, when the list is
// first fresh.
func TestRedisPubsubPeers_StartingHostChangesPeersOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The pubsub is down, so the only peer messages are the ones the test sends.
		peers, _, _ := newHungPeers(t, nil, nil)
		lists := recordPeerLists(t, peers)

		registerSelf(t, peers)
		register(peers, "a")
		time.Sleep(time.Second)
		register(peers, "b")
		for range 3 {
			time.Sleep(3 * time.Second)
			registerSelf(t, peers)
			register(peers, "a")
			register(peers, "b")
		}
		synctest.Wait()
		assert.Equal(t, [][]string{{"http://a:3443", "http://b:3443", "http://somehostname:3443"}}, lists())
	})
}

// TestRedisPubsubPeers_GetPeersIsOnlySelfUntilFresh tests that until the peer
// list has been fresh, GetPeers returns only this host, so that the sharder of
// a starting host keeps every trace until the first callback gives it the full
// list.
func TestRedisPubsubPeers_GetPeersIsOnlySelfUntilFresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The pubsub is down, so the only peer messages are the ones the test sends.
		peers, _, _ := newHungPeers(t, nil, nil)
		self := "http://somehostname:3443"

		registerSelf(t, peers)
		register(peers, "a")
		got, err := peers.GetPeers()
		require.NoError(t, err)
		assert.Equal(t, []string{self}, got, "a starting host")

		for range 3 {
			time.Sleep(maxRefreshCacheInterval)
			registerSelf(t, peers)
			register(peers, "a")
		}
		_, everFresh := peers.PeersAreFresh()
		require.True(t, everFresh)
		got, err = peers.GetPeers()
		require.NoError(t, err)
		assert.Equal(t, []string{"http://a:3443", self}, got, "the list has been fresh")
	})
}

// TestRedisPubsubPeers_LongGapsDoNotExpirePeers tests that a gap between peer
// messages longer than peerMessageGapLimit does not count toward the expiry
// of entries, and that a shorter gap does.
func TestRedisPubsubPeers_LongGapsDoNotExpirePeers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The pubsub is down, so the only peer messages are the ones the test sends.
		peers, _, _ := newHungPeers(t, nil, nil)
		self := "http://somehostname:3443"
		assertPeers := func(want []string, msg string) {
			t.Helper()
			got, err := peers.GetPeers()
			require.NoError(t, err)
			assert.Equal(t, want, got, msg)
		}
		cluster := []string{"http://a:3443", "http://b:3443", self}

		// b registers last at 9 seconds of contact time, so it expires after 19.
		for range 4 {
			registerSelf(t, peers)
			register(peers, "a")
			register(peers, "b")
			time.Sleep(3 * time.Second)
		}
		time.Sleep(PeerEntryTimeout + 7*time.Second)
		assertPeers(cluster, "a gap of 20 seconds")

		registerSelf(t, peers)
		register(peers, "a")
		assertPeers(cluster, "the first message after a gap of 20 seconds")

		// The gap of peerMessageGapLimit counts: 14 seconds of contact time.
		time.Sleep(peerMessageGapLimit)
		registerSelf(t, peers)
		register(peers, "a")
		time.Sleep(3 * time.Second)
		registerSelf(t, peers)
		register(peers, "a")
		assertPeers(cluster, "17 seconds of contact time")

		time.Sleep(3 * time.Second)
		registerSelf(t, peers)
		register(peers, "a")
		assertPeers([]string{"http://a:3443", self}, "20 seconds of contact time")
	})
}

// TestRedisPubsubPeers_PeerSurvivesALostRegistration tests that a peer whose
// next registration is lost stays in the list, even when the registration
// before came after a gap: its entry expires PeerEntryTimeout of contact time
// after that registration arrived, gap included.
func TestRedisPubsubPeers_PeerSurvivesALostRegistration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The pubsub is down, so the only peer messages are the ones the test sends.
		peers, _, _ := newHungPeers(t, nil, nil)
		for range 4 {
			registerSelf(t, peers)
			register(peers, "a")
			time.Sleep(3 * time.Second)
		}

		time.Sleep(1500 * time.Millisecond)
		register(peers, "a")
		// The next registration from a is lost; the one after comes two
		// intervals later.
		for range 2 {
			time.Sleep(maxRefreshCacheInterval)
			registerSelf(t, peers)
		}
		got, err := peers.GetPeers()
		require.NoError(t, err)
		assert.Equal(t, []string{"http://a:3443", "http://somehostname:3443"}, got)
	})
}

// TestRedisPubsubPeers_DropsItselfWhenItCannotPublish tests that a host whose
// publishes fail while it still hears its peers drops itself from its list,
// as its peers drop it.
func TestRedisPubsubPeers_DropsItselfWhenItCannotPublish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		peers, _, sadPubSub := newHungPeers(t, nil, nil)
		lists := recordPeerLists(t, peers)

		sadPubSub.Up()
		time.Sleep(4 * time.Second)
		synctest.Wait()

		sadPubSub.Down()
		for range 9 {
			register(peers, "a")
			register(peers, "b")
			time.Sleep(refreshCacheInterval)
		}
		synctest.Wait()
		got := lists()
		require.NotEmpty(t, got)
		assert.Equal(t, []string{"http://a:3443", "http://b:3443"}, got[len(got)-1], "the host is no longer in its own list")
		assertFresh(t, peers, true, true, "the list matches what its peers have")
	})
}

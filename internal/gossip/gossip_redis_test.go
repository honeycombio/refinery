package gossip

import (
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/redis"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoundTripChanRedis(t *testing.T) {
	cfg := config.MockConfig{
		GetRedisHostVal: "localhost:6379",
	}
	metric := &metrics.MockMetrics{}
	metric.Start()
	defer metric.Stop()
	redis := &redis.DefaultClient{
		Config:  &cfg,
		Metrics: metric,
	}
	require.NoError(t, redis.Start())
	defer redis.Stop()
	healthCheck := &health.Health{
		Clock: clockwork.NewRealClock(),
	}
	require.NoError(t, healthCheck.Start())
	defer healthCheck.Stop()

	g := &GossipRedis{
		Redis:  redis,
		Logger: &logger.NullLogger{},
		Health: healthCheck,
	}

	require.NoError(t, g.Start())
	chTest := g.GetChannel("test")
	chTest2 := g.GetChannel("test2")
	chJunk := g.GetChannel("junk")

	ch := g.Subscribe(chTest, 10)
	require.NotNil(t, ch)

	ch2 := g.Subscribe(chTest2, 10)
	require.NotNil(t, ch2)

	// deflaking -- pause to give subscriptions time to be set up
	time.Sleep(100 * time.Millisecond)
	// publish a junk message to get health check running
	require.NoError(t, g.Publish(chJunk, []byte("nevermind")))

	// deflaking -- pause to give subscriptions time to be set up
	// don't continue until we say we're ready
	assert.Eventually(t, func() bool {
		return healthCheck.IsReady()
	}, 5*time.Second, 200*time.Millisecond)

	// Test that we can publish messages
	require.NoError(t, g.Publish(chTest, []byte("hi")))
	require.NoError(t, g.Publish(chTest2, []byte("bye")))

	require.Eventually(t, func() bool {
		time.Sleep(100 * time.Millisecond)
		return len(ch) == 1 && len(ch2) == 1
	}, 5*time.Second, 200*time.Millisecond)

	select {
	case hi := <-ch:
		require.Equal(t, "hi", string(hi))
	default:
		t.Fatal("expected to receive a message on channel 'test'")
	}

	select {
	case bye := <-ch2:
		require.Equal(t, "bye", string(bye))
	default:
		t.Fatal("expected to receive a message on channel 'test2'")
	}

	require.NoError(t, g.Stop())
}

func TestRoundTripChanInMem(t *testing.T) {
	g := &InMemoryGossip{}

	require.NoError(t, g.Start())

	chTest := g.GetChannel("test")
	chTest2 := g.GetChannel("test2")

	ch := g.Subscribe(chTest, 10)
	require.NotNil(t, ch)

	ch2 := g.Subscribe(chTest2, 10)
	require.NotNil(t, ch2)

	// Test that we can publish a message
	require.NoError(t, g.Publish(chTest, []byte("hi")))
	require.NoError(t, g.Publish(chTest2, []byte("bye")))

	assert.Eventually(t, func() bool {
		time.Sleep(100 * time.Millisecond)
		return len(ch) == 1 && len(ch2) == 1
	}, 5*time.Second, 200*time.Millisecond)

	select {
	case hi := <-ch:
		require.Equal(t, "hi", string(hi))
	default:
		t.Fatal("expected to receive a message on channel 'test'")
	}

	select {
	case bye := <-ch2:
		require.Equal(t, "bye", string(bye))
	default:
		t.Fatal("expected to receive a message on channel 'test2'")
	}

	require.NoError(t, g.Stop())
}

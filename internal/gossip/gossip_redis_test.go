package gossip

import (
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/redis"
	"github.com/stretchr/testify/require"
)

func TestRoundTrip(t *testing.T) {
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
	g := &GossipRedis{
		Redis:  redis,
		Logger: &logger.NullLogger{},
	}

	require.NoError(t, g.Start())

	// Test that we can register a handler
	require.NoError(t, g.Subscribe("test"))

	// Test that we can publish a message
	require.NoError(t, g.Publish("test", []byte("hi")))

	// Test that we can receive a message
	require.Eventually(t, func() bool {
		msg := g.Receive()
		return "hi" == string(msg)
	}, 3*time.Second, 100*time.Millisecond)

	require.NoError(t, g.Stop())
}

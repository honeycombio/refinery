package peer

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

func newPeers(c config.Config) (Peers, error) {
	var peers Peers
	var pubsubber pubsub.PubSub
	ptype := c.GetPeerManagementType()
	switch ptype {
	case "file":
		peers = &FilePeers{
			Cfg:     c,
			Metrics: &metrics.NullMetrics{},
		}
		// we know FilePeers doesn't need to be Started, so as long as we gave it a Cfg above,
		// we can ask it how many peers we have.
		// if we only have one, we can use the local pubsub implementation.
		peerList, err := peers.GetPeers()
		if err != nil {
			return nil, err
		}
		if len(peerList) == 1 {
			pubsubber = &pubsub.LocalPubSub{}
		} else {
			pubsubber = &pubsub.GoRedisPubSub{}
		}
	case "redis":
		pubsubber = &pubsub.GoRedisPubSub{
			Metrics: &metrics.NullMetrics{},
			Tracer:  noop.NewTracerProvider().Tracer("test"),
		}
		peers = &RedisPubsubPeers{}
	default:
		// this should have been caught by validation
		return nil, errors.New("invalid config option 'PeerManagement.Type'")
	}

	// we need to include all the metrics types so we can inject them in case they're needed
	var g inject.Graph
	objects := []*inject.Object{
		{Value: c},
		{Value: peers},
		{Value: pubsubber},
		{Value: &metrics.NullMetrics{}, Name: "metrics"},
		{Value: &logger.NullLogger{}},
		{Value: clockwork.NewFakeClock()},
	}
	err := g.Provide(objects...)
	if err != nil {
		return nil, fmt.Errorf("failed to provide injection graph. error: %+v\n", err)
	}

	if err := g.Populate(); err != nil {
		return nil, fmt.Errorf("failed to populate injection graph. error: %+v\n", err)
	}

	ststLogger := logrus.New()
	ststLogger.SetLevel(logrus.InfoLevel)
	if err := startstop.Start(g.Objects(), ststLogger); err != nil {
		fmt.Printf("failed to start injected dependencies. error: %+v\n", err)
		os.Exit(1)
	}
	return peers, nil
}

func TestPeerShutdown(t *testing.T) {
	c := &config.MockConfig{
		GetPeerListenAddrVal: "0.0.0.0:8081",
		PeerManagementType:   "redis",
		PeerTimeout:          5 * time.Second,
	}

	p, err := newPeers(c)
	require.NoError(t, err)

	done := make(chan struct{})
	require.NotNil(t, p)

	peer, ok := p.(*RedisPubsubPeers)
	assert.True(t, ok)

	peers, err := peer.GetPeers()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(peers))
	assert.True(t, strings.HasPrefix(peers[0], "http"))
	assert.True(t, strings.HasSuffix(peers[0], "8081"))

	close(done)

	assert.Eventually(t, func() bool {
		peers, err = peer.GetPeers()
		assert.NoError(t, err)
		return len(peers) == 1
	}, 5*time.Second, 200*time.Millisecond)
}

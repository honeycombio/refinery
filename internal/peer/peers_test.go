package peer

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPeersFile(t *testing.T) {
	c := &config.MockConfig{
		PeerManagementType: "file",
		PeerTimeout:        5 * time.Second,
		TraceIdFieldNames:  []string{"trace.trace_id"},
		ParentIdFieldNames: []string{"trace.parent_id"},
	}

	done := make(chan struct{})
	defer close(done)
	p, err := NewPeers(context.Background(), c, done)
	assert.NoError(t, err)
	require.NotNil(t, p)

	switch i := p.(type) {
	case *filePeers:
	default:
		t.Errorf("received %T expected %T", i, &filePeers{})
	}
}

func TestNewPeersPubSub(t *testing.T) {
	c := &config.MockConfig{
		GetPeerListenAddrVal:    "0.0.0.0:8081",
		PeerManagementType:      "pubsub",
		PeerTimeout:             5 * time.Second,
		IdentifierInterfaceName: "eth0",
	}

	pubsub := pubsub.LocalPubSub{}
	pubsub.Start()

	done := make(chan struct{})
	defer close(done)
	p, err := NewPeers(context.Background(), c, done)
	assert.NoError(t, err)
	require.NotNil(t, p)
	p.(*pubsubPeers).PubSub = &pubsub
	p.Start()
	defer p.Stop()

	switch i := p.(type) {
	case *pubsubPeers:
	default:
		t.Errorf("received %T expected %T", i, &pubsubPeers{})
	}
}

func TestPeerShutdown(t *testing.T) {
	c := &config.MockConfig{
		GetPeerListenAddrVal: "0.0.0.0:8081",
		PeerManagementType:   "redis",
		PeerTimeout:          5 * time.Second,
	}

	done := make(chan struct{})
	p, err := NewPeers(context.Background(), c, done)
	assert.NoError(t, err)
	require.NotNil(t, p)

	peer, ok := p.(*redisPeers)
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
		return len(peers) == 0
	}, 5*time.Second, 200*time.Millisecond)
}

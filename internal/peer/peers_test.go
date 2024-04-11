package peer

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPeers(t *testing.T) {
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

	c = &config.MockConfig{
		GetPeerListenAddrVal: "0.0.0.0:8081",
		PeerManagementType:   "redis",
		PeerTimeout:          5 * time.Second,
	}

	p, err = NewPeers(context.Background(), c, done)
	assert.NoError(t, err)
	require.NotNil(t, p)

	switch i := p.(type) {
	case *RedisPeers:
	default:
		t.Errorf("received %T expected %T", i, &RedisPeers{})
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

	peer, ok := p.(*RedisPeers)
	assert.True(t, ok)
	peer.Config = c
	peer.RedisClient = &redis.TestService{}
	peer.RedisClient.Start()
	peer.Start()

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

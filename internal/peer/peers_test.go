package peer

import (
	"context"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPeers(t *testing.T) {
	c := &config.MockConfig{
		PeerManagementType: "file",
		PeerTimeout:        5 * time.Second,
	}

	p, err := NewPeers(context.Background(), c)
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

	p, err = NewPeers(context.Background(), c)
	assert.NoError(t, err)
	require.NotNil(t, p)

	switch i := p.(type) {
	case *redisPeers:
	default:
		t.Errorf("received %T expected %T", i, &redisPeers{})
	}
}

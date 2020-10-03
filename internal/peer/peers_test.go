// +build all race

package peer

import (
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/stretchr/testify/assert"
)

func TestNewPeers(t *testing.T) {
	c := &config.MockConfig{
		PeerManagementType: "file",
	}

	p, err := NewPeers(c)

	assert.Equal(t, nil, err)

	switch i := p.(type) {
	case *filePeers:
	default:
		t.Errorf("received %T expected %T", i, &filePeers{})
	}

	c = &config.MockConfig{
		GetPeerListenAddrVal: "0.0.0.0:8081",
		PeerManagementType:   "redis",
	}

	p, err = NewPeers(c)

	assert.Equal(t, nil, err)

	switch i := p.(type) {
	case *redisPeers:
	default:
		t.Errorf("received %T expected %T", i, &redisPeers{})
	}
}

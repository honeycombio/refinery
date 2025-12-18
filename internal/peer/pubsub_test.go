package peer

import (
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/stretchr/testify/assert"
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

func TestPeerDrainingAction(t *testing.T) {
	cmd := newPeerCommand(Draining, "http://10.0.0.1:8081", "abcd1234")
	marshaled := cmd.marshal()
	assert.Equal(t, "Dhttp://10.0.0.1:8081,abcd1234", marshaled)

	cmd2 := &peerCommand{}
	ok := cmd2.unmarshal(marshaled)
	assert.True(t, ok)
	assert.Equal(t, Draining, cmd2.action)
	assert.Equal(t, "http://10.0.0.1:8081", cmd2.address)
	assert.Equal(t, "abcd1234", cmd2.id)
}

func TestMockPeersDraining(t *testing.T) {
	peers := &MockPeers{
		Peers: []string{"http://peer1:8081", "http://peer2:8081"},
		ID:    "http://peer1:8081",
	}

	assert.False(t, peers.IsDraining())

	drainDone := peers.Drain()
	assert.NotNil(t, drainDone)
	assert.True(t, peers.IsDraining())

	drainDone2 := peers.Drain()
	assert.Equal(t, drainDone, drainDone2)
}

func TestFilePeersDraining(t *testing.T) {
	cfg := &config.MockConfig{
		GetPeerListenAddrVal: "127.0.0.1:8081",
		RedisIdentifier:      "testhost",
	}

	peers := &FilePeers{
		Cfg:    cfg,
		Logger: &logger.NullLogger{},
	}

	assert.False(t, peers.IsDraining())

	drainDone := peers.Drain()
	assert.NotNil(t, drainDone)
	assert.True(t, peers.IsDraining())

	drainDone2 := peers.Drain()
	assert.Equal(t, drainDone, drainDone2)
}

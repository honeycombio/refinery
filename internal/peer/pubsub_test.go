package peer

import (
	"context"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/pubsub"
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

// TestRedisPubsubPeers_Connected tests that a host is connected only while it
// keeps seeing its own registration come back through the peers channel.
func TestRedisPubsubPeers_Connected(t *testing.T) {
	clock := clockwork.NewFakeClock()
	ps := &pubsub.LocalPubSub{
		Config: &config.MockConfig{},
	}
	require.NoError(t, ps.Start())
	defer ps.Stop()

	peers := &RedisPubsubPeers{
		Config: &config.MockConfig{
			GetPeerListenAddrVal: "127.0.0.1:3443",
			RedisIdentifier:      "somehostname",
		},
		PubSub:     ps,
		Clock:      clock,
		InstanceID: "me",
		Done:       make(chan struct{}),
	}
	require.NoError(t, peers.Start())
	assert.True(t, peers.Connected(), "grace period starts at Start")

	clock.Advance(unreachableAfter + time.Second)
	assert.False(t, peers.Connected(), "no registration seen within the window")

	// someone else's registration does not count
	peers.listen(context.Background(), newPeerCommand(Register, "http://other:3443", "other").marshal())
	assert.False(t, peers.Connected())

	peers.listen(context.Background(), newPeerCommand(Register, "http://somehostname:3443", "me").marshal())
	assert.True(t, peers.Connected())
}

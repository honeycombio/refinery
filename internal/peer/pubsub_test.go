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

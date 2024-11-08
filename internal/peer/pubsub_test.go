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
			got, err := peers.publicAddr()
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
	cmd := newPeerCommand(Register, "foo", "id1")
	assert.Equal(t, "Rfoo,id1", cmd.marshal())
	assert.Equal(t, "foo", cmd.address)
	assert.Equal(t, Register, cmd.action)
	assert.Equal(t, "id1", cmd.id)
	cmd2 := peerCommand{}
	b := cmd2.unmarshal("Ubar,id2")
	assert.True(t, b)
	assert.Equal(t, "bar", cmd2.address)
	assert.Equal(t, Unregister, cmd2.action)
	assert.Equal(t, "id2", cmd2.id)

	b = cmd2.unmarshal("invalid")
	assert.False(t, b)
}

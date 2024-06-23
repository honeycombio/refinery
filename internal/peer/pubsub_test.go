package peer

import (
	"testing"

	"github.com/honeycombio/refinery/config"
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
			got, err := publicAddr(tt.c)
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

package peer

import (
	"testing"

	"github.com/honeycombio/refinery/config"
)

func TestFilePeers(t *testing.T) {
	peers := []string{"peer"}

	c := &config.MockConfig{
		PeerManagementType:   "file",
		GetPeersVal:          peers,
		GetPeerListenAddrVal: "10.244.0.114:8081",
	}
	p, err := newPeers(c)
	if err != nil {
		t.Error(err)
	}

	if d, _ := p.GetPeers(); !(len(d) == 1 && d[0] == "peer") {
		t.Error("received", d, "expected", "[peer]")
	}
}

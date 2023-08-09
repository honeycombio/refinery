package peer

import "github.com/honeycombio/refinery/config"

type filePeers struct {
	c config.Config
}

// NewFilePeers returns a peers collection backed by the config file
func newFilePeers(c config.Config) Peers {
	return &filePeers{
		c: c,
	}
}

func (p *filePeers) GetPeers() ([]string, error) {
	// we never want to return an empty list of peers, so if the config
	// returns an empty list, return a single peer. This keeps the sharding
	// logic happy.
	peers, err := p.c.GetPeers()
	if len(peers) == 0 {
		peers = []string{"http://127.0.0.1:8081"}
	}
	return peers, err
}

func (p *filePeers) RegisterUpdatedPeersCallback(callback func()) {
	// whenever registered, call the callback immediately
	// otherwise do nothing since they never change
	callback()
}

package peer

import "github.com/honeycombio/refinery/config"

type FilePeers struct {
	Cfg config.Config `inject:""`
}

func (p *FilePeers) GetPeers() ([]string, error) {
	// we never want to return an empty list of peers, so if the config
	// returns an empty list, return a single peer. This keeps the sharding
	// logic happy.
	peers, err := p.Cfg.GetPeers()
	if len(peers) == 0 {
		peers = []string{"http://127.0.0.1:8081"}
	}
	return peers, err
}

func (p *FilePeers) RegisterUpdatedPeersCallback(callback func()) {
	// whenever registered, call the callback immediately
	// otherwise do nothing since they never change
	callback()
}

func (p *FilePeers) Start() error {
	return nil
}

func (p *FilePeers) Stop() error {
	return nil
}

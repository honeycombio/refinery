package peer

import (
	"fmt"
	"net"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/metrics"
)

var _ Peers = &FilePeers{}

type FilePeers struct {
	Cfg     config.Config   `inject:""`
	Metrics metrics.Metrics `inject:"metrics"`

	id string
}

func (p *FilePeers) GetPeers() ([]string, error) {
	// we never want to return an empty list of peers, so if the config
	// returns an empty list, return a single peer. This keeps the sharding
	// logic happy.
	peers, err := p.Cfg.GetPeers()
	if len(peers) == 0 {
		addr, err := p.publicAddr()
		if err != nil {
			return nil, err

		}
		peers = []string{addr}
	}
	p.Metrics.Gauge("num_file_peers", float64(len(peers)))
	return peers, err
}

func (p *FilePeers) GetInstanceID() (string, error) {
	return p.id, nil
}

func (p *FilePeers) RegisterUpdatedPeersCallback(callback func()) {
	// whenever registered, call the callback immediately
	// otherwise do nothing since they never change
	callback()
}

func (p *FilePeers) Start() (err error) {
	p.Metrics.Register("num_file_peers", "gauge")

	p.id, err = p.publicAddr()
	if err != nil {
		return err
	}

	return nil
}

func (p *FilePeers) Stop() error {
	return nil
}

func (p *FilePeers) publicAddr() (string, error) {
	addr, err := p.Cfg.GetPeerListenAddr()
	if err != nil {
		return "", err
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("http://%s:%s", host, port), nil
}

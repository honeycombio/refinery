package peer

import (
	"fmt"
	"net"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/metrics"
)

var _ Peers = (*FilePeers)(nil)

type FilePeers struct {
	Cfg     config.Config   `inject:""`
	Metrics metrics.Metrics `inject:"metrics"`
	Done    chan struct{}

	id        string
	callbacks []func()
}

func (p *FilePeers) GetPeers() ([]string, error) {
	// we never want to return an empty list of peers, so if the config
	// returns an empty list, return a single peer. This keeps the sharding
	// logic happy.
	peers := p.Cfg.GetPeers()
	if len(peers) == 0 {
		addr, err := p.publicAddr()
		if err != nil {
			return nil, err

		}
		peers = []string{addr}
	}
	p.Metrics.Gauge("num_file_peers", float64(len(peers)))
	return peers, nil
}

func (p *FilePeers) GetInstanceID() (string, error) {
	return p.id, nil
}

func (p *FilePeers) RegisterUpdatedPeersCallback(callback func()) {
	// whenever registered, call the callback immediately
	callback()
	p.callbacks = append(p.callbacks, callback)
}

var filePeersMetrics = []metrics.Metadata{
	{Name: "num_file_peers", Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "Number of peers in the file peer list"},
}

func (p *FilePeers) Start() (err error) {
	for _, metric := range filePeersMetrics {
		p.Metrics.Register(metric)
	}

	p.id, err = p.publicAddr()
	if err != nil {
		return err
	}

	hash := hashList(p.Cfg.GetPeers())
	p.callbacks = make([]func(), 0)
	p.Cfg.RegisterReloadCallback(func(configHash, ruleCfgHash string) {
		newhash := hashList(p.Cfg.GetPeers())
		if newhash != hash {
			hash = newhash
			for _, cb := range p.callbacks {
				go cb()
			}
		}
	})

	return nil
}

func (p *FilePeers) Ready() error {
	return nil
}

func (p *FilePeers) publicAddr() (string, error) {
	addr := p.Cfg.GetPeerListenAddr()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("http://%s:%s", host, port), nil
}

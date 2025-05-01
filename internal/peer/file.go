package peer

import (
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
)

var _ Peers = (*FilePeers)(nil)

type FilePeers struct {
	Cfg     config.Config   `inject:""`
	Logger  logger.Logger   `inject:""`
	Metrics metrics.Metrics `inject:"metrics"`
	Done    chan struct{}

	publicAddr string
}

// GetPeers returns the list of peers, including the host itself.
func (p *FilePeers) GetPeers() ([]string, error) {
	addr, err := publicAddr(p.Logger, p.Cfg)
	if err != nil {
		return nil, err
	}
	peers := p.Cfg.GetPeers()
	peers = append(peers, addr)
	p.Metrics.Gauge("num_file_peers", float64(len(peers)))
	return peers, nil
}

func (p *FilePeers) GetInstanceID() (string, error) {
	return p.publicAddr, nil
}

func (p *FilePeers) RegisterUpdatedPeersCallback(callback func()) {
	// whenever registered, call the callback immediately
	// otherwise do nothing since they never change
	callback()
}

var filePeersMetrics = []metrics.Metadata{
	{Name: "num_file_peers", Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "Number of peers in the file peer list"},
}

func (p *FilePeers) Start() (err error) {
	for _, metric := range filePeersMetrics {
		p.Metrics.Register(metric)
	}

	p.publicAddr, err = publicAddr(p.Logger, p.Cfg)
	if err != nil {
		return err
	}

	return nil
}

func (p *FilePeers) Ready() error {
	return nil
}

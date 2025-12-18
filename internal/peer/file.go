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
	callbacks  []func()
	draining   bool
	drainDone  chan struct{}
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

// Drain signals that this peer is entering draining mode.
// For file-based peers, this is a no-op since there's no peer coordination,
// but we implement it to satisfy the interface.
func (p *FilePeers) Drain() <-chan struct{} {
	if p.draining {
		return p.drainDone
	}
	p.draining = true
	p.drainDone = make(chan struct{})
	p.Logger.Info().Logf("entering draining state (file peers - no peer coordination)")
	return p.drainDone
}

// IsDraining returns true if this peer is currently in draining mode.
func (p *FilePeers) IsDraining() bool {
	return p.draining
}

func (p *FilePeers) RegisterUpdatedPeersCallback(callback func()) {
	// whenever registered, call the callback immediately
	callback()
	// and also add it to the list of callbacks
	p.callbacks = append(p.callbacks, callback)
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

	hash := hashList(p.Cfg.GetPeers())
	p.callbacks = append(p.callbacks, func() {
		currentHash := hashList(p.Cfg.GetPeers())
		if currentHash != hash {
			hash = currentHash
			// Call the callbacks in a separate goroutine to avoid blocking
			for _, callback := range p.callbacks {
				go callback()
			}
		}
	})

	return nil
}

func (p *FilePeers) Ready() error {
	return nil
}

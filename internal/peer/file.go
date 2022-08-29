package peer

import (
	"context"

	"github.com/honeycombio/refinery/config"
)

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
	return p.c.GetPeers()
}

func (p *filePeers) RegisterUpdatedPeersCallback(callback func()) {
	// do nothing, file based peers are not reloaded
}

func (p *filePeers) Close(ctx context.Context) error {
	// do nothing, file based peers do not close
	return nil
}

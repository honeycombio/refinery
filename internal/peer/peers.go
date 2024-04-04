package peer

import (
	"context"
	"errors"

	"github.com/honeycombio/refinery/config"
)

// Peers holds the collection of peers for the cluster
type Peers interface {
	GetPeers() ([]string, error)

	RegisterUpdatedPeersCallback(callback func())
}

func NewPeers(ctx context.Context, c config.Config, done chan struct{}) (Peers, error) {
	t := c.GetPeerManagementType()

	switch t {
	case "file":
		return newFilePeers(c), nil
	case "redis":
		return newRedisPeers(ctx, c, done)
	default:
		return nil, errors.New("invalid config option 'PeerManagement.Type'")
	}
}

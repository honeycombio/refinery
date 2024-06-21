package peer

import (
	"context"
	"errors"

	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/config"
)

// Peers holds the collection of peers for the cluster
type Peers interface {
	GetPeers() ([]string, error)
	RegisterUpdatedPeersCallback(callback func())
	// make it injectable
	startstop.Starter
	startstop.Stopper
}

func NewPeers(ctx context.Context, c config.Config, done chan struct{}) (Peers, error) {
	t, err := c.GetPeerManagementType()

	if err != nil {
		return nil, err
	}

	switch t {
	case "file":
		return newFilePeers(c), nil
	case "redis":
		return newRedisPeers(ctx, c, done)
	case "pubsub":
		return newPubsubPeers(c)
	default:
		return nil, errors.New("invalid config option 'PeerManagement.Type'")
	}
}

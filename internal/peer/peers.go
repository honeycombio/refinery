package peer

import (
	"github.com/facebookgo/startstop"
)

// Peers holds the collection of peers for the cluster
type Peers interface {
	GetPeers() ([]string, error)
	GetInstanceID() (string, error)
	RegisterUpdatedPeersCallback(callback func())
	Ready() error
	// make it injectable
	startstop.Starter
}

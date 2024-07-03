package peer

import (
	"github.com/facebookgo/startstop"
)

// Peers holds the collection of peers for the cluster
type Peers interface {
	GetPeers() ([]string, error)
	RegisterUpdatedPeersCallback(callback func())
	// make it injectable
	startstop.Starter
	startstop.Stopper
}

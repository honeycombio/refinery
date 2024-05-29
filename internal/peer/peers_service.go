package peer

import (
	"bytes"
	"fmt"

	"github.com/facebookgo/startstop"
)

// Peers provides information about all peers in a Refinery cluster.
type Peers interface {
	// Subscribe returns a channel that will receive updates from other active peers.
	Subscribe() <-chan PeerInfo
	// PublishPeerInfo sends information about the local peer to other peers.
	PublishPeerInfo(PeerInfo) error
	// GetPeerCount returns the number of active peers.
	// This includes the local peer.
	GetPeerCount() int
	// HostID returns the unique identifier for the local peer.
	HostID() string

	startstop.Starter
	startstop.Stopper
}

type PeerInfo struct {
	id   string
	Data []byte
}

func (p PeerInfo) ID() string {
	return p.id
}

func (p PeerInfo) Bytes() []byte {
	return append([]byte(p.id+"/"), p.Data...)
}

func newPeerInfoFromBytes(b []byte) (PeerInfo, error) {
	parts := bytes.SplitN(b, []byte("/"), 2)
	if len(parts) != 2 {
		return PeerInfo{}, fmt.Errorf("invalid message format: %s", b)
	}
	return PeerInfo{
		id:   string(parts[0]),
		Data: parts[1],
	}, nil
}

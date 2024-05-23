package peer

import (
	"bytes"
	"fmt"

	"github.com/facebookgo/startstop"
)

// Peers provides information about all peers in a Refinery cluster.
type Peers interface {
	Subscribe(func(PeerInfo))
	PublishPeerInfo(PeerInfo) error
	GetPeerCount() int
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

func (p PeerInfo) ToBytes() []byte {
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

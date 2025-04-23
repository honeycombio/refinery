package sharder

import (
	"sort"
	"sync"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/pkg/errors"
)

// detShard implements Shard
type detShard string

func (d detShard) Equals(other Shard) bool {
	otherDetshard, ok := other.(detShard)
	if !ok {
		return false
	}
	return d == otherDetshard
}

// GetAddress returns the Shard's address in a usable form
func (d detShard) GetAddress() string {
	return string(d)
}

type SortableShardList []detShard

func (s SortableShardList) Len() int      { return len(s) }
func (s SortableShardList) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s SortableShardList) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s SortableShardList) Equals(other SortableShardList) bool {
	if len(s) != len(other) {
		return false
	}
	for i, shard := range s {
		if !shard.Equals(other[i]) {
			return false
		}
	}
	return true
}

// DeterministicSharder uses consistent hashing to assign traces to shards
type DeterministicSharder struct {
	Config config.Config `inject:""`
	Logger logger.Logger `inject:""`
	Peers  peer.Peers    `inject:""`

	myShard detShard
	peers   []detShard
	ring    *hashRing

	peerLock sync.RWMutex
}

// Make sure DeterministicSharder implements Sharder
var _ Sharder = (*DeterministicSharder)(nil)

func (d *DeterministicSharder) Start() error {
	d.Logger.Debug().Logf("Starting DeterministicSharder")
	defer func() { d.Logger.Debug().Logf("Finished starting DeterministicSharder") }()

	d.Peers.RegisterUpdatedPeersCallback(func() {
		d.Logger.Debug().Logf("reloading deterministic sharder config")
		// make an error-less version of the peer reloader
		if err := d.loadPeerList(); err != nil {
			d.Logger.Error().Logf("failed to reload peer list: %+v", err)
		}
	})

	if err := d.loadPeerList(); err != nil {
		d.Logger.Error().Logf("failed to reload peer list: %+v", err)
	}

	// Try up to 5 times to find myself in the peer list before giving up
	var self string
	var err error
	for j := 0; j < 5; j++ {
		self, err = d.Peers.GetInstanceID()
		if err == nil {
			for _, peerShard := range d.peers {
				if self == peerShard.GetAddress() {
					d.myShard = peerShard
					return nil
				}
			}
		}

		d.Logger.Debug().Logf("Failed to find self in peer list; waiting 5sec and trying again")
		time.Sleep(5 * time.Second)
	}

	d.Logger.Error().WithFields(map[string]interface{}{"peers": d.peers, "self": self}).Logf("failed to find self in the peer list")
	return errors.New("failed to find self in the peer list")
}

// loadPeerList updates the peer list and hash ring
func (d *DeterministicSharder) loadPeerList() error {
	d.Logger.Debug().Logf("loading peer list")
	// get my peers
	peerList, err := d.Peers.GetPeers()
	if err != nil {
		return errors.Wrap(err, "failed to get peer list config")
	}

	if len(peerList) == 0 {
		return errors.New("refusing to load empty peer list")
	}

	// turn the peer list into a list of shards
	newPeers := make([]detShard, len(peerList))
	for ix, peer := range peerList {
		peerShard := detShard(peer)
		newPeers[ix] = peerShard
	}

	// make sure the list is in a stable, comparable order
	sort.Sort(SortableShardList(newPeers))

	// Check if the peer list has changed
	d.peerLock.RLock()
	peersChanged := !SortableShardList(d.peers).Equals(newPeers)
	d.peerLock.RUnlock()

	if peersChanged {
		d.Logger.Info().WithField("peers", newPeers).Logf("Peer list has changed, rebuilding hash ring")

		// Build endpoint list for hash ring
		endpoints := make([]string, len(newPeers))
		for i, peer := range newPeers {
			endpoints[i] = peer.GetAddress()
		}

		// Create new hash ring
		newRing := newHashRing(endpoints)

		// Update the ring and peers atomically
		d.peerLock.Lock()
		d.peers = newPeers
		d.ring = newRing
		d.peerLock.Unlock()
	}

	return nil
}

func (d *DeterministicSharder) MyShard() Shard {
	return d.myShard
}

// WhichShard determines which shard a trace should be assigned to
func (d *DeterministicSharder) WhichShard(traceID string) Shard {
	d.peerLock.RLock()
	defer d.peerLock.RUnlock()

	if d.ring == nil || len(d.peers) == 0 {
		// If we don't have a ring yet, just use the first peer or return self
		if len(d.peers) > 0 {
			return d.peers[0]
		}
		return d.myShard
	}

	// Find the endpoint on the hash ring
	endpoint := d.ring.endpointFor([]byte(traceID))

	// Map the endpoint back to a shard
	for _, peer := range d.peers {
		if peer.GetAddress() == endpoint {
			return peer
		}
	}

	// Should never happen if hash ring is properly initialized
	d.Logger.Error().WithField("endpoint", endpoint).Logf("Could not find peer for endpoint")
	return d.peers[0]
}

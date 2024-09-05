package sharder

import (
	"sort"
	"sync"
	"time"

	"github.com/dgryski/go-wyhash"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/pkg/errors"
)

// These are random bits to make sure we differentiate between different
// hash cases even if we use the same value (traceID).
const (
	shardingSalt        = "gf4LqTwcJ6PEj2vO"
	peerSeed     uint64 = 6789531204236
)

type hashShard struct {
	uhash      uint64
	shardIndex int
}

var _ Shard = detShard("")

// detShard implements Shard
type detShard string

// GetHashesFor generates a number of hashShards for a given detShard by repeatedly hashing the
// seed with itself. The intent is to generate a repeatable pseudo-random sequence.
func (d detShard) GetHashesFor(index int, n int, seed uint64) []hashShard {
	hashes := make([]hashShard, 0)
	addr := d.GetAddress()
	for i := 0; i < n; i++ {
		hashes = append(hashes, hashShard{
			uhash:      wyhash.Hash([]byte(addr), seed),
			shardIndex: index,
		})
		// generate another seed from the previous seed; we want this to be the same
		// sequence for everything.
		seed = wyhash.Hash([]byte("anything"), seed)
	}
	return hashes
}
func (d detShard) Equals(other Shard) bool {
	otherDetshard, ok := other.(detShard)
	if !ok {
		// can't be equal if it's a different kind of Shard!
		return false
	}
	// only basic types in this struct; we can use == hooray
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

// make sure DeterministicSharder implements Sharder
var _ Sharder = (*DeterministicSharder)(nil)

type DeterministicSharder struct {
	Config config.Config `inject:""`
	Logger logger.Logger `inject:""`
	Peers  peer.Peers    `inject:""`

	myShard detShard
	peers   []detShard
	hashes  []hashShard

	peerLock sync.RWMutex
}

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
		// go through peer list, resolve each address, see if any of them match any
		// local interface. Note that this assumes only one instance of Refinery per
		// host can run.
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

	d.Logger.Error().WithFields(map[string]interface{}{"peers": d.peers, "self": self}).Logf("list of current peers")
	return errors.New("failed to find self in the peer list")
}

// loadPeerList will run every time any config changes (not only when the list
// of peers changes). Because of this, it only updates the in-memory peer list
// after verifying that it actually changed.
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
	// and a list of hashes
	newPeers := make([]detShard, len(peerList))
	for ix, peer := range peerList {
		peerShard := detShard(peer)
		newPeers[ix] = peerShard
	}

	// make sure the list is in a stable, comparable order
	sort.Sort(SortableShardList(newPeers))

	// In general, the variation in the traffic assigned to a randomly partitioned space is
	// controlled by the number of partitions. PartitionCount controls the minimum number
	// of partitions used to control node assignment when we use the "hash" strategy.
	// When there's a small number of partitions, the two-layer hash strategy can end up giving
	// one partition a disproportionate fraction of the traffic. So we create a large number of
	// random partitions and then assign (potentially) multiple partitions to individual nodes.
	// We're asserting that if we randomly divide the space among at this many partitions, the variation
	// between them is likely to be acceptable. (As this is random, there might be exceptions.)
	// The reason not to make this value much larger, say 1000, is that finding the right partition
	// is linear -- O(number of partitions) and so we want it to be as small as possible
	// while still being big enough.
	// PartitionCount, therefore, is the smallest value that we believe will yield reasonable
	// distribution between nodes. We divide it by the number of nodes using integer division
	// and add 1 to get partitionsPerPeer. We then actually create (nNodes*partitionsPerPeer)
	// partitions, which will always be greater than or equal to partitionCount.
	// Examples: if we have 6 nodes, then partitionsPerPeer will be 9, and we will create
	// 54 partitions. If we have 85 nodes, then partitionsPerPeer will be 1, and we will create
	// 85 partitions.
	const partitionCount = 50
	// now build the hash list;
	// We make a list of hash value and an index to a peer.
	hashes := make([]hashShard, 0)
	partitionsPerPeer := partitionCount/len(peerList) + 1
	for ix := range newPeers {
		hashes = append(hashes, newPeers[ix].GetHashesFor(ix, partitionsPerPeer, peerSeed)...)
	}
	// now sort the hash list by hash value so we can search it efficiently
	sort.Slice(hashes, func(i, j int) bool {
		return hashes[i].uhash < hashes[j].uhash
	})

	// if the peer list changed, load the new list
	d.peerLock.RLock()
	if !SortableShardList(d.peers).Equals(newPeers) {
		d.Logger.Info().WithField("peers", newPeers).Logf("Peer list has changed.")
		d.peerLock.RUnlock()
		d.peerLock.Lock()
		d.peers = newPeers
		d.hashes = hashes
		d.peerLock.Unlock()
	} else {
		d.peerLock.RUnlock()
	}
	return nil
}

func (d *DeterministicSharder) MyShard() Shard {
	return d.myShard
}

// WhichShard calculates which shard we want by keeping a list of partitions. Each
// partition has a different hash value and a map from partition to a given shard.
// We take the traceID and calculate a hash for each partition, using the partition
// hash as the seed for the trace hash. Whichever one has the highest value is the
// partition we use, which determines the shard we use.
// This is O(N) where N is the number of partitions, but because we use an efficient hash,
// (as opposed to SHA1) it executes in 1 uSec for 50 partitions.
func (d *DeterministicSharder) WhichShard(traceID string) Shard {
	d.peerLock.RLock()
	defer d.peerLock.RUnlock()

	tid := []byte(traceID)

	bestix := 0
	var maxHash uint64
	for _, hash := range d.hashes {
		h := wyhash.Hash(tid, hash.uhash)
		if h > maxHash {
			maxHash = h
			bestix = hash.shardIndex
		}
	}
	return d.peers[bestix]
}

package sharder

import (
	"sort"

	"github.com/dgryski/go-wyhash"
)

// These are random bits to make sure we differentiate between different
// hash cases even if we use the same value (traceID).
const (
	shardingSalt        = "gf4LqTwcJ6PEj2vO"
	peerSeed     uint64 = 6789531204236
)

type hashShard struct {
	uhash    uint64
	endpoint string
}

// hashHRW uses the highest random weight (HRW) algorithm to assign a traceID to a shard.
type hashHRW struct {
	shards []hashShard
}

func (h *hashHRW) New(peerList []string, newPeers []detShard) ConsistentHash {
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
	shards := make([]hashShard, 0)
	partitionsPerPeer := partitionCount/len(peerList) + 1
	for ix := range newPeers {
		shards = append(shards, getHashesFor(newPeers[ix], ix, partitionsPerPeer, peerSeed)...)
	}
	// now sort the hash list by hash value so we can search it efficiently
	sort.Slice(shards, func(i, j int) bool {
		return shards[i].uhash < shards[j].uhash
	})

	return &hashHRW{
		shards: shards,
	}
}

func (h *hashHRW) GetDestinationFor(traceID []byte) string {
	var endpoint string
	var maxHash uint64
	for _, hash := range h.shards {
		h := wyhash.Hash(traceID, hash.uhash)
		if h > maxHash {
			maxHash = h
			endpoint = hash.endpoint
		}
	}

	return endpoint
}

// getHashesFor generates a number of hashShards for a given detShard by repeatedly hashing the
// seed with itself. The intent is to generate a repeatable pseudo-random sequence.
func getHashesFor(peer detShard, index int, n int, seed uint64) []hashShard {
	hashes := make([]hashShard, 0)
	addr := peer.GetAddress()
	for i := 0; i < n; i++ {
		hashes = append(hashes, hashShard{
			uhash:    wyhash.Hash([]byte(addr), seed),
			endpoint: addr,
		})
		// generate another seed from the previous seed; we want this to be the same
		// sequence for everything.
		seed = wyhash.Hash([]byte("anything"), seed)
	}
	return hashes
}

package sharder

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/dgryski/go-wyhash"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// These are random bits to make sure we differentiate between different
// hash cases even if we use the same value (traceID).
const (
	shardingSalt        = "gf4LqTwcJ6PEj2vO"
	peerSeed     uint64 = 6789531204236
)

// DetShard implements Shard
type DetShard struct {
	scheme   string
	ipOrHost string
	port     string
}

type hashShard struct {
	uhash      uint64
	shardIndex int
}

func (d *DetShard) Equals(other Shard) bool {
	otherDetshard, ok := other.(*DetShard)
	if !ok {
		// can't be equal if it's a different kind of Shard!
		return false
	}
	// only basic types in this struct; we can use == hooray
	return *d == *otherDetshard
}

type SortableShardList []*DetShard

func (s SortableShardList) Len() int      { return len(s) }
func (s SortableShardList) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s SortableShardList) Less(i, j int) bool {
	if s[i].ipOrHost != s[j].ipOrHost {
		return s[i].ipOrHost < s[j].ipOrHost
	}
	if s[i].scheme != s[j].scheme {
		return s[i].scheme < s[j].scheme
	}
	return s[i].port < s[j].port
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

// GetAddress returns the Shard's address in a usable form
func (d *DetShard) GetAddress() string {
	return fmt.Sprintf("%s://%s:%s", d.scheme, d.ipOrHost, d.port)
}

func (d *DetShard) String() string {
	return d.GetAddress()
}

// GetHashesFor generates a number of hashShards for a given DetShard by repeatedly hashing the
// seed with itself. The intent is to generate a repeatable pseudo-random sequence.
func (d *DetShard) GetHashesFor(index int, n int, seed uint64) []hashShard {
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

type DeterministicSharder struct {
	Config config.Config `inject:""`
	Logger logger.Logger `inject:""`
	Peers  peer.Peers    `inject:""`

	myShard   *DetShard
	peers     []*DetShard
	hashes    []hashShard
	shardFunc func(traceID string) Shard

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

	// this isn't runtime-reloadable because it would
	// reassign nearly every trace to a new shard.
	strat, err := d.Config.GetPeerManagementStrategy()
	if err != nil {
		return errors.Wrap(err, "failed to get peer management strategy")
	}
	switch strat {
	case "legacy", "":
		d.shardFunc = d.WhichShardLegacy
	case "hash":
		d.shardFunc = d.WhichShardHashed
	default:
		return fmt.Errorf("unknown PeerManagementStrategy '%s'", strat)
	}

	// Try up to 5 times to find myself in the peer list before giving up
	var found bool
	var selfIndexIntoPeerList int
	for j := 0; j < 5; j++ {
		err := d.loadPeerList()
		if err != nil {
			return err
		}

		// get my listen address for peer traffic for the Port number
		listenAddr, err := d.Config.GetPeerListenAddr()
		if err != nil {
			return errors.Wrap(err, "failed to get listen addr config")
		}
		_, localPort, err := net.SplitHostPort(listenAddr)
		if err != nil {
			return errors.Wrap(err, "failed to parse listen addr into host:port")
		}
		d.Logger.Debug().Logf("picked up local peer port of %s", localPort)

		var localIPs []string

		// If RedisIdentifier is an IP, use as localIPs value.
		if redisIdentifier, err := d.Config.GetRedisIdentifier(); err == nil && redisIdentifier != "" {
			if ip := net.ParseIP(redisIdentifier); ip != nil {
				d.Logger.Debug().Logf("Using RedisIdentifier as public IP: %s", redisIdentifier)
				localIPs = []string{redisIdentifier}
			}
		}

		// Otherwise, get my local interfaces' IPs.
		if len(localIPs) == 0 {
			localAddrs, err := net.InterfaceAddrs()
			if err != nil {
				return errors.Wrap(err, "failed to get local interface list to initialize sharder")
			}
			localIPs = make([]string, len(localAddrs))
			for i, addr := range localAddrs {
				addrStr := addr.String()
				ip, _, err := net.ParseCIDR(addrStr)
				if err != nil {
					return errors.Wrap(err, fmt.Sprintf("failed to parse CIDR for local IP %s", addrStr))
				}
				localIPs[i] = ip.String()
			}
		}

		// go through peer list, resolve each address, see if any of them match any
		// local interface. Note that this assumes only one instance of Refinery per
		// host can run.
		for i, peerShard := range d.peers {
			d.Logger.Debug().WithFields(logrus.Fields{
				"peer": peerShard,
				"self": localIPs,
			}).Logf("Considering peer looking for self")
			peerIPList, err := net.LookupHost(peerShard.ipOrHost)
			if err != nil {
				// TODO something better than fail to start if peer is missing
				return errors.Wrap(err, fmt.Sprintf("couldn't resolve peer hostname %s", peerShard.ipOrHost))
			}
			for _, peerIP := range peerIPList {
				for _, ipAddr := range localIPs {
					if peerIP == ipAddr {
						if peerShard.port == localPort {
							d.Logger.Debug().WithField("peer", peerShard).Logf("Found myself in peer list")
							found = true
							selfIndexIntoPeerList = i
						} else {
							d.Logger.Debug().WithFields(logrus.Fields{
								"peer":         peerShard,
								"expectedPort": localPort,
							}).Logf("Peer port mismatch")
						}
					}
				}
			}
		}
		if found {
			break
		}
		d.Logger.Debug().Logf("Failed to find self in peer list; waiting 5sec and trying again")
		time.Sleep(5 * time.Second)
	}
	if !found {
		d.Logger.Debug().Logf("list of current peers: %+v", d.peers)
		return errors.New("failed to find self in the peer list")
	}
	d.myShard = d.peers[selfIndexIntoPeerList]

	return nil
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
	newPeers := make([]*DetShard, len(peerList))
	for ix, peer := range peerList {
		peerURL, err := url.Parse(peer)
		if err != nil {
			return errors.Wrap(err, "couldn't parse peer as a URL")
		}
		peerShard := &DetShard{
			scheme:   peerURL.Scheme,
			ipOrHost: peerURL.Hostname(),
			port:     peerURL.Port(),
		}
		newPeers[ix] = peerShard
	}

	// make sure the list is in a stable, comparable order
	sort.Sort(SortableShardList(newPeers))

	// PartitionCount controls the minimum number of partitions used to control node assignment
	// when we use the "hash" strategy.
	// Ideally, this value should be 3-5x the maximum expected number of nodes.
	// If necessary, it will (effectively) be increased to equal the number of nodes.
	// A larger value will achieve more even trace distribution, at the cost of a minor performance
	// hit once per span (roughly 1 microsecond per 50 partitions). Since the distribution isn't
	// that even anyway (yay randomness) 50 seems to be a good balance; it helps with small
	// installations and doesn't hurt for big ones.
	const partitionCount = 50
	// now build the hash list;
	// We make a list of hash value and an index to a peer.
	hashes := make([]hashShard, 0)
	hashesPerPeer := partitionCount/len(peerList) + 1
	for ix := range newPeers {
		hashes = append(hashes, newPeers[ix].GetHashesFor(ix, hashesPerPeer, peerSeed)...)
	}
	// now sort the hash list by hash value so we can search it efficiently
	sort.Slice(hashes, func(i, j int) bool {
		return hashes[i].uhash < hashes[j].uhash
	})

	// if the peer list changed, load the new list
	d.peerLock.RLock()
	if !SortableShardList(d.peers).Equals(newPeers) {
		d.Logger.Info().Logf("Peer list has changed. New peer list: %+v", newPeers)
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

func (d *DeterministicSharder) WhichShard(traceID string) Shard {
	return d.shardFunc(traceID)
}

// WhichShardLegacy is the original sharding decider. It uses sha1, which is
// slow and not well-distributed, and also simply partitions the sharding
// space into N evenly-divided buckets, which means that on every change in
// shard count, half of the traces get reassigned (which leads to broken traces).
// We leave it here to avoid disrupting things and provide a fallback if needed,
// but the intent is eventually to delete this.
func (d *DeterministicSharder) WhichShardLegacy(traceID string) Shard {
	d.peerLock.RLock()
	defer d.peerLock.RUnlock()

	// add in the sharding salt to ensure the sh1sum is spread differently from
	// others that use the same algorithm
	sum := sha1.Sum([]byte(traceID + shardingSalt))
	v := binary.BigEndian.Uint32(sum[:4])

	portion := math.MaxUint32 / len(d.peers)
	index := v / uint32(portion)

	// #454 -- index can get out of range if v is close to 0xFFFFFFFF and portion would be non-integral.
	// Consider revisiting this with a different sharding mechanism if we rework our scaling behavior.
	if index >= uint32(len(d.peers)) {
		index = 0
	}

	return d.peers[index]
}

// WhichShardHashed calculates which shard we want by keeping a list of partitions. Each
// partition has a different hash value and a map from partition to a given shard.
// We take the traceID and calculate a hash for each partition, using the partition
// hash as the seed for the trace hash. Whichever one has the highest value is the
// partition we use, which determines the shard we use.
// This is O(N) where N is the number of partitions, but because we use an efficient hash,
// (as opposed to SHA1) it executes in 1 uSec for 50 partitions, so it works out to about
// the same cost as the legacy sharder.
func (d *DeterministicSharder) WhichShardHashed(traceID string) Shard {
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

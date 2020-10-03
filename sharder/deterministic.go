package sharder

import (
	"crypto/sha1"
	"fmt"
	"math"
	"net"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/pkg/errors"
)

// shardingSalt is a random bit to make sure we don't shard the same as any
// other sharding that uses the trace ID (eg deterministic sampling)
const shardingSalt = "gf4LqTwcJ6PEj2vO"

// DetShard implements Shard
type DetShard struct {
	scheme   string
	ipOrHost string
	port     string
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

type DeterministicSharder struct {
	Config config.Config `inject:""`
	Logger logger.Logger `inject:""`
	Peers  peer.Peers    `inject:""`

	myShard *DetShard
	peers   []*DetShard

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

		// get my local interfaces
		localAddrs, err := net.InterfaceAddrs()
		if err != nil {
			return errors.Wrap(err, "failed to get local interface list to initialize sharder")
		}

		// go through peer list, resolve each address, see if any of them match any
		// local interface. Note that this assumes only one instance of Refinery per
		// host can run.
		for i, peerShard := range d.peers {
			d.Logger.Debug().WithField("peer", peerShard).WithField("self", localAddrs).Logf("Considering peer looking for self")
			peerIPList, err := net.LookupHost(peerShard.ipOrHost)
			if err != nil {
				// TODO something better than fail to start if peer is missing
				return errors.Wrap(err, fmt.Sprintf("couldn't resolve peer hostname %s", peerShard.ipOrHost))
			}
			for _, peerIP := range peerIPList {
				for _, localIP := range localAddrs {
					ipAddr, _, err := net.ParseCIDR(localIP.String())
					if err != nil {
						return errors.Wrap(err, fmt.Sprintf("failed to parse CIDR for local IP %s", localIP.String()))
					}
					if peerIP == ipAddr.String() {
						if peerShard.port == localPort {
							d.Logger.Debug().WithField("peer", peerShard).Logf("Found myself in peer list")
							found = true
							selfIndexIntoPeerList = i
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

	// turn my peer list into a list of shards
	newPeers := make([]*DetShard, 0, len(peerList))
	for _, peer := range peerList {
		peerURL, err := url.Parse(peer)
		if err != nil {
			return errors.Wrap(err, "couldn't parse peer as a URL")
		}
		peerShard := &DetShard{
			scheme:   peerURL.Scheme,
			ipOrHost: peerURL.Hostname(),
			port:     peerURL.Port(),
		}
		newPeers = append(newPeers, peerShard)
	}

	// the redis peer discovery already sorts its content. Does every backend?
	// well, it's not too much work, let's sort it one more time.
	sort.Sort(SortableShardList(newPeers))

	// if the peer list changed, load the new list
	d.peerLock.RLock()
	if !SortableShardList(d.peers).Equals(newPeers) {
		d.Logger.Info().Logf("Peer list has changed. New peer list: %+v", newPeers)
		d.peerLock.RUnlock()
		d.peerLock.Lock()
		d.peers = newPeers
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
	d.peerLock.RLock()
	defer d.peerLock.RUnlock()

	// add in the sharding salt to ensure the sh1sum is spread differently from
	// others that use the same algorithm
	sum := sha1.Sum([]byte(traceID + shardingSalt))
	v := bytesToUint32be(sum[:4])

	portion := math.MaxUint32 / len(d.peers)
	index := v / uint32(portion)

	return d.peers[index]
}

// bytesToUint32 takes a slice of 4 bytes representing a big endian 32 bit
// unsigned value and returns the equivalent uint32.
func bytesToUint32be(b []byte) uint32 {
	return uint32(b[3]) | (uint32(b[2]) << 8) | (uint32(b[1]) << 16) | (uint32(b[0]) << 24)
}

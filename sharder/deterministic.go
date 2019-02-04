package sharder

import (
	"crypto/sha1"
	"fmt"
	"math"
	"net"
	"net/url"

	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/pkg/errors"
)

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
	return d == otherDetshard
}

// GetAddress returns the Shard's address in a usable form
func (d *DetShard) GetAddress() string {
	return fmt.Sprintf("%s://%s:%s", d.scheme, d.ipOrHost, d.port)
}

type DeterministicSharder struct {
	Config config.Config `inject:""`
	Logger logger.Logger `inject:""`

	myShard *DetShard
	peers   []*DetShard
}

func (d *DeterministicSharder) Start() error {

	d.Config.RegisterReloadCallback(func() {
		// make an error-less version of the peer reloader
		if err := d.loadPeerList(); err != nil {
			d.Logger.Errorf("failed to reload peer list: %+v", err)
		}
	})

	err := d.loadPeerList()
	if err != nil {
		return err
	}

	// get my listen address for the Port number
	listenAddr, err := d.Config.GetListenAddr()
	if err != nil {
		return errors.Wrap(err, "failed to get listen addr config")
	}
	_, localPort, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return errors.Wrap(err, "failed to parse listen addr into host:port")
	}
	d.Logger.Debugf("picked up local port of %s", localPort)

	// get my local interfaces
	localAddrs, err := net.InterfaceAddrs()
	if err != nil {
		return errors.Wrap(err, "failed to get local interface list to initialize sharder")
	}

	// go through peer list, resolve each address, see if any of them match any
	// local interface
	var selfIndexIntoPeerList int
	var found bool
	for i, peerShard := range d.peers {
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
						d.Logger.WithField("peer", peerShard).Debugf("Found myself in peer list")
						found = true
						selfIndexIntoPeerList = i
					}
				}
			}
		}
	}
	if !found {
		return errors.New("failed to find self in the peer list")
	}
	d.myShard = d.peers[selfIndexIntoPeerList]

	return nil
}

func (d *DeterministicSharder) loadPeerList() error {
	d.Logger.Debugf("loading peer list")
	// get my peers
	peerList, err := d.Config.GetPeers()
	if err != nil {
		return errors.Wrap(err, "failed to get peer list config")
	}

	// turn my peer list into a list of shards
	d.peers = make([]*DetShard, 0, len(peerList))
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
		d.peers = append(d.peers, peerShard)
	}
	return nil
}

func (d *DeterministicSharder) MyShard() Shard {
	return d.myShard
}

func (d *DeterministicSharder) WhichShard(traceID string) Shard {

	sum := sha1.Sum([]byte(traceID))
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

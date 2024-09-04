package peer

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	"github.com/dgryski/go-wyhash"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/generics"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/jonboulle/clockwork"
)

const (
	// PeerEntryTimeout is how long we will wait before expiring a peer that
	// doesn't check in. The ratio of refresh to peer timeout should be about
	// 1/3; we overshoot because we add a jitter to the refresh interval.
	PeerEntryTimeout = 10 * time.Second

	// refreshCacheInterval is how frequently this host will re-register itself
	// by publishing their address. This should happen about 3x during each
	// timeout phase in order to allow multiple timeouts to fail and yet still
	// keep the host in the mix.
	refreshCacheInterval = 3 * time.Second
)

type peerAction string

const (
	Register   peerAction = "R"
	Unregister peerAction = "U"
)

type peerCommand struct {
	action peerAction
	peer   string
}

func newPeerCommand(action peerAction, peer string) *peerCommand {
	return &peerCommand{
		action: action,
		peer:   peer,
	}
}

func (p *peerCommand) unmarshal(msg string) bool {
	if len(msg) < 2 {
		return false
	}
	p.action = peerAction(msg[:1])
	p.peer = msg[1:]
	switch p.action {
	case Register, Unregister:
		return true
	default:
		return false
	}
}

func (p *peerCommand) marshal() string {
	return string(p.action) + p.peer
}

var _ Peers = (*RedisPubsubPeers)(nil)

type RedisPubsubPeers struct {
	Config  config.Config   `inject:""`
	Metrics metrics.Metrics `inject:"metrics"`
	Logger  logger.Logger   `inject:""`
	PubSub  pubsub.PubSub   `inject:""`
	Clock   clockwork.Clock `inject:""`

	// Done is a channel that will be closed when the service should stop.
	// After it is closed, peers service should signal the rest of the cluster
	// that it is no longer available.
	// However, any messages send on the peers channel will still be processed
	// since the pubsub subscription is still active.
	Done chan struct{}

	peers     *generics.SetWithTTL[string]
	hash      uint64
	callbacks []func()
	sub       pubsub.Subscription
}

// checkHash checks the hash of the current list of peers and calls any registered callbacks
func (p *RedisPubsubPeers) checkHash() {
	peers := p.peers.Members()
	newhash := hashList(peers)
	if newhash != p.hash {
		p.hash = newhash
		for _, cb := range p.callbacks {
			go cb()
		}
	}
	p.Metrics.Gauge("num_peers", float64(len(peers)))
	p.Metrics.Gauge("peer_hash", float64(p.hash))
}

func (p *RedisPubsubPeers) listen(ctx context.Context, msg string) {
	cmd := &peerCommand{}
	if !cmd.unmarshal(msg) {
		return
	}
	p.Metrics.Count("peer_messages", 1)
	switch cmd.action {
	case Unregister:
		p.peers.Remove(cmd.peer)
	case Register:
		p.peers.Add(cmd.peer)
	}
	p.checkHash()
}

func (p *RedisPubsubPeers) Start() error {
	if p.PubSub == nil {
		return errors.New("injected pubsub is nil")
	}
	// if we didn't get an injected logger or metrics, use the null ones (for tests)
	if p.Metrics == nil {
		p.Metrics = &metrics.NullMetrics{}
	}
	if p.Logger == nil {
		p.Logger = &logger.NullLogger{}
	}

	p.peers = generics.NewSetWithTTL[string](PeerEntryTimeout)
	p.callbacks = make([]func(), 0)
	p.Logger.Info().Logf("subscribing to pubsub peers channel")
	p.sub = p.PubSub.Subscribe(context.Background(), "peers", p.listen)

	p.Metrics.Register("num_peers", "gauge")
	p.Metrics.Register("peer_hash", "gauge")
	p.Metrics.Register("peer_messages", "counter")

	myaddr, err := p.publicAddr()
	if err != nil {
		return err
	}
	p.peers.Add(myaddr)
	return nil
}

func (p *RedisPubsubPeers) Ready() error {
	myaddr, err := p.publicAddr()
	if err != nil {
		return err
	}
	// periodically refresh our presence in the list of peers, and update peers as they come in
	go func() {
		// we want our refresh cache interval to vary from peer to peer so they
		// don't always hit redis at the same time, so we add a random jitter of up
		// to 20% of the interval
		interval := refreshCacheInterval + time.Duration(rand.Int63n(int64(refreshCacheInterval/5)))
		ticker := p.Clock.NewTicker(interval)
		defer ticker.Stop()

		// every 25-35 seconds, log the current state of the peers
		// (we could make this configurable if we wanted but it's not that important)
		logTicker := p.Clock.NewTicker((time.Duration(rand.Intn(10000))*time.Millisecond + (25 * time.Second)))
		defer logTicker.Stop()
		for {
			select {
			case <-p.Done:
				p.stop()
				return
			case <-ticker.Chan():

				// publish our presence periodically
				ctx, cancel := context.WithTimeout(context.Background(), p.Config.GetPeerTimeout())
				err := p.PubSub.Publish(ctx, "peers", newPeerCommand(Register, myaddr).marshal())
				if err != nil {
					p.Logger.Error().WithFields(map[string]interface{}{
						"error":       err,
						"hostaddress": myaddr,
					}).Logf("failed to publish peer address")
				}
				cancel()
			case <-logTicker.Chan():
				p.Logger.Debug().WithFields(map[string]any{
					"peers":     p.peers.Members(),
					"hash":      p.hash,
					"num_peers": len(p.peers.Members()),
					"self":      myaddr,
				}).Logf("peer report")
			}
		}
	}()

	return nil
}

// stop send a message to the pubsub channel to unregister this peer
// but it does not close the subscription.
func (p *RedisPubsubPeers) stop() {
	// unregister ourselves
	myaddr, err := p.publicAddr()
	if err != nil {
		p.Logger.Error().Logf("failed to get public address")
		return
	}

	err = p.PubSub.Publish(context.Background(), "peers", newPeerCommand(Unregister, myaddr).marshal())
	if err != nil {
		p.Logger.Error().WithFields(map[string]interface{}{
			"error":       err,
			"hostaddress": myaddr,
		}).Logf("failed to publish peer address")
	}
}

func (p *RedisPubsubPeers) GetPeers() ([]string, error) {
	// we never want to return an empty list of peers, so if the system returns
	// an empty list, return a single peer (its name doesn't really matter).
	// This keeps the sharding logic happy.
	peers := p.peers.Members()
	if len(peers) == 0 {
		peers = []string{"http://127.0.0.1:8081"}
	}
	return peers, nil
}

func (p *RedisPubsubPeers) GetInstanceID() (string, error) {
	return p.publicAddr()
}

func (p *RedisPubsubPeers) RegisterUpdatedPeersCallback(callback func()) {
	p.callbacks = append(p.callbacks, callback)
}

func (p *RedisPubsubPeers) publicAddr() (string, error) {
	// compute the public version of my peer listen address
	listenAddr := p.Config.GetPeerListenAddr()
	// first, extract the port
	_, port, err := net.SplitHostPort(listenAddr)

	if err != nil {
		return "", err
	}

	var myIdentifier string

	// If RedisIdentifier is set, use as identifier.
	if redisIdentifier := p.Config.GetRedisIdentifier(); redisIdentifier != "" {
		myIdentifier = redisIdentifier
		p.Logger.Info().WithField("identifier", myIdentifier).Logf("using specified RedisIdentifier from config")
	} else {
		// Otherwise, determine identifier from network interface.
		myIdentifier, err = p.getIdentifierFromInterface()
		if err != nil {
			return "", err
		}
	}

	publicListenAddr := fmt.Sprintf("http://%s:%s", myIdentifier, port)

	return publicListenAddr, nil
}

// getIdentifierFromInterface returns a string that uniquely identifies this
// host in the network. If an interface is specified, it will scan it to
// determine an identifier from the first IP address on that interface.
// Otherwise, it will use the hostname.
func (p *RedisPubsubPeers) getIdentifierFromInterface() (string, error) {
	myIdentifier, _ := os.Hostname()
	identifierInterfaceName := p.Config.GetIdentifierInterfaceName()

	if identifierInterfaceName != "" {
		ifc, err := net.InterfaceByName(identifierInterfaceName)
		if err != nil {
			p.Logger.Error().WithField("interface", identifierInterfaceName).
				Logf("IdentifierInterfaceName set but couldn't find interface by that name")
			return "", err
		}
		addrs, err := ifc.Addrs()
		if err != nil {
			p.Logger.Error().WithField("interface", identifierInterfaceName).
				Logf("IdentifierInterfaceName set but couldn't list addresses")
			return "", err
		}
		var ipStr string
		for _, addr := range addrs {
			// ParseIP doesn't know what to do with the suffix
			ip := net.ParseIP(strings.Split(addr.String(), "/")[0])
			ipv6 := p.Config.GetUseIPV6Identifier()
			if ipv6 && ip.To16() != nil {
				ipStr = fmt.Sprintf("[%s]", ip.String())
				break
			}
			if !ipv6 && ip.To4() != nil {
				ipStr = ip.String()
				break
			}
		}
		if ipStr == "" {
			err = errors.New("could not find a valid IP to use from interface")
			p.Logger.Error().WithField("interface", ifc.Name).
				Logf("IdentifierInterfaceName set but couldn't find a valid IP to use from interface")
			return "", err
		}
		myIdentifier = ipStr
		p.Logger.Info().WithField("identifier", myIdentifier).WithField("interface", ifc.Name).
			Logf("using identifier from interface")
	}

	return myIdentifier, nil
}

// hashList hashes a list of strings into a single uint64
func hashList(list []string) uint64 {
	var h uint64 = 255798297204 // arbitrary seed
	for _, s := range list {
		h = wyhash.Hash([]byte(s), h)
	}
	return h
}

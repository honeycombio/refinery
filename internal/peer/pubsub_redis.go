package peer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/dgryski/go-wyhash"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/generics"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
)

const (
	// refreshCacheInterval is how frequently this host will re-register itself
	// with Redis. This should happen about 3x during each timeout phase in order
	// to allow multiple timeouts to fail and yet still keep the host in the mix.
	// Falling out of Redis will result in re-hashing the host-trace affinity and
	// will cause broken traces for those that fall on both sides of the rehashing.
	// This is why it's important to ensure hosts stay in the pool.
	refreshCacheInterval = 3 * time.Second

	// peerEntryTimeout is how long redis will wait before expiring a peer that
	// doesn't check in. The ratio of refresh to peer timeout should be 1/3. Redis
	// timeouts are in seconds and entries can last up to 2 seconds longer than
	// their expected timeout (in my load testing), so the lower bound for this
	// timer should be ... 5sec?
	peerEntryTimeout = 10 * time.Second
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

type RedisPubsubPeers struct {
	Config config.Config   `inject:""`
	PubSub pubsub.PubSub   `inject:""`
	Clock  clockwork.Clock `inject:""`

	peers     *generics.SetWithTTL[string]
	hash      uint64
	callbacks []func()
	sub       pubsub.Subscription
	done      chan struct{}
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
}

func (p *RedisPubsubPeers) listen(msg string) {
	cmd := &peerCommand{}
	if !cmd.unmarshal(msg) {
		return
	}
	switch cmd.action {
	case Unregister:
		p.peers.Remove(cmd.peer)
	case Register:
		p.peers.Add(cmd.peer)
	}
	p.checkHash()
}

func (p *RedisPubsubPeers) Start() error {
	p.peers = generics.NewSetWithTTL[string](peerEntryTimeout)
	if p.PubSub == nil {
		return errors.New("injected pubsub is nil")
	}

	p.done = make(chan struct{})
	p.peers = generics.NewSetWithTTL[string](peerEntryTimeout)
	p.callbacks = make([]func(), 0)
	p.sub = p.PubSub.Subscribe(context.Background(), "peers", p.listen)

	myaddr, err := publicAddr(p.Config)
	if err != nil {
		return err
	}

	// periodically refresh our presence in the list of peers, and update peers as they come in
	go func() {
		ticker := p.Clock.NewTicker(refreshCacheInterval)
		for {
			select {
			case <-p.done:
				return
			case <-ticker.Chan():
				// publish our presence periodically
				ctx, cancel := context.WithTimeout(context.Background(), p.Config.GetPeerTimeout())
				p.PubSub.Publish(ctx, "peers", newPeerCommand(Register, myaddr).marshal())
				cancel()
			}
		}
	}()

	return nil
}

func (p *RedisPubsubPeers) Stop() error {
	// unregister ourselves
	myaddr, err := publicAddr(p.Config)
	if err != nil {
		return err
	}
	p.PubSub.Publish(context.Background(), "peers", newPeerCommand(Unregister, myaddr).marshal())
	close(p.done)
	return nil
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

func (p *RedisPubsubPeers) RegisterUpdatedPeersCallback(callback func()) {
	p.callbacks = append(p.callbacks, callback)
}

func publicAddr(c config.Config) (string, error) {
	// compute the public version of my peer listen address
	listenAddr, _ := c.GetPeerListenAddr()
	// first, extract the port
	_, port, err := net.SplitHostPort(listenAddr)

	if err != nil {
		return "", err
	}

	var myIdentifier string

	// If RedisIdentifier is set, use as identifier.
	if redisIdentifier, _ := c.GetRedisIdentifier(); redisIdentifier != "" {
		myIdentifier = redisIdentifier
		logrus.WithField("identifier", myIdentifier).Info("using specified RedisIdentifier from config")
	} else {
		// Otherwise, determine identifier from network interface.
		myIdentifier, err = getIdentifierFromInterface(c)
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
func getIdentifierFromInterface(c config.Config) (string, error) {
	myIdentifier, _ := os.Hostname()
	identifierInterfaceName, _ := c.GetIdentifierInterfaceName()

	if identifierInterfaceName != "" {
		ifc, err := net.InterfaceByName(identifierInterfaceName)
		if err != nil {
			logrus.WithError(err).WithField("interface", identifierInterfaceName).
				Error("IdentifierInterfaceName set but couldn't find interface by that name")
			return "", err
		}
		addrs, err := ifc.Addrs()
		if err != nil {
			logrus.WithError(err).WithField("interface", identifierInterfaceName).
				Error("IdentifierInterfaceName set but couldn't list addresses")
			return "", err
		}
		var ipStr string
		for _, addr := range addrs {
			// ParseIP doesn't know what to do with the suffix
			ip := net.ParseIP(strings.Split(addr.String(), "/")[0])
			ipv6, _ := c.GetUseIPV6Identifier()
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
			logrus.WithField("interface", ifc.Name).WithError(err)
			return "", err
		}
		myIdentifier = ipStr
		logrus.WithField("identifier", myIdentifier).WithField("interface", ifc.Name).Info("using identifier from interface")
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

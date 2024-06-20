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

type pubsubPeers struct {
	Config config.Config   `inject:""`
	PubSub pubsub.PubSub   `inject:""`
	Clock  clockwork.Clock `inject:""`

	peers       *generics.SetWithTTL[string]
	hash        uint64
	callbacks   []func()
	gossipTopic pubsub.Topic
	peerchan    <-chan string
	done        chan struct{}
}

// NewPubsubPeers returns a peers collection backed by a pubsub system.
// It expects members to publish their own presence to the pubsub system every so often
// as determined by refreshCacheInterval. If they fail to do after peerEntryTimeout, they
// will be removed from the list of peers.
// They can also remove themselves from the list of peers by publishing an "unregister" message.
// The register message is just "register address" and the unregister message is "unregister address".
func newPubsubPeers() (Peers, error) {
	return &pubsubPeers{}, nil
}

// checkHash checks the hash of the current list of peers and calls any registered callbacks
func (p *pubsubPeers) checkHash() {
	peers := p.peers.Members()
	newhash := hashList(peers)
	if newhash != p.hash {
		p.hash = newhash
		for _, cb := range p.callbacks {
			cb()
		}
	}
}

func (p *pubsubPeers) Start() error {
	if p.PubSub == nil {
		return errors.New("injected pubsub is nil")
	}

	p.done = make(chan struct{})
	p.peers = generics.NewSetWithTTL[string](peerEntryTimeout)
	p.callbacks = make([]func(), 0)
	p.gossipTopic = p.PubSub.NewTopic(context.Background(), "peers")
	p.peerchan = p.gossipTopic.Subscribe(context.Background())

	myname, err := getIdentifierFromInterfaces(p.Config)
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
				p.gossipTopic.Publish(ctx, "register "+myname)
				cancel()
			case msg := <-p.peerchan:
				parts := strings.Split(msg, " ")
				if len(parts) != 2 {
					continue
				}
				action, peer := parts[0], parts[1]
				switch action {
				case "unregister":
					p.peers.Remove(peer)
				case "register":
					p.peers.Add(peer)
				}
				p.checkHash()
			}
		}
	}()

	return nil
}

func (p *pubsubPeers) Stop() error {
	// unregister ourselves
	myname, err := getIdentifierFromInterfaces(p.Config)
	if err != nil {
		return err
	}
	p.gossipTopic.Publish(context.Background(), "unregister "+myname)
	close(p.done)
	return nil
}

func (p *pubsubPeers) GetPeers() ([]string, error) {
	// we never want to return an empty list of peers, so if the system returns
	// an empty list, return a single peer (its name doesn't really matter).
	// This keeps the sharding logic happy.
	peers := p.peers.Members()
	if len(peers) == 0 {
		peers = []string{"http://127.0.0.1:8081"}
	}
	return peers, nil
}

func (p *pubsubPeers) RegisterUpdatedPeersCallback(callback func()) {
	p.callbacks = append(p.callbacks, callback)
}

// Scan network interfaces to determine an identifier from either IP or hostname.
func getIdentifierFromInterfaces(c config.Config) (string, error) {
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

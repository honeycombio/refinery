package peer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/redimem"
	"github.com/honeycombio/refinery/redis"

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

type RedisPeers struct {
	store       *redimem.RedisMembership
	RedisClient redis.Client `inject:"redis"`
	peers       []string
	peerLock    sync.Mutex
	Config      config.Config `inject:""`
	callbacks   []func()
	publicAddr  string
	done        chan struct{}
}

func (p *RedisPeers) Start() error {
	address, err := publicAddr(p.Config)
	if err != nil {
		return err
	}

	p.store = &redimem.RedisMembership{
		Prefix: p.Config.GetRedisPrefix(),
		Pool:   p.RedisClient,
	}
	p.peers = make([]string, 1)
	p.callbacks = make([]func(), 0)
	p.publicAddr = address

	// register myself once
	ctx := context.Background()
	err = p.store.Register(ctx, address, peerEntryTimeout)
	if err != nil {
		logrus.WithError(err).Errorf("failed to register self with redis peer store")
		return err
	}

	// go establish a regular registration heartbeat to ensure I stay alive in redis
	go p.registerSelf()

	// get our peer list once to seed ourselves
	p.updatePeerListOnce()

	// go watch the list of peers and trigger callbacks whenever it changes.
	// populate my local list of peers so each request can hit memory and only hit
	// redis on a ticker
	go p.watchPeers()
	return nil
}

func (p *RedisPeers) Stop() error {
	close(p.done)
	return nil
}

func (p *RedisPeers) GetPeers() ([]string, error) {
	p.peerLock.Lock()
	defer p.peerLock.Unlock()
	retList := make([]string, len(p.peers))
	copy(retList, p.peers)
	return retList, nil
}

func (p *RedisPeers) RegisterUpdatedPeersCallback(cb func()) {
	p.callbacks = append(p.callbacks, cb)
}

// registerSelf inserts self into the peer list and updates self's entry on a
// regular basis so it doesn't time out and get removed from the list of peers.
// When this function stops, it tries to remove the registered key.
func (p *RedisPeers) registerSelf() {
	tk := time.NewTicker(refreshCacheInterval)
	for {
		select {
		case <-tk.C:
			ctx, cancel := context.WithTimeout(context.Background(), p.Config.GetPeerTimeout())
			// every interval, insert a timeout record. we ignore the error
			// here since Register() logs the error for us.
			p.store.Register(ctx, p.publicAddr, peerEntryTimeout)
			cancel()
		case <-p.done:
			// unregister ourselves
			ctx, cancel := context.WithTimeout(context.Background(), p.Config.GetPeerTimeout())
			p.store.Unregister(ctx, p.publicAddr)
			cancel()
			return
		}
	}
}

func (p *RedisPeers) updatePeerListOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), p.Config.GetPeerTimeout())
	defer cancel()

	currentPeers, err := p.store.GetMembers(ctx)
	if err != nil {
		logrus.WithError(err).
			WithFields(logrus.Fields{
				"name":    p.publicAddr,
				"timeout": p.Config.GetPeerTimeout().String(),
			}).
			Error("get members failed")
		return
	}
	sort.Strings(currentPeers)
	// update peer list and trigger callbacks saying the peer list has changed
	p.peerLock.Lock()
	p.peers = currentPeers
	p.peerLock.Unlock()
}

func (p *RedisPeers) watchPeers() {
	oldPeerList := p.peers
	sort.Strings(oldPeerList)
	tk := time.NewTicker(refreshCacheInterval)

	for {
		select {
		case <-tk.C:
			ctx, cancel := context.WithTimeout(context.Background(), p.Config.GetPeerTimeout())
			currentPeers, err := p.store.GetMembers(ctx)
			cancel()

			if err != nil {
				logrus.WithError(err).
					WithFields(logrus.Fields{
						"name":     p.publicAddr,
						"timeout":  p.Config.GetPeerTimeout().String(),
						"oldPeers": oldPeerList,
					}).
					Error("get members failed during watch")
				continue
			}

			sort.Strings(currentPeers)
			if !equal(oldPeerList, currentPeers) {
				// update peer list and trigger callbacks saying the peer list has changed
				p.peerLock.Lock()
				p.peers = currentPeers
				oldPeerList = currentPeers
				p.peerLock.Unlock()
				for _, callback := range p.callbacks {
					// don't block on any of the callbacks.
					go callback()
				}
			}
		case <-p.done:
			p.peerLock.Lock()
			p.peers = []string{}
			p.peerLock.Unlock()
			return
		}
	}
}

func publicAddr(c config.Config) (string, error) {
	// compute the public version of my peer listen address
	listenAddr := c.GetPeerListenAddr()
	_, port, err := net.SplitHostPort(listenAddr)

	if err != nil {
		return "", err
	}

	var myIdentifier string

	// If RedisIdentifier is set, use as identifier.
	if redisIdentifier := c.GetRedisIdentifier(); redisIdentifier != "" {
		myIdentifier = redisIdentifier
		logrus.WithField("identifier", myIdentifier).Info("using specified RedisIdentifier from config")
	} else {
		// Otherwise, determine identifier from network interface.
		myIdentifier, err = getIdentifierFromInterfaces(c)
		if err != nil {
			return "", err
		}
	}

	publicListenAddr := fmt.Sprintf("http://%s:%s", myIdentifier, port)

	return publicListenAddr, nil
}

// Scan network interfaces to determine an identifier from either IP or hostname.
func getIdentifierFromInterfaces(c config.Config) (string, error) {
	myIdentifier, _ := os.Hostname()
	identifierInterfaceName := c.GetIdentifierInterfaceName()

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
			ipv6 := c.GetUseIPV6Identifier()
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

// equal tells whether a and b contain the same elements.
// A nil argument is equivalent to an empty slice.
// lifted from https://yourbasic.org/golang/compare-slices/
func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

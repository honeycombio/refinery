package config

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/honeycombio/samproxy/config/redimem"
)

// RedisPeerFileConfig uses a FileConfig for everything except getting Peers.
// Peers are negotiated via redis.
type RedisPeerFileConfig struct {
	FileConfig

	// RedisAddress is the connection string used to get us a valid redis to use to
	// store and check for peers
	RedisAddress string

	// peers is my local cache of the peer list. It gets updated by a background
	// ticker
	peers []string
	// peerLock protects the peers list while it's getting updated by redis
	peerLock sync.Mutex

	startOnce sync.Once

	// publicAddr is the public version of PeerListenAddr. The listen address
	// should be on 0.0.0.0 so it listens on all interfaces but the version pushed
	// to Redis must be addressable by _other_ hosts, so should be
	// http://hostname:port instead. This address is computed by looking up the
	// hostname and using the port from the PeerListenAddr and assuming HTTP. It is
	// cached here for re-registration.
	publicAddr string

	peerStore redimem.Membership
}

const (
	RedisHostEnvVarName  = "SAMPROXY_REDIS_HOST"
	refreshCacheInterval = 5 * time.Second
	peerEntryTimeout     = 30 * time.Second
)

// Start reads the config initially and spins up some goroutines to manage peer
// registration and status
func (rc *RedisPeerFileConfig) Start() error {
	var err error
	// the config is special in that it gets started once before all the other
	// dependencies are constructed so that the other dependencies may be specified
	// in the config file itself. After that when the dependency graph is built it
	// gets started again. We don't actually want to start it twice, so let's put
	// the entire start function in a sync.Once.
	rc.startOnce.Do(func() {
		// first read the rest of the config. This is equiv of the FileConfig.Start()
		// call.
		err = rc.FileConfig.reloadConfig()
		if err != nil {
			return
		}

		rc.RedisAddress, _ = rc.GetRedisHost()

		if rc.RedisAddress == "" {
			rc.RedisAddress = "localhost:6379"
		}

		pool := &redis.Pool{
			MaxIdle:     3,
			MaxActive:   30,
			IdleTimeout: 5 * time.Minute,
			Wait:        true,
			Dial: func() (redis.Conn, error) {
				return redis.Dial(
					"tcp", rc.RedisAddress,
					redis.DialReadTimeout(1*time.Second),
					redis.DialConnectTimeout(1*time.Second),
					redis.DialDatabase(0), // TODO enable multiple databases for multiple samproxies
				)
			},
		}

		rc.peerStore = &redimem.RedisMembership{
			Prefix: "samproxy",
			Pool:   pool,
		}

		// compute the public version of my peer listen address
		listenAddr, _ := rc.FileConfig.GetPeerListenAddr()
		port := strings.Split(listenAddr, ":")[1]
		myhostname, _ := os.Hostname()
		publicListenAddr := fmt.Sprintf("http://%s:%s", myhostname, port)
		rc.publicAddr = publicListenAddr

		// register myself once
		rc.peerStore.Register(context.TODO(), publicListenAddr, peerEntryTimeout)

		// go establish a regular registration heartbeat to ensure I stay alive in redis
		go rc.registerSelf()

		// get our peer list once to seed ourselves
		rc.updatePeerListOnce()

		// go watch the list of peers and trigger callbacks whenever it changes.
		// populate my local list of peers so each request can hit memory and only hit
		// redis on a ticker
		go rc.watchPeers()

	})
	return err
}

// GetPeers returns the locally cached list of peers that are in redis. It
// returns a copy so that it doesn't need to worry about protecting the list
// that gets retuned
func (rc *RedisPeerFileConfig) GetPeers() ([]string, error) {
	rc.peerLock.Lock()
	defer rc.peerLock.Unlock()
	retList := make([]string, len(rc.peers))
	copy(retList, rc.peers)
	return retList, nil
}

// GetRedisHost prefers to get the value from the environment and falls back to
// the config file if the environment variable is not set. Returns empty string
// if neither is set.
func (rc *RedisPeerFileConfig) GetRedisHost() (string, error) {
	envRedisHost := os.Getenv(RedisHostEnvVarName)
	if envRedisHost != "" {
		return envRedisHost, nil
	}
	return rc.FileConfig.GetRedisHost()
}

// registerSelf inserts self into the peer list and updates self's entry on a
// regular basis so it doesn't time out and get removed from the list of peers.
// If this function stops, this host will get ejected from other's peer lists.
func (rc *RedisPeerFileConfig) registerSelf() {
	tk := time.NewTicker(refreshCacheInterval)
	for range tk.C {
		// every 5 seconds, insert a 30sec timeout record
		rc.peerStore.Register(context.TODO(), rc.publicAddr, peerEntryTimeout)
	}
}

func (rc *RedisPeerFileConfig) watchPeers() {
	oldPeerList := rc.peers
	sort.Strings(oldPeerList)
	tk := time.NewTicker(refreshCacheInterval)

	defer func() {
		fmt.Printf("leaving watchPeers\n")
	}()

	for range tk.C {
		fmt.Printf("watchPeers: starting tick\n")
		currentPeers, err := rc.peerStore.GetMembers(context.TODO())
		if err != nil {
			fmt.Printf("watchPeers: got error %+v|n", err)
			// TODO maybe do something better here?
			continue
		}
		sort.Strings(currentPeers)
		if !equal(oldPeerList, currentPeers) {
			// update peer list and trigger callbacks saying the peer list has changed
			fmt.Printf("peer list has changed; new peer list +%v\n", currentPeers)
			rc.peerLock.Lock()
			rc.peers = currentPeers
			oldPeerList = currentPeers
			rc.peerLock.Unlock()
			for _, callback := range rc.callbacks {
				// don't block on any of the callbacks.
				go callback()
			}
		}
	}
}

func (rc *RedisPeerFileConfig) updatePeerListOnce() {
	currentPeers, err := rc.peerStore.GetMembers(context.TODO())
	if err != nil {
		// TODO maybe do something better here?
		return
	}
	sort.Strings(currentPeers)
	// update peer list and trigger callbacks saying the peer list has changed
	rc.peerLock.Lock()
	rc.peers = currentPeers
	rc.peerLock.Unlock()
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

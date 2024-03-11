package peer

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/sirupsen/logrus"
)

var (
	internalAddr string = fmt.Sprintf("%s.%s.internal", os.Getenv("FLY_REGION"), os.Getenv("FLY_APP_NAME"))
	peerPort     int    = 8193
)

type dnsPeers struct {
	c         config.Config
	peers     []string
	peerLock  sync.Mutex
	callbacks []func()
}

func newDnsPeers(c config.Config, done chan struct{}) (Peers, error) {
	peers := &dnsPeers{
		c: c,
	}
	peerList, err := peers.getFromDns()
	if err != nil {
		return nil, err
	}

	peers.peerLock.Lock()
	peers.peers = peerList
	peers.peerLock.Unlock()

	go peers.watchPeers(done)

	return peers, nil
}

func (p *dnsPeers) getFromDns() ([]string, error) {
	ips, err := net.LookupIP(internalAddr)
	if err != nil {
		return nil, err
	}

	var addrs []string
	for _, ip := range ips {
		addr := url.URL{
			Scheme: "http",
			Host:   net.JoinHostPort(ip.String(), strconv.Itoa(peerPort)),
		}
		addrs = append(addrs, addr.String())
	}

	return addrs, nil
}

func (p *dnsPeers) GetPeers() ([]string, error) {
	p.peerLock.Lock()
	defer p.peerLock.Unlock()
	retList := make([]string, len(p.peers))
	copy(retList, p.peers)
	return retList, nil
}

func (p *dnsPeers) watchPeers(done chan struct{}) {
	oldPeerList := p.peers
	sort.Strings(oldPeerList)
	tk := time.NewTicker(refreshCacheInterval)

	for {
		select {
		case <-tk.C:
			currentPeers, err := p.getFromDns()
			if err != nil {
				logrus.WithError(err).
					WithFields(logrus.Fields{
						"timeout":  p.c.GetPeerTimeout().String(),
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
		case <-done:
			p.peerLock.Lock()
			p.peers = []string{}
			p.peerLock.Unlock()
			return
		}
	}
}

func (p *dnsPeers) RegisterUpdatedPeersCallback(callback func()) {
	p.callbacks = append(p.callbacks, callback)
}

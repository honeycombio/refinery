package peer

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/honeycombio/refinery/internal/gossip"
	"github.com/jonboulle/clockwork"

	"github.com/sirupsen/logrus"
)

const (
	// keepAliveInterval is how frequently this host will announce itself
	// with the rest of the cluster. This is a heartbeat to ensure that the host
	// is still alive and participating in the cluster when no active peer info is
	// being sent.
	keepAliveInterval = 3 * time.Second

	// peerEntryTimeout is how long to wait before expiring a peer that
	// doesn't check in.
	peerEntryTimeout = 10 * time.Second

	defaultPeerInfoChannelSize = 20
)

var _ Peers = (*PeerStore)(nil)

type PeerStore struct {
	Gossip gossip.Gossiper `inject:"gossip"`
	Clock  clockwork.Clock `inject:""`

	done           chan struct{}
	identification string

	peerChan chan []byte

	peerLock sync.RWMutex
	peers    map[string]time.Time

	callbackLock sync.RWMutex
	callbacks    []func(PeerInfo)
	publishChan  chan PeerInfo

	wg sync.WaitGroup
}

func (p *PeerStore) Start() error {
	p.identification = p.HostID()
	p.peerChan = p.Gossip.Subscribe("peer-info", defaultPeerInfoChannelSize)
	p.publishChan = make(chan PeerInfo, defaultPeerInfoChannelSize)
	p.done = make(chan struct{})
	p.peers = make(map[string]time.Time)
	p.wg = sync.WaitGroup{}

	p.wg.Add(1)
	go p.watchPeers()
	p.wg.Add(1)
	go p.publish()

	return nil
}

func (p *PeerStore) Stop() error {
	close(p.done)
	fmt.Println("closing peer store")
	close(p.publishChan)
	p.wg.Wait()
	return nil
}

// Subscribe adds a callback to be called when new peer info is received.
func (p *PeerStore) Subscribe(callback func(PeerInfo)) {
	p.callbackLock.Lock()
	defer p.callbackLock.Unlock()
	p.callbacks = append(p.callbacks, callback)
}

// PublishPeerInfo sends peer info to the rest of the cluster.
func (p *PeerStore) PublishPeerInfo(info PeerInfo) error {
	info.id = p.identification
	select {
	case <-p.done:
		return errors.New("peer store has been stopped")
	case p.publishChan <- info:
		return nil
	default:
		return errors.New("failed to publish peer info")
	}
}

// GetPeerCount returns the number of active peers in the cluster.
// This includes the current host.
func (p *PeerStore) GetPeerCount() int {
	p.peerLock.Lock()
	defer p.peerLock.Unlock()

	for id, ts := range p.peers {
		if p.Clock.Since(ts) > peerEntryTimeout {
			delete(p.peers, id)
		}
	}

	// If we're the only peer, we should return 1.
	if len(p.peers) == 0 {
		return 1
	}

	return len(p.peers) + 1
}

func (p *PeerStore) HostID() string {
	if p.identification != "" {
		return p.identification
	}

	// We need to identify ourselves to the cluster. We'll use the hostname if we can, but if we can't, we'll use a UUID.
	if hostname, err := os.Hostname(); err == nil && hostname != "" {
		return hostname
	}
	if p.identification == "" {
		id, err := uuid.NewV7()
		if err != nil {
			panic("failed to generate a UUID for the StressRelief system")
		}
		return id.String()

	}

	return ""
}

// publish sends peer info from this host to the rest of the cluster.
// If there is no new peer info to send, it will send a heartbeat.
func (p *PeerStore) publish() {
	defer p.wg.Done()

	tk := p.Clock.NewTicker(keepAliveInterval)
	defer tk.Stop()

	for {
		var msg PeerInfo
		select {
		case <-p.done:
			return
		case data := <-p.publishChan:
			msg = data
			tk.Reset(keepAliveInterval)
		case <-tk.Chan():
			msg = PeerInfo{id: p.identification}
		default:
		}

		if msg.id == "" {
			continue
		}

		if err := p.Gossip.Publish("peer-info", msg.ToBytes()); err != nil {
			logrus.WithError(err).Error("failed to publish peer info")
		}
	}
}

// watchPeers listens for new peer info from the cluster and triggers callbacks.
func (p *PeerStore) watchPeers() {
	defer p.wg.Done()

	for {
		select {
		case msg := <-p.peerChan:
			info, err := newPeerInfoFromBytes(msg)
			if err != nil {
				logrus.WithError(err).Error("failed to decode peer info")
				continue
			}
			if info.id == p.identification {
				continue
			}

			p.callbackLock.RLock()
			for _, callback := range p.callbacks {
				// don't block on any of the callbacks.
				go callback(info)
			}
			p.callbackLock.RUnlock()

			p.peerLock.Lock()
			p.peers[info.id] = p.Clock.Now()
			p.peerLock.Unlock()

		case <-p.done:
			return
		}
	}
}

type MockPeerStore struct {
	PeerStore

	Identification string
	PeerCount      int
}

func (m *MockPeerStore) Start() error {
	m.PeerStore.identification = m.Identification
	return m.PeerStore.Start()
}

func (m *MockPeerStore) GetPeerCount() int {
	return m.PeerCount
}

package peer

import (
	"errors"
	"maps"
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

	defaultPeerInfoChannelSize = 100
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

	publishChan chan PeerInfo

	subscriptionLock sync.RWMutex
	subscriptions    []chan PeerInfo

	wg sync.WaitGroup
}

func (p *PeerStore) Start() error {
	p.identification = p.HostID()
	p.peerChan = p.Gossip.Subscribe(p.Gossip.GetChannel(gossip.ChannelPeer), defaultPeerInfoChannelSize)
	p.publishChan = make(chan PeerInfo, defaultPeerInfoChannelSize)
	p.done = make(chan struct{})
	p.peers = make(map[string]time.Time)
	p.wg = sync.WaitGroup{}
	p.subscriptions = make([]chan PeerInfo, 0)

	p.wg.Add(1)
	go p.watchPeers()
	p.wg.Add(1)
	go p.publish()

	return nil
}

func (p *PeerStore) Stop() error {
	close(p.done)
	close(p.publishChan)
	p.wg.Wait()
	return nil
}

// Subscribe adds a callback to be called when new peer info is received.
func (p *PeerStore) Subscribe() <-chan PeerInfo {
	ch := make(chan PeerInfo, defaultPeerInfoChannelSize)
	p.subscriptionLock.Lock()
	p.subscriptions = append(p.subscriptions, ch)
	p.subscriptionLock.Unlock()
	return ch
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

	maps.DeleteFunc(p.peers, func(id string, ts time.Time) bool {
		return p.Clock.Since(ts) > peerEntryTimeout
	})

	// If we're the only peer, we won't be in the peer list, so we should return 1.
	if len(p.peers) == 0 {
		return 1
	}

	return len(p.peers) + 1
}

// HostID returns the unique identifier for this host.
// If the host has a hostname, it will be used as part of the identifier.
// Otherwise, a UUID will be generated.
func (p *PeerStore) HostID() string {
	if p.identification != "" {
		return p.identification
	}

	var identification string
	hostname, err := os.Hostname()
	if err == nil && hostname != "" {
		identification = hostname
	}
	id, err := uuid.NewV7()
	if err != nil {
		panic("failed to generate a UUID for the StressRelief system")
	}

	if identification == "" {
		return id.String()
	}

	return identification + "-" + id.String()
}

// publish sends peer info from this host to the rest of the cluster.
// If there is no new peer info to send, it will send a heartbeat.
func (p *PeerStore) publish() {
	defer p.wg.Done()

	tk := p.Clock.NewTicker(keepAliveInterval)
	defer tk.Stop()
	peerChan := p.Gossip.GetChannel(gossip.ChannelPeer)

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
		}

		if err := p.Gossip.Publish(peerChan, msg.Bytes()); err != nil {
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

			// Ignore our own messages.
			if info.id == p.identification {
				continue
			}

			p.peerLock.Lock()
			p.peers[info.id] = p.Clock.Now()
			p.peerLock.Unlock()

			if len(info.Data) == 0 {
				continue
			}
			p.subscriptionLock.RLock()
			for _, ch := range p.subscriptions {
				ch <- info
			}
			p.subscriptionLock.RUnlock()

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

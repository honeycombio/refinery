package peer

import (
	"sync"
)

var _ Peers = (*MockPeers)(nil)

type MockPeers struct {
	mut       sync.RWMutex
	peers     []string
	id        string
	callbacks []func()
}

func NewMockPeers(peers []string, id string) *MockPeers {
	return &MockPeers{
		peers: peers,
		id:    id,
	}
}

func (p *MockPeers) GetPeers() ([]string, error) {
	p.mut.RLock()
	defer p.mut.RUnlock()
	return p.peers, nil
}

func (p *MockPeers) GetInstanceID() (string, error) {
	p.mut.RLock()
	defer p.mut.RUnlock()

	return p.id, nil
}

func (p *MockPeers) RegisterUpdatedPeersCallback(callback func()) {
	p.mut.Lock()
	defer p.mut.Unlock()
	p.callbacks = append(p.callbacks, callback)
}

func (p *MockPeers) Start() error {
	p.mut.Lock()
	defer p.mut.Unlock()
	if len(p.id) == 0 && len(p.peers) > 0 {
		p.id = p.peers[0]
	}
	return nil
}

func (p *MockPeers) Ready() error {
	return nil
}

func (p *MockPeers) TriggerCallbacks() {
	var callbacks []func()
	p.mut.RLock()
	copy(callbacks, p.callbacks)
	p.mut.RUnlock()

	if callbacks != nil {
		for _, c := range callbacks {
			c()
		}
	}
}

// UpdatePeers changes the peer list and triggers callbacks
func (p *MockPeers) UpdatePeers(newPeers []string) {
	p.mut.Lock()
	p.peers = newPeers
	if len(newPeers) > 0 && p.id == "" {
		p.id = newPeers[0]
	}
	callbacks := make([]func(), len(p.callbacks))
	copy(callbacks, p.callbacks)
	p.mut.Unlock()

	// Trigger callbacks outside the lock
	for _, callback := range callbacks {
		callback()
	}
}

package peer

import (
	"math/rand"
	"strconv"
)

var _ Peers = (*MockPeers)(nil)

type MockPeers struct {
	Peers []string
	ID    string
}

func (p *MockPeers) GetPeers() ([]string, error) {
	return p.Peers, nil
}

func (p *MockPeers) GetInstanceID() (string, error) {
	return p.ID, nil
}

func (p *MockPeers) RegisterUpdatedPeersCallback(callback func()) {
	callback()
}

func (p *MockPeers) Start() error {
	if len(p.ID) == 0 {
		p.ID = strconv.Itoa(rand.Intn(10000))
	}
	return nil
}

func (p *MockPeers) Stop() error {
	return nil
}

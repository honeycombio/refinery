package peer

type MockPeers struct {
	Peers []string
}

// ensure that MockPeers implements the Peers interface
var _ Peers = (*MockPeers)(nil)

func (p *MockPeers) GetPeers() ([]string, error) {
	return p.Peers, nil
}

func (p *MockPeers) RegisterUpdatedPeersCallback(callback func()) {
	callback()
}

func (p *MockPeers) Start() error {
	return nil
}

func (p *MockPeers) Stop() error {
	return nil
}

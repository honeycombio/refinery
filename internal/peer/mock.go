package peer

type MockPeers struct {
	Peers []string
}

func (p *MockPeers) GetPeers() ([]string, error) {
	return p.Peers, nil
}
func (p *MockPeers) RegisterUpdatedPeersCallback(callback func()) {
	callback()
}

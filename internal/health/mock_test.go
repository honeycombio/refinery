package health

import "sync"

type MockHealthReporter struct {
	isAlive bool
	isReady bool
	mutex   sync.Mutex
}

func (m *MockHealthReporter) SetAlive(isAlive bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.isAlive = isAlive
}

func (m *MockHealthReporter) IsAlive() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.isAlive
}

func (m *MockHealthReporter) SetReady(isReady bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.isReady = isReady
}

func (m *MockHealthReporter) IsReady() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.isReady
}

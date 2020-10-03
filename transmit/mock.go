package transmit

import (
	"sync"

	"github.com/honeycombio/refinery/types"
)

type MockTransmission struct {
	Events []*types.Event
	Mux    sync.RWMutex
}

func (m *MockTransmission) Start() error {
	m.Events = make([]*types.Event, 0)
	return nil
}

func (m *MockTransmission) EnqueueEvent(ev *types.Event) {
	m.Mux.Lock()
	defer m.Mux.Unlock()
	m.Events = append(m.Events, ev)
}
func (m *MockTransmission) EnqueueSpan(ev *types.Span) {
	m.Mux.Lock()
	defer m.Mux.Unlock()
	m.Events = append(m.Events, &ev.Event)
}
func (m *MockTransmission) Flush() {
	m.Mux.Lock()
	defer m.Mux.Unlock()
	m.Events = m.Events[:0]
}

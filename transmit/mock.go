package transmit

import (
	"github.com/honeycombio/refinery/types"
)

type MockTransmission struct {
	Events   chan *types.Event
	Capacity int
}

func (m *MockTransmission) Start() error {
	if m.Capacity == 0 {
		m.Capacity = 100
	}
	m.Events = make(chan *types.Event, m.Capacity)
	return nil
}

func (m *MockTransmission) Stop() error {
	close(m.Events)
	return nil
}

func (m *MockTransmission) Get(expectedCount int) []*types.Event {
	events := make([]*types.Event, 0, len(m.Events))
	for {
		select {
		case ev := <-m.Events:
			events = append(events, ev)
		default:
			if len(events) != expectedCount {
				continue
			}
			return events
		}
	}
}

func (m *MockTransmission) EnqueueEvent(ev *types.Event) {
	m.Events <- ev
}
func (m *MockTransmission) EnqueueSpan(ev *types.Span) {
	m.Events <- &ev.Event
}
func (m *MockTransmission) Flush() {
	m.Events = make(chan *types.Event, m.Capacity)
}

func (m *MockTransmission) RegisterMetrics() {}

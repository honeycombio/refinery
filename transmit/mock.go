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
	for {
		select {
		case <-m.Events:
		default:
			return nil
		}
	}
	return nil
}

// GetBlock will return up to `expectedCount` events from the channel. If there are
// fewer than `expectedCount` events in the channel, it will block until there
// are enough events to return.
// If `expectedCount` is 0, it will retry up to 3 times before returning the
// events that are in the channel.
func (m *MockTransmission) GetBlock(expectedCount int) []*types.Event {
	events := make([]*types.Event, 0, len(m.Events))
	var maxDelayCount int
	if expectedCount == 0 {
		maxDelayCount = 3
	}

	for {
		select {
		case ev := <-m.Events:
			events = append(events, ev)
		default:
			if maxDelayCount > 0 {
				maxDelayCount--
				continue
			}
		}

		if len(events) != expectedCount {
			continue
		}
		return events
	}
}

func (m *MockTransmission) EnqueueEvent(ev *types.Event) {
	m.Events <- ev
}
func (m *MockTransmission) EnqueueSpan(ev *types.Span) {
	m.Events <- &ev.Event
}
func (m *MockTransmission) Flush() {
	return
}

func (m *MockTransmission) RegisterMetrics() {}

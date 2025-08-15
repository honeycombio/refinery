package transmit

import (
	"time"

	"github.com/honeycombio/refinery/types"
)

type MockTransmission struct {
	Events   chan *types.Event
	Capacity int
	WaitTime time.Duration
}

func (m *MockTransmission) Start() error {
	if m.Capacity == 0 {
		m.Capacity = 100
	}
	if m.WaitTime == 0 {
		m.WaitTime = 100 * time.Millisecond
	}
	m.Events = make(chan *types.Event, m.Capacity)
	return nil
}

func (m *MockTransmission) Stop() error {
	for {
		select {
		case <-m.Events:
		default:
			close(m.Events)
			return nil
		}
	}
}

// GetBlock will return up to `expectedCount` events from the channel. If there are
// fewer than `expectedCount` events in the channel, it will block until there
// are enough events to return.
// If `expectedCount` is 0, will wait WaitTime for events.
func (m *MockTransmission) GetBlock(expectedCount int) []*types.Event {
	events := make([]*types.Event, 0, len(m.Events))
	var expiry <-chan time.Time

	// Initialize ticker only if expectedCount is zero
	if expectedCount == 0 {
		expiry = time.After(m.WaitTime)
	}

	for {
		select {
		case ev := <-m.Events:
			events = append(events, ev)

			// If we have collected enough events, return
			if expectedCount > 0 && len(events) == expectedCount {
				return events
			}
		case <-expiry:
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

func (m *MockTransmission) RegisterMetrics() {}

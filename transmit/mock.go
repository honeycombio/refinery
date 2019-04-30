package transmit

import (
	"github.com/honeycombio/samproxy/types"
)

type MockTransmission struct {
	Events []*types.Event
}

func (m *MockTransmission) Start() error {
	m.Events = make([]*types.Event, 0)
	return nil
}

func (m *MockTransmission) EnqueueEvent(ev *types.Event) { m.Events = append(m.Events, ev) }
func (m *MockTransmission) EnqueueSpan(ev *types.Span)   { m.Events = append(m.Events, &ev.Event) }
func (m *MockTransmission) Flush()                       {}

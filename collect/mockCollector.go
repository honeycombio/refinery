package collect

import (
	"github.com/honeycombio/refinery/types"
)

type MockCollector struct {
	Spans chan *types.Span
}

func NewMockCollector() *MockCollector {
	return &MockCollector{
		Spans: make(chan *types.Span, 100),
	}
}

func (m *MockCollector) AddSpan(span *types.Span) error {
	m.Spans <- span
	return nil
}

func (m *MockCollector) AddSpanFromPeer(span *types.Span) error {
	m.Spans <- span
	return nil
}

func (m *MockCollector) GetStressedSampleRate(traceID string) (rate uint, keep bool, reason string) {
	return 0, false, ""
}

func (m *MockCollector) ProcessSpanImmediately(sp *types.Span) (bool, bool) {
	m.Spans <- sp

	return true, true
}

func (m *MockCollector) Stressed() bool {
	return false
}

func (m *MockCollector) Flush() {
	for {
		select {
		case <-m.Spans:
		default:
			return
		}
	}
}

var _ Collector = (*MockCollector)(nil)

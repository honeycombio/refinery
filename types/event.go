package types

import (
	"context"
	"sync"
	"time"
)

const (
	APIKeyHeader     = "X-Honeycomb-Team"
	SampleRateHeader = "X-Honeycomb-Samplerate"
	TimestampHeader  = "X-Honeycomb-Event-Time"
)

// event is not part of a trace - it's an event that showed up with no trace ID
type Event struct {
	Context    context.Context
	APIHost    string
	APIKey     string
	Dataset    string
	SampleRate uint
	Timestamp  time.Time
	Data       map[string]interface{}
}

// Trace isn't something that shows up on the wire; it gets created within Samproxy
type Trace struct {
	APIHost string
	APIKey  string
	Dataset string
	TraceID string
	Spans   []*Span

	// SendSampleLock protects the SampleRate and KeepSample attributes of the
	// trace because they should be modified together. It is accessed from here
	// and from within the collector.
	SendSampleLock sync.Mutex
	// SampleRate should only be changed if the changer holds the SendSampleLock
	SampleRate uint
	// KeepSample should only be changed if the changer holds the SendSampleLock
	KeepSample bool
	// Sent should only be changed if the changer holds the SendSampleLock
	Sent bool

	// StartTime is the server time when the first span arrived for this trace.
	// Used to calculate how long traces spend sitting in Samproxy
	StartTime time.Time
	// FinishTime is when a trace is prepared for sending; it does not include
	// any additional delay imposed by the DelaySend config option.
	FinishTime time.Time

	// CanceSending is a cancel function to abort the trace timeout if we send
	// or sample this trace so that we're not sitting around with tons of
	// goroutines waiting a full minute (or whatever the trace timeout is) then
	// doing nothing.
	CancelSending context.CancelFunc
}

// GetSent returns true if this trace has already been sent, false if it has not
// yet been sent.
func (t *Trace) GetSent() bool {
	t.SendSampleLock.Lock()
	defer t.SendSampleLock.Unlock()
	return t.Sent
}

// Span is an event that shows up with a trace ID, so will be part of a Trace
type Span struct {
	Event
	TraceID string
}

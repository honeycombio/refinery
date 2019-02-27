package types

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	APIKeyHeader = "X-Honeycomb-Team"
	// libhoney-js uses this
	APIKeyHeaderShort = "X-Hny-Team"
	SampleRateHeader  = "X-Honeycomb-Samplerate"
	TimestampHeader   = "X-Honeycomb-Event-Time"
)

// used to put a request ID into the request context for logging
type RequestIDContextKey struct{}

var TraceAlreadySent = errors.New("Can't add span; trace has already been sent.")

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

	// spanListLock protects multiple accessors to the list of spans in this
	// trace
	spanListLock sync.Mutex

	// spans is the list of spans in this trace, protected by the list lock
	spans []*Span
}

// GetSent returns true if this trace has already been sent, false if it has not
// yet been sent.
func (t *Trace) GetSent() bool {
	t.SendSampleLock.Lock()
	defer t.SendSampleLock.Unlock()
	return t.Sent
}

// AddSpan adds a span to this trace
func (t *Trace) AddSpan(sp *Span) error {
	t.SendSampleLock.Lock()
	defer t.SendSampleLock.Unlock()
	if t.Sent {
		return TraceAlreadySent
	}
	t.spanListLock.Lock()
	defer t.spanListLock.Unlock()
	t.spans = append(t.spans, sp)
	return nil
}

// GetSpans returns the list of spans in this trace
func (t *Trace) GetSpans() []*Span {
	t.spanListLock.Lock()
	defer t.spanListLock.Unlock()
	// since we only ever append to this list, we can return a new reference to
	// the slice as it exists now and it will be safe for concurrent reads.
	return t.spans[:]

}

// Span is an event that shows up with a trace ID, so will be part of a Trace
type Span struct {
	Event
	TraceID string
}

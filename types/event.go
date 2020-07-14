package types

import (
	"context"
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

type EventType int

const (
	EventTypeUnknown EventType = iota
	EventTypeSpan
	EventTypeEvent
)

func (et EventType) String() string {
	switch et {
	case EventTypeUnknown:
		return "unknown"
	case EventTypeSpan:
		return "span"
	case EventTypeEvent:
		return "event"
	}
	return "unknown_event_type"
}

type TargetType int

const (
	TargetUnknown TargetType = iota
	TargetPeer
	TargetUpstream
)

func (tt TargetType) String() string {
	switch tt {
	case TargetUnknown:
		return "unknown"
	case TargetPeer:
		return "peer"
	case TargetUpstream:
		return "upstream"
	}
	return "unknown_target_type"
}

// event is not part of a trace - it's an event that showed up with no trace ID
type Event struct {
	Context    context.Context
	Type       EventType
	Target     TargetType
	APIHost    string
	APIKey     string
	Dataset    string
	SampleRate uint
	Timestamp  time.Time
	Data       map[string]interface{}
}

// Trace isn't something that shows up on the wire; it gets created within
// Samproxy. Traces are not thread-safe; only one goroutine should be working
// with a trace object at a time.
type Trace struct {
	APIHost string
	APIKey  string
	Dataset string
	TraceID string

	// SampleRate should only be changed if the changer holds the SendSampleLock
	SampleRate uint
	// KeepSample should only be changed if the changer holds the SendSampleLock
	KeepSample bool
	// Sent should only be changed if the changer holds the SendSampleLock
	Sent bool

	// If non-zero, deadline to send this trace by.
	SendBy time.Time

	// StartTime is the server time when the first span arrived for this trace.
	// Used to calculate how long traces spend sitting in Samproxy
	StartTime time.Time
	// FinishTime is when a trace is prepared for sending; it does not include
	// any additional delay imposed by the DelaySend config option.
	FinishTime time.Time

	// spans is the list of spans in this trace
	spans []*Span
}

// GetSent returns true if this trace has already been sent, false if it has not
// yet been sent.
func (t *Trace) GetSent() bool {
	return t.Sent
}

// AddSpan adds a span to this trace
func (t *Trace) AddSpan(sp *Span) {
	t.spans = append(t.spans, sp)
}

// GetSpans returns the list of spans in this trace
func (t *Trace) GetSpans() []*Span {
	return t.spans
}

// SetDeadline sets the deadline after which this trace should be sent. It
// will get sent sometime after the deadline expires (by a ticker check) but
// not before. If a deadline has already been set, this call will only shrink
// the deadline, not expand it. 
func (t *Trace) SetDeadline(d time.Duration) {
	deadline := time.Now().Add(d)
	if t.SendBy.IsZero() || t.SendBy.After(deadline) {
		t.SendBy = deadline
	}
}

// Span is an event that shows up with a trace ID, so will be part of a Trace
type Span struct {
	Event
	TraceID string
}

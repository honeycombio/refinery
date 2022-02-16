package types

import (
	"context"
	"time"
)

const (
	APIKeyHeader = "X-Honeycomb-Team"
	// libhoney-js uses this
	APIKeyHeaderShort = "X-Hny-Team"
	DatasetHeader     = "X-Honeycomb-Dataset"
	SampleRateHeader  = "X-Honeycomb-Samplerate"
	TimestampHeader   = "X-Honeycomb-Event-Time"
)

// used to put a request ID into the request context for logging
type RequestIDContextKey struct{}

// event is not part of a trace - it's an event that showed up with no trace ID
type Event struct {
	Context     context.Context
	APIHost     string
	APIKey      string
	Dataset     string
	Environment string
	SampleRate  uint
	Timestamp   time.Time
	Data        map[string]interface{}
}

// Trace isn't something that shows up on the wire; it gets created within
// Refinery. Traces are not thread-safe; only one goroutine should be working
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

	SendBy time.Time

	// StartTime is the server time when the first span arrived for this trace.
	// Used to calculate how long traces spend sitting in Refinery
	StartTime time.Time

	HasRootSpan bool

	// spans is the list of spans in this trace
	spans []*Span
}

// AddSpan adds a span to this trace
func (t *Trace) AddSpan(sp *Span) {
	t.spans = append(t.spans, sp)
}

// GetSpans returns the list of spans in this trace
func (t *Trace) GetSpans() []*Span {
	return t.spans
}

func (t *Trace) GetSamplerKey() (string, bool) {
	if IsLegacyAPIKey(t.APIKey) {
		return t.Dataset, true
	}

	env := ""
	for _, sp := range t.GetSpans() {
		if sp.Event.Environment != "" {
			env = sp.Event.Environment
			break
		}
	}

	return env, false
}

// Span is an event that shows up with a trace ID, so will be part of a Trace
type Span struct {
	Event
	TraceID string
}

func IsLegacyAPIKey(apiKey string) bool {
	return len(apiKey) == 32
}

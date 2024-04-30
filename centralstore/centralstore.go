package centralstore

import (
	"context"
	"time"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/types"
)

// CentralSpan is the subset of a span that is sent to the central store.
// If AllFields is non-nil, it contains all the fields from the original span; this
// is used when a refinery needs to shut down; it can forward all its spans to the central
// store for forwarding to whichever refinery makes the eventual decision.
// IsRoot should be set to true if the span is the root of the trace (we don't ask the store
// to make this decision; the refinery should know this).
type CentralSpan struct {
	TraceID         string
	SpanID          string // need access to this field for updating all fields
	samplerSelector string
	Type            types.SpanType
	KeyFields       map[string]interface{}
	AllFields       map[string]interface{}
	IsRoot          bool
}

func (s *CentralSpan) Fields() map[string]interface{} {
	if s.KeyFields == nil {
		return s.AllFields
	}

	return s.KeyFields
}

func (s *CentralSpan) SetSamplerSelector(key string) {
	s.samplerSelector = key
}

type CentralTraceState string

const (
	Unknown          CentralTraceState = "unknown"
	Collecting       CentralTraceState = "collecting"
	DecisionDelay    CentralTraceState = "decision_delay"
	ReadyToDecide    CentralTraceState = "ready_to_decide"
	AwaitingDecision CentralTraceState = "awaiting_decision"
	DecisionKeep     CentralTraceState = "decision_keep"
	DecisionDrop     CentralTraceState = "decision_drop"
)

func (s CentralTraceState) String() string {
	return string(s)
}

type CentralTraceStatus struct {
	TraceID         string
	State           CentralTraceState
	Rate            uint
	Metadata        map[string]interface{}
	KeepReason      string
	SamplerSelector string
	reasonIndex     uint      // this is the cache ID for the reason
	Timestamp       time.Time // this is the last time the trace state was changed
	Count           uint32    // number of spans in the trace
	EventCount      uint32    // number of span events in the trace
	LinkCount       uint32    // number of span links in the trace
}

// ensure that CentralTraceStatus implements KeptTrace
var _ cache.KeptTrace = (*CentralTraceStatus)(nil)

func NewCentralTraceStatus(traceID string, state CentralTraceState, timestamp time.Time) *CentralTraceStatus {
	return &CentralTraceStatus{
		TraceID:   traceID,
		State:     state,
		Timestamp: timestamp,
		Metadata:  make(map[string]interface{}),
	}
}

func (s *CentralTraceStatus) Clone() *CentralTraceStatus {
	c := *s
	c.Metadata = make(map[string]interface{}, len(s.Metadata))
	for k, v := range s.Metadata {
		c.Metadata[k] = v
	}
	return &c
}

type CentralTrace struct {
	TraceID   string
	Timestamp uint64
	Root      *CentralSpan
	Spans     []*CentralSpan
}

func (t *CentralTrace) ID() string {
	return t.TraceID
}

func (t *CentralTrace) RootFields() types.Fielder {
	if t.Root == nil {
		return nil
	}
	return t.Root
}

func (t *CentralTrace) AllFields() []types.Fielder {
	fields := make([]types.Fielder, 0, len(t.Spans))
	for _, span := range t.Spans {
		fields = append(fields, span)
	}
	return fields
}

// ensure that CentralTraceStatus implements the KeptTrace interface
var _ cache.KeptTrace = (*CentralTraceStatus)(nil)

func (t *CentralTraceStatus) ID() string {
	return t.TraceID
}

func (t *CentralTraceStatus) SampleRate() uint {
	return uint(t.Rate)
}

func (t *CentralTraceStatus) DescendantCount() uint32 {
	return t.Count + t.EventCount + t.LinkCount
}

func (t *CentralTraceStatus) SpanEventCount() uint32 {
	return t.EventCount
}

func (t *CentralTraceStatus) SpanLinkCount() uint32 {
	return t.LinkCount
}

func (t *CentralTraceStatus) SpanCount() uint32 {
	return t.Count
}

func (t *CentralTraceStatus) SetSentReason(reason uint) {
	t.reasonIndex = reason
}

func (t *CentralTraceStatus) SentReason() uint {
	return t.reasonIndex
}

// SmartStorer is the interface that an intelligent central store must implement.
// This is the data storage interface used by the trace decision engine; it
// is not the trace decision engine itself.
type SmartStorer interface {
	// Start should be called once at Refinery startup to start the store.
	Start() error
	// Stop should be called once at Refinery shutdown to shut things down.
	Stop() error

	// WriteSpan writes one or more CentralSpans to the CentralStore.
	// It is valid to write a span that has already been written; this can happen
	// on shutdown, when a refinery forwards all its remaining spans to the central store.
	// The only error that can be returned is if the queue is full. This method is non-blocking.
	WriteSpan(ctx context.Context, span *CentralSpan) error

	// GetTrace fetches the current state of a trace (including all its spans)
	// from the central store. The trace contains a list of CentralSpans, but
	// note that these spans will usually only contain the key fields. The spans
	// returned from this call should be used for making the trace decision;
	// they should not be sent as telemetry unless AllFields is non-null. If the
	// trace has a root span, it will be the first span in the list.
	// GetTrace is intended to be used to make a trace decision, so
	// it has the side effect of moving a trace from ReadyForDecision to
	// AwaitingDecision. If the trace is not in the ReadyForDecision state,
	// its state will not be changed.
	GetTrace(ctx context.Context, traceID string) (*CentralTrace, error)

	// GetStatusForTraces returns the current state for a list of traces if they
	// match any of the states passed in, including any reason information if
	// the trace decision has been made and it was to keep the trace. If a trace
	// is not found in any of the listed states, it will be not be returned. Any
	// traces with a state of DecisionKeep or DecisionDrop should be considered
	// to be final and appropriately disposed of; the central store will not
	// change the state of these traces after this call.
	GetStatusForTraces(ctx context.Context, traceIDs []string, statesToCheck []CentralTraceState) ([]*CentralTraceStatus, error)

	// GetTracesForState returns a list of trace IDs that match the provided status.
	GetTracesForState(ctx context.Context, state CentralTraceState) ([]string, error)

	// GetTracesNeedingDecision returns a list of up to n trace IDs that are in the
	// ReadyForDecision state. These IDs are moved to the AwaitingDecision state
	// atomically, so that no other refinery will be assigned the same trace.
	GetTracesNeedingDecision(ctx context.Context, n int) ([]string, error)

	// SetTraceStatuses sets the status of a set of traces in the central store.
	// This is used to record the decision made by the trace decision engine. If
	// the state is DecisionKeep, the reason should be provided; if the state is
	// DecisionDrop, the reason should be empty as it will be ignored. Note that
	// the SmartStorer is permitted to manipulate the state of the trace after
	// this call, so the caller should not assume that the state persists as
	// set.
	SetTraceStatuses(ctx context.Context, statuses []*CentralTraceStatus) error

	// RecordMetrics Populates metric data from the smart store.
	RecordMetrics(ctx context.Context) error

	// RecordTraceDecision records the decision made by the trace decision engine.
	RecordTraceDecision(ctx context.Context, trace *CentralTraceStatus, keep bool, reason string) error
}

// Spec for the central store's internal behavior:
//
// * Refinery registers itself with a unique ID with the central store on startup; central store records
//   that ID. All transactions to the store include the ID, which allows the store to record and attribute
// 	 all the actions (for telemetry and debugging).
// * As spans are written to the interface, they get queued up and sent to the central store in batches.
// * The store organizes spans into traces, and maintains state information for each trace.
//
// The states are as follows:
// * Collecting: the trace is still being collected; spans are being added to it. This is the initial state of a trace
// when it is first seen (unless the trace includes the root span on the first request, in which case it will start in WaitingToDecide).
// * DecisionDelay: either the trace timeout has expired or the root span has arrived, and is in the delay period before making a decision.
// * ReadyForDecision — The DecisionDelay timeout has expired and the trace has been assigned to a refinery for a decision.
//   Any trace in this state beyond a maximum times out and reassigns to a different refinery.
// * AwaitingDecision — A refinery has requested the trace and should return with the actual decision.
//   If a timeout expires the trace is returned to ReadyForDecision and assigned to a different refinery; any decision received
//   from the non-owning refinery after the timeout is ignored.
// * Decision:Keep — this decision has been received from the refinery, and moves to a state that includes recorded trace metadata and sample rate information
// * Decision:Drop — this decision forgets all trace information except the traceID (we typically use something like a Bloom filter to track it)

// We expect a remote store to collect spans and manage the state of traces.
// Data structures it has to provide:
// - a list of spans for a trace
// - a record of the state of a trace and its metadata (span count, etc)
//
// It should be able to:
// - receive a span and add it to the appropriate trace, creating the trace if necessary
// - count spans and metadata about them
// - return the entire state of an individual trace
// - return the IDs of traces in a given state (e.g. "WaitingToDecide")
// - move a trace atomically from one state to another
// - return the IDs of traces that have been in a given state for a given time
// - return metrics about the traces it has seen

// BasicStorer is the interface for a non-intelligent remote store.
// It will be injected, so also has to implement the Stopper and Starter interfaces from stopstart.
type BasicStorer interface {
	// Start should be called once at Refinery startup to start the store.
	Start() error
	// Stop should be called once at Refinery shutdown to shut things down.
	Stop() error

	// WriteSpan writes a span to the store. It must always contain TraceID.
	// If this is a span containing any non-empty key fields, it must also contain
	// SpanID (and ParentID if it is not a root span).
	// For span events and span links, it may contain only the TraceID and the SpanType field;
	// these are counted but not stored.
	// Root spans should always be sent and must contain at least SpanID, and have the IsRoot flag set.
	// AllFields is optional and is used during shutdown.
	// WriteSpan may be asynchronous and will only return an error if the span could not be written.
	WriteSpan(ctx context.Context, span *CentralSpan) error

	// GetTrace fetches the current state of a trace (including all of its
	// spans) from the central store. The trace contains a list of CentralSpans,
	// and these spans will usually (but not always) only contain the key
	// fields. The spans returned from this call should be used for making the
	// trace decision; they should not be sent as telemetry unless AllFields is
	// non-null, in which case these spans should be sent if the trace decision
	// is Keep. If the trace has a root span, the Root property will be
	// populated. Normally this call will be made after Refinery has been asked
	// to make a trace decision.
	GetTrace(ctx context.Context, traceID string) (*CentralTrace, error)

	// GetStatusForTraces returns the current state for a list of traces if they
	// match any of the states passed in, including any reason information if
	// the trace decision has been made and it was to keep the trace. If a trace
	// is not found in any of the listed states, it will be not be returned. Any
	// traces with a state of DecisionKeep or DecisionDrop should be considered
	// to be final and appropriately disposed of; the central store will not
	// change the state of these traces after this call.
	GetStatusForTraces(ctx context.Context, traceIDs []string, statesToCheck []CentralTraceState) ([]*CentralTraceStatus, error)

	// GetTracesForState returns a list of trace IDs that match the provided status.
	GetTracesForState(ctx context.Context, state CentralTraceState) ([]string, error)

	// GetTracesNeedingDecision returns a list of up to n trace IDs that are in the
	// ReadyForDecision state. These IDs are moved to the AwaitingDecision state
	// atomically, so that no other refinery will be assigned the same trace.
	GetTracesNeedingDecision(ctx context.Context, n int) ([]string, error)

	// ChangeTraceStatus changes the status of a set of traces from one state to another
	// atomically. This can be used for all trace states except transition to Keep.
	// This call updates the timestamps in the trace status.
	ChangeTraceStatus(ctx context.Context, traceIDs []string, fromState, toState CentralTraceState) error

	// KeepTraces changes the status of a set of traces from AwaitingDecision to Keep;
	// it is used to record the keep decisions made by the trace decision engine.
	// Statuses should include Reason and Rate; do not include State as it will be ignored.
	KeepTraces(ctx context.Context, statuses []*CentralTraceStatus) error

	// RecordMetrics Populates metric data from the basic store.
	RecordMetrics(ctx context.Context) error

	//RecordTraceDecision records the decision made by the trace decision engine.
	RecordTraceDecision(ctx context.Context, trace *CentralTraceStatus, keep bool, reason string) error
}

package centralstore

import (
	"fmt"
	"sync"
	"time"
)

// LocalRemoteStore (yes, a contradiction in terms, deal with it) is a remote
// store that is local to the Refinery process. This is used when there is only one
// refinery in the system, and for testing and benchmarking.
type LocalRemoteStore struct {
	// states holds the current state of each trace in a map of the different states
	// indexed by trace ID.
	states map[CentralTraceState]statusMap
	// traces holds the current data for each trace
	traces map[string]*CentralTrace
	mutex  sync.RWMutex
}

// NewLocalRemoteStore returns a new LocalRemoteStore.
func NewLocalRemoteStore() *LocalRemoteStore {
	lrs := &LocalRemoteStore{
		states: make(map[CentralTraceState]statusMap),
		traces: make(map[string]*CentralTrace),
	}
	var cts CentralTraceState
	for _, state := range cts.ValidStates() {
		// initialize the map for each state
		lrs.states[state] = make(statusMap)
	}
	return lrs
}

// ensure that LocalRemoteStore implements RemoteStore
var _ RemoteStore = (*LocalRemoteStore)(nil)

// findTraceStatus returns the state and status of a trace, or Unknown if the trace
// wasn't found in any state. If the trace is found, the status will be non-nil.
// Only call this if you're holding at least an RLock on i.mut.
func (lrs *LocalRemoteStore) findTraceStatus(traceID string) (CentralTraceState, *CentralTraceStatus) {
	for state, statuses := range lrs.states {
		if status, ok := statuses[traceID]; ok {
			return state, status
		}
	}
	return Unknown, nil
}

// changes the status of a trace; if fromState is Unknown, the trace will be searched for
// in all states. If the trace wasn't found, false will be returned.
// Only call this if you're holding a Lock on i.mut.
func (lrs *LocalRemoteStore) changeTraceState(traceID string, fromState, toState CentralTraceState) bool {
	var status *CentralTraceStatus
	var ok bool
	if fromState == Unknown {
		fromState, status = lrs.findTraceStatus(traceID)
		if fromState == Unknown {
			return false
		}
	} else {
		if status, ok = lrs.states[fromState][traceID]; !ok {
			return false
		}
	}

	status.State = toState
	lrs.states[toState][traceID] = status
	status.timestamp = time.Now()
	delete(lrs.states[fromState], traceID)
	return true
}

// WriteSpan writes a span to the store. It must always contain TraceID.
// If this is a span containing any non-empty key fields, it must also contain
// SpanID (and ParentID if it is not a root span).
// For span events and span links, it may contain only the TraceID and the SpanType field;
// these are counted but not stored.
// Root spans should always be sent and must contain at least SpanID, and have the IsRoot flag set.
// AllFields is optional and is used during shutdown.
// WriteSpan is asynchronous and will only return an error if the span could not be written.
func (lrs *LocalRemoteStore) WriteSpan(span *CentralSpan) error {
	lrs.mutex.Lock()
	defer lrs.mutex.Unlock()

	// we have to find the state and decide what to do based on that
	state, _ := lrs.findTraceStatus(span.TraceID)

	// TODO: Integrate the trace decision cache
	switch state {
	case DecisionDrop:
		// The decision has been made and we can't affect it, so we just ignore the span
		// We'll only get here if the decision was made after the span was added
		// to the channel but before it got read out.
		return nil
	case DecisionKeep, AwaitingDecision:
		// The decision has been made and we can't affect it anymore, so we add the span to the trace
		// if it exists; if it doesn't exist in the traces map anymore, we'll create it again.
		// We do need to mark it late, though.
		// i.states[state][span.TraceID].KeepReason = "late" // TODO: decorate this properly
	case Collecting, WaitingToDecide, ReadyForDecision:
		// we're in a state where we can just add the span
	case Unknown:
		// we don't have a state for this trace, so we create it
		lrs.states[Collecting][span.TraceID] = NewCentralTraceStatus(span.TraceID, Collecting)
		lrs.traces[span.TraceID] = &CentralTrace{}
	}

	// Add the span to the trace; this works even if the trace doesn't exist yet
	lrs.traces[span.TraceID].Spans = append(lrs.traces[span.TraceID].Spans, span)

	if span.ParentID == "" {
		// this is a root span, so we need to move it to the right state
		lrs.traces[span.TraceID].Root = span
		switch state {
		case Collecting:
			lrs.changeTraceState(span.TraceID, Collecting, WaitingToDecide)
		default:
			// for all other states, we don't need to do anything
		}
	}
	return nil
}

// GetTrace fetches the current state of a trace (including all of its
// spans) from the central store. The trace contains a list of CentralSpans,
// and these spans will usually (but not always) only contain the key
// fields. The spans returned from this call should be used for making the
// trace decision; they should not be sent as telemetry unless AllFields is
// non-null, in which case these spans should be sent if the trace decision
// is Keep. If the trace has a root span, the Root property will be
// populated. Normally this call will be made after Refinery has been asked
// to make a trace decision.
func (lrs *LocalRemoteStore) GetTrace(traceID string) (*CentralTrace, error) {
	lrs.mutex.RLock()
	defer lrs.mutex.RUnlock()
	if trace, ok := lrs.traces[traceID]; ok {
		return trace, nil
	}
	return nil, fmt.Errorf("trace %s not found", traceID)
}

// GetStatusForTraces returns the current state for a list of trace IDs,
// including any reason information and trace counts if the trace decision
// has been made and it was to keep the trace. If a requested trace was not
// found, it will be returned as Status:Unknown. This should be considered
// to be a bug in the central store, as the trace should have been created
// when the first span was added. Any traces with a state of DecisionKeep or
// DecisionDrop should be considered to be final and appropriately disposed
// of; the central store will not change the decision state of these traces
// after this call (although kept spans will have counts updated when late
// spans arrive).
func (lrs *LocalRemoteStore) GetStatusForTraces(traceIDs []string) ([]*CentralTraceStatus, error) {
	lrs.mutex.RLock()
	defer lrs.mutex.RUnlock()
	var statuses = make([]*CentralTraceStatus, 0, len(traceIDs))
	for _, traceID := range traceIDs {
		if state, status := lrs.findTraceStatus(traceID); state != Unknown {
			statuses = append(statuses, status.Clone())
		} else {
			statuses = append(statuses, NewCentralTraceStatus(traceID, state))
		}
	}
	return statuses, nil
}

// GetTracesForState returns a list of trace IDs that match the provided status.
func (lrs *LocalRemoteStore) GetTracesForState(state CentralTraceState) ([]string, error) {
	lrs.mutex.RLock()
	defer lrs.mutex.RUnlock()
	if _, ok := lrs.states[state]; !ok {
		return nil, fmt.Errorf("invalid state %s", state)
	}
	traceids := make([]string, 0, len(lrs.states[state]))
	for _, traceStatus := range lrs.states[state] {
		traceids = append(traceids, traceStatus.TraceID)
	}
	return traceids, nil
}

// ChangeTraceStatus changes the status of a set of traces from one state to another
// atomically. This can be used for all trace states except transition to Keep.
// If a traceID is not found in the fromState, this is not considered to be an error.
// TODO: should we consider returning the list of traces that were not modified? Do we care?
// Alternative is to make this function only do one trace at a time.
func (lrs *LocalRemoteStore) ChangeTraceStatus(traceIDs []string, fromState, toState CentralTraceState) error {
	lrs.mutex.Lock()
	defer lrs.mutex.Unlock()
	for _, traceID := range traceIDs {
		if !lrs.changeTraceState(traceID, fromState, toState) {
			continue
		}
	}
	return nil
}

// KeepTraces changes the status of a set of traces from AwaitingDecision to Keep;
// it is used to record the keep decisions made by the trace decision engine.
// Statuses should include Reason and Rate; do not include State as it will be ignored.
// If a trace is not in the AwaitingDecision state, it will be ignored.
func (lrs *LocalRemoteStore) KeepTraces(statuses []*CentralTraceStatus) error {
	lrs.mutex.Lock()
	defer lrs.mutex.Unlock()
	for _, status := range statuses {
		if !lrs.changeTraceState(status.TraceID, AwaitingDecision, DecisionKeep) {
			continue
		}
		lrs.states[DecisionKeep][status.TraceID].KeepReason = status.KeepReason
		lrs.states[DecisionKeep][status.TraceID].Rate = status.Rate
	}
	return nil
}

// GetMetrics returns a map of metrics from the remote store, accumulated
// since the previous time this method was called.
func (lrs *LocalRemoteStore) GetMetrics() (map[string]interface{}, error) {
	return nil, nil
}

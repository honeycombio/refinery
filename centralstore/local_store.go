package centralstore

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/generics"
	"github.com/honeycombio/refinery/internal/otelutil"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
	"github.com/jonboulle/clockwork"
	"go.opentelemetry.io/otel/trace"
)

// LocalStore is a basic store that is local to the Refinery process. This is
// used when there is only one refinery in the system.
type LocalStore struct {
	Config        config.Config        `inject:""`
	DecisionCache cache.TraceSentCache `inject:""`
	Metrics       metrics.Metrics      `inject:"genericMetrics"`
	Tracer        trace.Tracer         `inject:"tracer"`
	Clock         clockwork.Clock      `inject:""`
	// states holds the current state of each trace in a map of the different states
	// indexed by trace ID.
	states map[CentralTraceState]statusMap
	traces map[string]*CentralTrace
	mutex  sync.RWMutex
}

// ensure that LocalStore implements RemoteStore
var _ BasicStorer = (*LocalStore)(nil)

func (lrs *LocalStore) Start() error {
	if lrs.DecisionCache == nil {
		return fmt.Errorf("LocalStore requires a DecisionCache")
	}
	if lrs.Clock == nil {
		return fmt.Errorf("LocalStore requires a Clock")
	}
	if lrs.Config == nil {
		return fmt.Errorf("LocalStore requires a Config")
	}

	lrs.states = make(map[CentralTraceState]statusMap)
	lrs.traces = make(map[string]*CentralTrace)

	// these states are the ones we need to maintain as separate maps
	mapStates := []CentralTraceState{
		// Unknown, // not a valid state for a trace
		Collecting,
		DecisionDelay,
		ReadyToDecide,
		AwaitingDecision,
		// DecisionKeep, // these are in the decision cache
		// DecisionDrop, // these are in the decision cache
	}
	for _, state := range mapStates {
		// initialize the map for each state
		lrs.states[state] = make(statusMap)
	}
	return nil
}

func (lrs *LocalStore) Stop() error {
	return nil
}

// findTraceStatus returns the state and status of a trace, or Unknown if the trace
// wasn't found in any state. If the trace is found, the status will be non-nil.
// Only call this if you're holding a Lock.
func (lrs *LocalStore) findTraceStatus(traceID string) (CentralTraceState, *CentralTraceStatus) {
	if tracerec, reason, found := lrs.DecisionCache.Test(traceID); found {
		// it was in the decision cache, so we can return the right thing
		if tracerec.Kept() {
			status := NewCentralTraceStatus(traceID, DecisionKeep, lrs.Clock.Now())
			status.KeepReason = reason
			return DecisionKeep, status
		} else {
			return DecisionDrop, NewCentralTraceStatus(traceID, DecisionDrop, lrs.Clock.Now())
		}
	}
	// wasn't in the cache, look in all the other states
	for state, statuses := range lrs.states {
		if status, ok := statuses[traceID]; ok {
			return state, status
		}
	}
	return Unknown, nil
}

// changes the status of a trace; if fromState is Unknown, the trace will be searched for
// in all states. If the trace wasn't found, false will be returned.
// Only call this if you're holding a Lock.
func (lrs *LocalStore) changeTraceState(traceID string, fromState, toState CentralTraceState) bool {
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
	status.Timestamp = time.Now()
	delete(lrs.states[fromState], traceID)
	return true
}

// WriteSpan writes a span to the store. It must always contain TraceID. If this
// is a span containing any non-empty key fields, it must also contain SpanID
// (and ParentID if it is not a root span). For span events and span links, it
// may contain only the TraceID and the SpanType field; these are counted but
// not stored. Root spans should always be sent and must contain at least
// SpanID, and have the IsRoot flag set. AllFields is optional and is used
// during shutdown. WriteSpan is expecting to be called from an asynchronous
// process and will only return an error if the span could not be written.
func (lrs *LocalStore) WriteSpans(ctx context.Context, spans []*CentralSpan) error {
	// TODO: fix the instrumentation here
	_, spanWrite := otelutil.StartSpan(ctx, lrs.Tracer, "LocalStore.WriteSpan")
	defer spanWrite.End()
	lrs.mutex.Lock()
	defer lrs.mutex.Unlock()
spanLoop:
	for _, span := range spans {
		// first let's check if we've already processed and dropped this trace; if so, we're done and
		// can just ignore the span.
		if lrs.DecisionCache.Dropped(span.TraceID) {
			continue
		}

		// we have to find the state and decide what to do based on that
		state, _ := lrs.findTraceStatus(span.TraceID)

		// TODO: Integrate the trace decision cache
		switch state {
		case DecisionDrop:
			// The decision has been made and we can't affect it, so we just ignore the span
			// We'll only get here if the decision was made after the span was added
			// to the channel but before it got read out.
			continue spanLoop
		case DecisionKeep:
			// The decision has been made and we can't affect it, but we do have to count this span
			// The centralspan needs to be converted to a span and then counted or we have to
			// give the cache a way to count span events and links
			// lrs.decisionCache.Check(span)
			continue spanLoop
		case AwaitingDecision:
			// The decision hasn't been received yet but it has been sent to a refinery for a decision
			// We can't affect the decision but we can append it and mark it late.
			// i.states[state][span.TraceID].KeepReason = "late" // TODO: decorate this properly
		case Collecting, DecisionDelay, ReadyToDecide:
			// we're in a state where we can just add the span
		case Unknown:
			// we don't have a state for this trace, so we create it
			state = Collecting
			lrs.states[Collecting][span.TraceID] = NewCentralTraceStatus(span.TraceID, Collecting, lrs.Clock.Now())
			lrs.traces[span.TraceID] = &CentralTrace{TraceID: span.TraceID}
		}

		// Add the span to the trace; this works even if the trace has no spans yet
		fmt.Printf("Adding span %s to trace %s\n", span.SpanID, span.TraceID)
		lrs.traces[span.TraceID].Spans = append(lrs.traces[span.TraceID].Spans, span)
		lrs.states[state][span.TraceID].Count++
		if span.Type == types.SpanTypeLink {
			lrs.states[state][span.TraceID].LinkCount++
		} else if span.Type == types.SpanTypeEvent {
			lrs.states[state][span.TraceID].EventCount++
		}

		if span.IsRoot {
			// this is a root span, so we need to move it to the right state
			lrs.traces[span.TraceID].Root = span
			switch state {
			case Collecting:
				lrs.changeTraceState(span.TraceID, Collecting, DecisionDelay)
			default:
				// for all other states, we don't need to do anything
			}
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
func (lrs *LocalStore) GetTrace(ctx context.Context, traceID string) (*CentralTrace, error) {
	_, span := otelutil.StartSpan(ctx, lrs.Tracer, "LocalStore.GetTrace")
	defer span.End()
	lrs.mutex.RLock()
	defer lrs.mutex.RUnlock()
	if trace, ok := lrs.traces[traceID]; ok {
		otelutil.AddSpanField(span, "found", true)
		return trace, nil
	}
	otelutil.AddSpanField(span, "found", false)
	return nil, fmt.Errorf("trace %s not found", traceID)
}

// GetStatusForTraces returns the current state for a list of traces if they
// match any of the states passed in, including any reason information if
// the trace decision has been made and it was to keep the trace. If a trace
// is not found in any of the listed states, it will be not be returned. Any
// traces with a state of DecisionKeep or DecisionDrop should be considered
// to be final and appropriately disposed of; the central store will not
// change the state of these traces after this call.
func (lrs *LocalStore) GetStatusForTraces(ctx context.Context, traceIDs []string, statesToCheck ...CentralTraceState) ([]*CentralTraceStatus, error) {
	// create a function to check if a state is one of the ones we're looking for; we can
	// specialize this for the common case where we're only looking for one state.
	// this has been benchmarked and is faster than using a map for the common case.
	var shouldUse func(st CentralTraceState) bool
	if len(statesToCheck) == 1 {
		shouldUse = func(st CentralTraceState) bool { return st == statesToCheck[0] }
	} else {
		// put the states in a set for faster lookup
		states := generics.NewSet[CentralTraceState]()
		states.Add(statesToCheck...)
		shouldUse = func(st CentralTraceState) bool { return states.Contains(st) }
	}

	_, span := otelutil.StartSpan(ctx, lrs.Tracer, "LocalStore.GetStatusForTraces")
	defer span.End()
	lrs.mutex.RLock()
	defer lrs.mutex.RUnlock()
	var statuses = make([]*CentralTraceStatus, 0, len(traceIDs))
	for _, traceID := range traceIDs {
		if state, status := lrs.findTraceStatus(traceID); shouldUse(state) {
			statuses = append(statuses, status.Clone())
		}
	}
	return statuses, nil
}

// GetTracesForState returns a list of trace IDs that match the provided status.
func (lrs *LocalStore) GetTracesForState(ctx context.Context, state CentralTraceState) ([]string, error) {
	_, span := otelutil.StartSpan(ctx, lrs.Tracer, "LocalStore.GetTracesForState")
	defer span.End()
	lrs.mutex.RLock()
	defer lrs.mutex.RUnlock()
	switch state {
	case DecisionKeep, DecisionDrop:
		// these are in the decision cache and can't be fetched from the states map
		return nil, nil
	}

	if _, ok := lrs.states[state]; !ok {
		return nil, fmt.Errorf("invalid state %s", state)
	}
	traceids := make([]string, 0, len(lrs.states[state]))
	for _, traceStatus := range lrs.states[state] {
		traceids = append(traceids, traceStatus.TraceID)
	}
	return traceids, nil
}

// GetTracesNeedingDecision returns a list of up to n trace IDs that are in the
// ReadyForDecision state. These IDs are moved to the AwaitingDecision state
// atomically, so that no other refinery will be assigned the same trace.
func (lrs *LocalStore) GetTracesNeedingDecision(ctx context.Context, n int) ([]string, error) {
	_, span := otelutil.StartSpan(ctx, lrs.Tracer, "LocalStore.GetTracesNeedingDecision")
	defer span.End()
	lrs.mutex.Lock()
	defer lrs.mutex.Unlock()
	// get the list of traces in the ReadyForDecision state
	traceids := make([]string, 0, len(lrs.states[ReadyToDecide]))
	for _, traceStatus := range lrs.states[ReadyToDecide] {
		traceids = append(traceids, traceStatus.TraceID)
		lrs.changeTraceState(traceStatus.TraceID, ReadyToDecide, AwaitingDecision)
		n--
		if n == 0 {
			break
		}
	}
	return traceids, nil
}

// ChangeTraceStatus changes the status of a set of traces from one state to another
// atomically. This can be used for all trace states except transition to Keep.
// If a traceID is not found in the fromState, this is not considered to be an error.
func (lrs *LocalStore) ChangeTraceStatus(ctx context.Context, traceIDs []string, fromState, toState CentralTraceState) error {
	_, span := otelutil.StartSpan(ctx, lrs.Tracer, "LocalStore.ChangeTraceStatus")
	defer span.End()
	lrs.mutex.Lock()
	defer lrs.mutex.Unlock()
	for _, traceID := range traceIDs {
		if toState == DecisionDrop {
			// if we're dropping, record it in the decision cache
			if trace, ok := lrs.states[fromState][traceID]; ok {
				lrs.DecisionCache.Record(trace, false, "")
				// and remove it from the states map
				delete(lrs.states[fromState], traceID)
				delete(lrs.traces, traceID)
			}
		} else {
			lrs.changeTraceState(traceID, fromState, toState)
		}
	}
	return nil
}

// KeepTraces changes the status of a set of traces from AwaitingDecision to Keep;
// it is used to record the keep decisions made by the trace decision engine.
// Statuses should include Reason and Rate; do not include State as it will be ignored.
// If a trace is not in the AwaitingDecision state, it will be ignored.
func (lrs *LocalStore) KeepTraces(ctx context.Context, statuses []*CentralTraceStatus) error {
	_, span := otelutil.StartSpan(ctx, lrs.Tracer, "LocalStore.KeepTraces")
	defer span.End()
	lrs.mutex.Lock()
	defer lrs.mutex.Unlock()
	for _, status := range statuses {
		if _, ok := lrs.states[AwaitingDecision][status.TraceID]; ok {
			lrs.DecisionCache.Record(status, true, status.KeepReason)
			delete(lrs.states[AwaitingDecision], status.TraceID)
			delete(lrs.traces, status.TraceID)
		}
	}
	return nil
}

func (lrs *LocalStore) RecordTraceDecision(ctx context.Context, trace *CentralTraceStatus, keep bool, reason string) error {
	_, span := otelutil.StartSpan(ctx, lrs.Tracer, "LocalStore.RecordTraceDecision")
	defer span.End()
	lrs.mutex.Lock()
	defer lrs.mutex.Unlock()
	if keep {
		lrs.DecisionCache.Record(trace, keep, reason)
	} else {
		lrs.DecisionCache.Dropped(trace.TraceID)
	}

	return nil
}

// RecordMetrics returns a map of metrics from the remote store, accumulated
// since the previous time this method was called.
func (lrs *LocalStore) RecordMetrics(ctx context.Context) error {
	_, span := otelutil.StartSpan(ctx, lrs.Tracer, "LocalStore.RecordMetrics")
	defer span.End()
	lrs.mutex.RLock()
	defer lrs.mutex.RUnlock()
	m, err := lrs.DecisionCache.GetMetrics()
	if err != nil {
		return err
	}
	for k, v := range m {
		lrs.Metrics.Count("localstore_"+k, v)
	}

	// add the state counts and trace count
	m2 := map[string]any{
		"count_" + string(Collecting):       len(lrs.states[Collecting]),
		"count_" + string(DecisionDelay):    len(lrs.states[DecisionDelay]),
		"count_" + string(ReadyToDecide):    len(lrs.states[ReadyToDecide]),
		"count_" + string(AwaitingDecision): len(lrs.states[AwaitingDecision]),
		"count_" + string(DecisionKeep):     len(lrs.states[DecisionKeep]),
		"count_" + string(DecisionDrop):     len(lrs.states[DecisionDrop]),
		"ntraces":                           len(lrs.traces),
	}
	for k, v := range m2 {
		lrs.Metrics.Count("localstore_"+k, v)
	}
	return nil
}

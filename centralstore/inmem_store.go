package centralstore

import (
	"fmt"
	"sync"
	"time"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/metrics"
)

type statusMap map[string]*CentralTraceStatus

// func (sm statusMap) addStatus(traceID string, status *CentralTraceStatus) {
// 	sm[traceID] = status
// }

// func (sm statusMap) getStatus(traceID string) *CentralTraceStatus {
// 	return sm[traceID]
// }

// func (sm statusMap) deleteStatus(traceID string) {
// 	delete(sm, traceID)
// }

// This is an implementation of CentralStorer that stores spans in memory locally.
type InMemStore struct {
	// states holds the current state of each trace in a map of the different states
	// indexed by trace ID.
	states map[CentralTraceState]statusMap
	// traces holds the current data for each trace
	traces        map[string]*CentralTrace
	keyfields     []string
	spanChan      chan *CentralSpan
	decisionCache cache.TraceSentCache
	mut           sync.RWMutex
}

// ensure that we implement CentralStorer
var _ CentralStorer = (*InMemStore)(nil)

// NewInMemStore creates a new InMemStore.
func NewInMemStore() *InMemStore {
	// TODO: make these configurable
	cfg := config.SampleCacheConfig{
		KeptSize:          10000,
		DroppedSize:       1_000_000,
		SizeCheckInterval: config.Duration(10 * time.Second),
	}
	decisionCache, err := cache.NewCuckooSentCache(cfg, &metrics.NullMetrics{})
	if err != nil {
		// TODO: handle this better
		panic(err)
	}

	i := &InMemStore{
		states:        make(map[CentralTraceState]statusMap),
		traces:        make(map[string]*CentralTrace),
		spanChan:      make(chan *CentralSpan, 1000),
		decisionCache: decisionCache,
	}

	var cts CentralTraceState
	for _, state := range cts.ValidStates() {
		// initialize the map for each state
		i.states[state] = make(statusMap)
	}

	// start the span processor
	go i.ProcessSpans()

	return i
}

func (i *InMemStore) Stop() {
	// close the span channel to stop the span processor,
	// but first set spanChan to nil so that we won't accept any more spans
	schan := i.spanChan
	i.spanChan = nil
	close(schan)
}

// findTraceStatus returns the state and status of a trace, or Unknown if the trace
// wasn't found in any state. If the trace is found, the status will be non-nil.
// Only call this if you're holding at least an RLock on i.mut.
func (i *InMemStore) findTraceStatus(traceID string) (CentralTraceState, *CentralTraceStatus) {
	for state, statuses := range i.states {
		if status, ok := statuses[traceID]; ok {
			return state, status
		}
	}
	return Unknown, nil
}

func (i *InMemStore) addTraceStatus(status *CentralTraceStatus) {
	i.states[status.State][status.TraceID] = status
}

// changes the status of a trace; if fromState is Unknown, the trace will be searched for
// in all states. If the trace wasn't found, false will be returned.
func (i *InMemStore) changeTraceState(traceID string, fromState, toState CentralTraceState) bool {
	var status *CentralTraceStatus
	var ok bool
	if fromState == Unknown {
		fromState, status = i.findTraceStatus(traceID)
		if fromState == Unknown {
			return false
		}
	} else {
		if status, ok = i.states[fromState][traceID]; !ok {
			return false
		}
	}

	status.State = toState
	i.states[toState][traceID] = status
	delete(i.states[fromState], traceID)
	return true
}

// Register should be called once at Refinery startup to register itself
// with the central store. This is used to ensure that the central store
// knows about all the refineries that are running, and can make decisions
// about which refinery is responsible for which trace.
// For an in-memory store, this is a no-op.
func (i *InMemStore) Register() error {
	return nil
}

// Unregister should be called once at Refinery shutdown to unregister itself
// with the central store. Once it has been unregistered, the central store
// will no longer ask it to make decisions on any new traces. (Calls to
// GetTracesNeedingDecision will return an empty list.)
// For an in-memory store, this is a no-op.
func (i *InMemStore) Unregister() error {
	return nil
}

// SetKeyFields sets the fields that will be recorded in the central store;
// if they are changed live, inflight trace decisions may be affected.
// Certain fields are always recorded, such as the trace ID, span ID, and
// parent ID; listing them in keyFields is not necessary (but won't hurt).
func (i *InMemStore) SetKeyFields(keyFields []string) error {
	// someday maybe we compare new keyFields to old keyFields and do something
	i.keyfields = keyFields
	return nil
}

// WriteSpan writes one or more CentralSpans to the CentralStore.
// It is valid to write a span that has already been written; this can happen
// on shutdown, when a refinery forwards all its remaining spans to the central store.
// The latest value for a span should be retained, because during shutdown, the
// span will contain more fields.
func (i *InMemStore) WriteSpan(span *CentralSpan) error {
	// first let's check if we've already processed and dropped this trace; if so, we're done and
	// can just ignore the span.
	if i.decisionCache.Dropped(span.TraceID) {
		return nil
	}

	// if the span channel doesn't exist, we're shutting down and we can't accept any more spans
	if i.spanChan == nil {
		return fmt.Errorf("span channel closed")
	}

	// otherwise, put the span in the channel but don't block
	select {
	case i.spanChan <- span:
	default:
		return fmt.Errorf("span queue full")
	}
	return nil
}

// ProcessSpans processes spans from the span channel. This is run in a
// goroutine and runs indefinitely until cancelled by calling Stop().
func (i *InMemStore) ProcessSpans() {
	for span := range i.spanChan {
		// we have to find the state and decide what to do based on that
		state, _ := i.findTraceStatus(span.TraceID)

		// TODO: Integrate the trace decision cache
		switch state {
		case DecisionDrop:
			// The decision has been made and we can't affect it, so we just ignore the span
			// We'll only get here if the decision was made after the span was added
			// to the channel but before it got read out.
			continue
		case DecisionKeep, AwaitingDecision:
			// The decision has been made and we can't affect it anymore, so we add the span to the trace
			// if it exists; if it doesn't exist in the traces map anymore, we'll create it again.
			// We do need to mark it late, though.
			// i.states[state][span.TraceID].KeepReason = "late" // TODO: decorate this properly
		case Collecting, WaitingToDecide, ReadyForDecision:
			// we're in a state where we can just add the span
		case Unknown:
			// we don't have a state for this trace, so we create it
			i.states[Collecting][span.TraceID] = &CentralTraceStatus{TraceID: span.TraceID, State: Collecting}
		}

		// Add the span to the trace; this works even if the trace doesn't exist yet
		i.traces[span.TraceID].Spans = append(i.traces[span.TraceID].Spans, span)

		if span.ParentID == "" {
			// this is a root span, so we need to move it to the right state
			i.traces[span.TraceID].Root = span
			switch state {
			case Collecting:
				i.changeTraceState(span.TraceID, Collecting, WaitingToDecide)
			default:
				// for all other states, we don't need to do anything
			}
		}
	}
}

// GetTrace fetches the current state of a trace (including all its spans)
// from the central store. The trace contains a list of CentralSpans, but
// note that these spans will usually only contain the key fields. The spans
// returned from this call should be used for making the trace decision;
// they should not be sent as telemetry unless AllFields is non-null. If the
// trace has a root span, it will be the first span in the list.
func (i *InMemStore) GetTrace(traceID string) (*CentralTrace, error) {
	if trace, ok := i.traces[traceID]; ok {
		return trace, nil
	}
	return nil, fmt.Errorf("trace %s not found", traceID)
}

// GetStatusForTraces returns the current state for a list of traces,
// including any reason information if the trace decision has been made and
// it was to keep the trace. If a trace is not found, it will be returned as
// Status:Unknown. This should be considered to be a bug in the central
// store, as the trace should have been created when the first span was
// added. Any traces with a state of DecisionKeep or DecisionDrop should be
// considered to be final and appropriately disposed of; the central store
// will not change the state of these traces after this call.
func (i *InMemStore) GetStatusForTraces(traceIDs []string) ([]*CentralTraceStatus, error) {
	var statuses []*CentralTraceStatus
	for _, traceID := range traceIDs {
		if state, status := i.findTraceStatus(traceID); state != Unknown {
			statuses = append(statuses, status)
		} else {
			statuses = append(statuses, &CentralTraceStatus{TraceID: traceID, State: state})
		}
	}
	return statuses, nil
}

// GetTracesForState returns a list of trace IDs that match the provided status.
func (i *InMemStore) GetTracesForState(state CentralTraceState) ([]string, error) {
	if _, ok := i.states[state]; !ok {
		return nil, fmt.Errorf("invalid state %s", state)
	}
	traceids := make([]string, 0, len(i.states[state]))
	for _, traceStatus := range i.states[state] {
		traceids = append(traceids, traceStatus.TraceID)
	}
	return traceids, nil
}

// SetTraceStatuses sets the status of a set of traces in the central store.
// This is used to record the decision made by the trace decision engine. If
// the state is DecisionKeep, the reason should be provided; if the state is
// DecisionDrop, the reason should be empty as it will be ignored. Note that
// the CentralStorer is permitted to manipulate the state of the trace after
// this call, so the caller should not assume that the state persists as
// set.
func (i *InMemStore) SetTraceStatuses(statuses []*CentralTraceStatus) error {
	for _, status := range statuses {
		i.changeTraceState(status.TraceID, Unknown, status.State)
	}
	return nil
}

// GetMetrics returns a map of metrics from the central store, accumulated
// since the previous time this method was called.
func (i *InMemStore) GetMetrics() (map[string]interface{}, error) {
	return nil, nil
}

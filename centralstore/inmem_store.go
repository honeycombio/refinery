package centralstore

import (
	"errors"
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
	remoteStore   RemoteStore
	keyfields     []string
	spanChan      chan *CentralSpan
	decisionCache cache.TraceSentCache
	done          chan struct{}
	mutex         sync.RWMutex
}

// ensure that we implement CentralStorer
var _ CentralStorer = (*InMemStore)(nil)

// this probably gets moved into config eventually
type InMemStoreOptions struct {
	KeptSize          uint
	DroppedSize       uint
	SpanChannelSize   int
	SizeCheckInterval config.Duration
	StateTicker       config.Duration
	SendDelay         config.Duration
	TraceTimeout      config.Duration
	DecisionTimeout   config.Duration
}

// NewInMemStore creates a new InMemStore.
func NewInMemStore(options InMemStoreOptions) *InMemStore {
	cfg := config.SampleCacheConfig{
		KeptSize:          options.KeptSize,
		DroppedSize:       options.DroppedSize,
		SizeCheckInterval: options.SizeCheckInterval,
	}
	decisionCache, err := cache.NewCuckooSentCache(cfg, &metrics.NullMetrics{})
	if err != nil {
		// TODO: handle this better
		panic(err)
	}

	i := &InMemStore{
		remoteStore:   NewLocalRemoteStore(),
		spanChan:      make(chan *CentralSpan, options.SpanChannelSize),
		decisionCache: decisionCache,
		done:          make(chan struct{}),
	}
	// start the span processor
	go i.processSpans()

	// start the state manager
	go i.manageStates(options)

	return i
}

func (i *InMemStore) Stop() {
	// close the span channel to stop the span processor,
	// but first set spanChan to nil so that we won't accept any more spans
	i.mutex.Lock()
	defer i.mutex.Unlock()
	schan := i.spanChan
	i.spanChan = nil
	close(schan)
	// stop the state manager
	close(i.done)
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

// processSpans processes spans from the span channel. This is run in a
// goroutine and runs indefinitely until cancelled by calling Stop().
func (i *InMemStore) processSpans() {
	for span := range i.spanChan {
		i.remoteStore.WriteSpan(span)
	}
}

// helper function for manageStates
func (i *InMemStore) manageTimeouts(timeout time.Duration, fromState, toState CentralTraceState) error {
	st, err := i.remoteStore.GetTracesForState(fromState)
	if err != nil {
		return err
	}
	statuses, err := i.remoteStore.GetStatusForTraces(st)
	if err != nil {
		return err
	}
	traceIDsToChange := make([]string, 0)
	for _, status := range statuses {
		if time.Since(status.timestamp) > timeout {
			traceIDsToChange = append(traceIDsToChange, status.TraceID)
		}
	}
	if len(traceIDsToChange) > 0 {
		err = i.remoteStore.ChangeTraceStatus(traceIDsToChange, fromState, toState)
	}
	return err
}

// manageStates is a goroutine that manages the time-based transitions between states.
// It runs once each tick of the stateTicker and looks for several different transitions.
// We unlock the mutex between the different state transitions so that we don't block for too long
func (i *InMemStore) manageStates(options InMemStoreOptions) {
	ticker := time.NewTicker(time.Duration(options.StateTicker))
	for {
		select {
		case <-i.done:
			ticker.Stop()
			return
		case <-ticker.C:
			// traces that are past SendDelay should be moved to ready for decision
			// the only errors can be syntax errors, so won't happen at runtime
			i.manageTimeouts(time.Duration(options.SendDelay), WaitingToDecide, ReadyForDecision)

			// trace that are past TraceTimeout should be moved to waiting to decide
			i.manageTimeouts(time.Duration(options.TraceTimeout), Collecting, WaitingToDecide)

			// see if AwaitDecision traces have been waiting too long
			i.manageTimeouts(time.Duration(options.DecisionTimeout), AwaitingDecision, ReadyForDecision)
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
	return i.remoteStore.GetTrace(traceID)
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
	return i.remoteStore.GetStatusForTraces(traceIDs)
}

// GetTracesForState returns a list of trace IDs that match the provided status.
func (i *InMemStore) GetTracesForState(state CentralTraceState) ([]string, error) {
	return i.remoteStore.GetTracesForState(state)
}

// SetTraceStatuses sets the status of a set of traces in the central store.
// This is used to record the decision made by the trace decision engine. If
// the state is DecisionKeep, the reason should be provided; if the state is
// DecisionDrop, the reason should be empty as it will be ignored. Note that
// the CentralStorer is permitted to manipulate the state of the trace after
// this call, so the caller should not assume that the state persists as
// set.
func (i *InMemStore) SetTraceStatuses(statuses []*CentralTraceStatus) error {
	keeps := make([]*CentralTraceStatus, 0)
	drops := make([]string, 0)
	for _, status := range statuses {
		switch status.State {
		case DecisionKeep:
			keeps = append(keeps, status)
		case DecisionDrop:
			drops = append(drops, status.TraceID)
		default:
			// ignore all other states -- but do we want to log something? This is a bug.
		}
	}

	var err error
	if len(keeps) > 0 {
		err = i.remoteStore.KeepTraces(keeps)
	}
	if len(drops) > 0 {
		err2 := i.remoteStore.ChangeTraceStatus(drops, WaitingToDecide, DecisionDrop)
		if err2 != nil {
			if err != nil {
				err = errors.Join(err, err2)
			} else {
				err = err2
			}
		}
	}
	return err
}

// GetMetrics returns a map of metrics from the central store, accumulated
// since the previous time this method was called.
func (i *InMemStore) GetMetrics() (map[string]interface{}, error) {
	return nil, nil
}

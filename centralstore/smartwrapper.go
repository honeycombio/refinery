package centralstore

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/metrics"
	"github.com/jonboulle/clockwork"
	"go.opentelemetry.io/otel/trace"
)

type statusMap map[string]*CentralTraceStatus

// This is an implementation of SmartStorer that stores spans in memory locally.
type SmartWrapper struct {
	Config         config.Config   `inject:""`
	Metrics        metrics.Metrics `inject:"genericMetrics"`
	BasicStore     BasicStorer     `inject:""`
	Tracer         trace.Tracer    `inject:"tracer"`
	Clock          clockwork.Clock `inject:""`
	keyfields      []string
	spanChan       chan *CentralSpan
	stopped        chan struct{}
	done           chan struct{}
	doneProcessing chan struct{}
}

// ensure that we implement SmartStorer
var _ SmartStorer = (*SmartWrapper)(nil)

// Start validates and starts the SmartWrapper.
func (w *SmartWrapper) Start() error {
	// check that the injection is doing its job
	if w.Config == nil {
		return fmt.Errorf("config is nil")
	}
	if w.Tracer == nil {
		return fmt.Errorf("tracer is nil")
	}
	if w.BasicStore == nil {
		return fmt.Errorf("basicStore is nil")
	}
	opts := w.Config.GetCentralStoreOptions()

	w.stopped = make(chan struct{})
	w.spanChan = make(chan *CentralSpan, opts.SpanChannelSize)
	w.done = make(chan struct{})
	w.doneProcessing = make(chan struct{})

	// start the span processor
	go w.processSpans(context.Background())

	// start the state manager
	go w.manageStates(context.Background(), opts)

	return nil
}

func (w *SmartWrapper) Stop() error {
	// close the span channel to stop the span processor,
	close(w.spanChan)
	// stop the state manager
	close(w.done)
	// wait for the span processor to finish
	<-w.doneProcessing
	return nil
}

// Register should be called once at Refinery startup to register itself
// with the central store. This is used to ensure that the central store
// knows about all the refineries that are running, and can make decisions
// about which refinery is responsible for which trace.
// For an in-memory store, this is a no-op.
func (w *SmartWrapper) Register(ctx context.Context) error {
	return nil
}

// Unregister should be called once at Refinery shutdown to unregister itself
// with the central store. Once it has been unregistered, the central store
// will no longer ask it to make decisions on any new traces. (Calls to
// GetTracesNeedingDecision will return an empty list.)
// For an in-memory store, this is a no-op.
func (w *SmartWrapper) Unregister(ctx context.Context) error {
	return nil
}

// SetKeyFields sets the fields that will be recorded in the central store;
// if they are changed live, inflight trace decisions may be affected.
// Certain fields are always recorded, such as the trace ID, span ID, and
// parent ID; listing them in keyFields is not necessary (but won't hurt).
func (w *SmartWrapper) SetKeyFields(ctx context.Context, keyFields []string) error {
	// someday maybe we compare new keyFields to old keyFields and do something
	w.keyfields = keyFields
	return nil
}

// WriteSpan writes one or more CentralSpans to the CentralStore.
// It is valid to write a span that has already been written; this can happen
// on shutdown, when a refinery forwards all its remaining spans to the central store.
// The latest value for a span should be retained, because during shutdown, the
// span will contain more fields.
func (w *SmartWrapper) WriteSpan(ctx context.Context, span *CentralSpan) error {
	// if the span channel doesn't exist, we're shutting down and we can't accept any more spans
	if w.spanChan == nil {
		return fmt.Errorf("span channel closed")
	}

	// otherwise, put the span in the channel but don't block
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return fmt.Errorf("span channel closed")
	case w.spanChan <- span:
		// fmt.Println("span written")
	default:
		return fmt.Errorf("span queue full")
	}
	return nil
}

// processSpans processes spans from the span channel. This is run in a
// goroutine and runs indefinitely until cancelled by calling Stop().
func (w *SmartWrapper) processSpans(ctx context.Context) {
	for span := range w.spanChan {
		err := w.BasicStore.WriteSpan(ctx, span)
		if err != nil {
			fmt.Println(err)
		}
	}

	w.doneProcessing <- struct{}{}
}

// helper function for manageStates
func (w *SmartWrapper) manageTimeouts(ctx context.Context, timeout time.Duration, fromState, toState CentralTraceState) error {
	if w.BasicStore == nil {
		fmt.Println("basicStore is nil")
	}
	st, err := w.BasicStore.GetTracesForState(ctx, fromState)
	if err != nil {
		return err
	}
	if len(st) == 0 || st == nil {
		return nil
	}
	// TODO: This is inefficent, this call searches all statuses but we know that they can only be in fromState
	statuses, err := w.BasicStore.GetStatusForTraces(ctx, st)
	if err != nil {
		return err
	}
	traceIDsToChange := make([]string, 0)
	for _, status := range statuses {
		if w.Clock.Since(status.Timestamp) > timeout {
			traceIDsToChange = append(traceIDsToChange, status.TraceID)
		}
	}
	if len(traceIDsToChange) > 0 {
		fmt.Println("changing", traceIDsToChange, "traces from", fromState, "to", toState)
		err = w.BasicStore.ChangeTraceStatus(ctx, traceIDsToChange, fromState, toState)
	}
	return err
}

// manageStates is a goroutine that manages the time-based transitions between
// states. It runs once each tick of the stateTicker (plus up to 10% so we don't
// all run at the same rate) and looks for several different transitions.
func (w *SmartWrapper) manageStates(ctx context.Context, options config.SmartWrapperOptions) {
	ticker := w.Clock.NewTicker(time.Duration(options.StateTicker + config.Duration(rand.Int63n(int64(options.StateTicker)/10))))
	for {
		select {
		case <-w.done:
			ticker.Stop()
			return
		case <-ticker.Chan():
			// the order of these is important!

			// see if AwaitDecision traces have been waiting too long
			w.manageTimeouts(ctx, time.Duration(options.DecisionTimeout), AwaitingDecision, ReadyToDecide)

			// traces that are past SendDelay should be moved to ready for decision
			// the only errors can be syntax errors, so won't happen at runtime
			w.manageTimeouts(ctx, time.Duration(options.SendDelay), DecisionDelay, ReadyToDecide)

			// trace that are past TraceTimeout should be moved to waiting to decide
			w.manageTimeouts(ctx, time.Duration(options.TraceTimeout), Collecting, DecisionDelay)
		}
	}
}

// GetTrace fetches the current state of a trace (including all its spans) from
// the central store. The trace contains a list of CentralSpans, but note that
// these spans will usually only contain the key fields. The spans returned from
// this call should be used for making the trace decision; they should not be
// sent as telemetry unless AllFields is non-null. If the trace has a root span,
// it will be the first span in the list. GetTrace is intended to be used to
// make a trace decision, so it has the side effect of moving a trace from
// ReadyForDecision to AwaitingDecision. If the trace is not in the
// ReadyForDecision state, its state will not be changed.
func (w *SmartWrapper) GetTrace(ctx context.Context, traceID string) (*CentralTrace, error) {
	return w.BasicStore.GetTrace(ctx, traceID)
}

// GetStatusForTraces returns the current state for a list of traces,
// including any reason information if the trace decision has been made and
// it was to keep the trace. If a trace is not found, it will be returned as
// Status:Unknown. This should be considered to be a bug in the central
// store, as the trace should have been created when the first span was
// added. Any traces with a state of DecisionKeep or DecisionDrop should be
// considered to be final and appropriately disposed of; the central store
// will not change the state of these traces after this call.
func (w *SmartWrapper) GetStatusForTraces(ctx context.Context, traceIDs []string) ([]*CentralTraceStatus, error) {
	return w.BasicStore.GetStatusForTraces(ctx, traceIDs)
}

// GetTracesForState returns a list of trace IDs that match the provided status.
func (w *SmartWrapper) GetTracesForState(ctx context.Context, state CentralTraceState) ([]string, error) {
	return w.BasicStore.GetTracesForState(ctx, state)
}

// GetTracesNeedingDecision returns a list of up to n trace IDs that are in the
// ReadyForDecision state. These IDs are moved to the AwaitingDecision state
// atomically, so that no other refinery will be assigned the same trace.
func (w *SmartWrapper) GetTracesNeedingDecision(ctx context.Context, n int) ([]string, error) {
	return w.BasicStore.GetTracesNeedingDecision(ctx, n)
}

// SetTraceStatuses sets the status of a set of traces in the central store.
// This is used to record the decision made by the trace decision engine. If
// the state is DecisionKeep, the reason should be provided; if the state is
// DecisionDrop, the reason should be empty as it will be ignored. Note that
// the SmartWrapper is permitted to manipulate the state of the trace after
// this call, so the caller should not assume that the state persists as
// set.
func (w *SmartWrapper) SetTraceStatuses(ctx context.Context, statuses []*CentralTraceStatus) error {
	keeps := make([]*CentralTraceStatus, 0)
	drops := make([]string, 0)
	for _, status := range statuses {
		switch status.State {
		case DecisionKeep:
			keeps = append(keeps, status)
		case DecisionDrop:
			drops = append(drops, status.TraceID)
		default:
			// ignore all other states
		}
	}

	var err error
	if len(keeps) > 0 {
		err = w.BasicStore.KeepTraces(ctx, keeps)
	}
	if len(drops) > 0 {
		err2 := w.BasicStore.ChangeTraceStatus(ctx, drops, AwaitingDecision, DecisionDrop)
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

// RecordMetrics returns a map of metrics from the central store, accumulated
// since the previous time this method was called.
func (w *SmartWrapper) RecordMetrics(ctx context.Context) error {
	return w.BasicStore.RecordMetrics(ctx)
}

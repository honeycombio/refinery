package centralstore

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/otelutil"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/jonboulle/clockwork"
	"go.opentelemetry.io/otel/trace"
)

type statusMap map[string]*CentralTraceStatus

// This is an implementation of SmartStorer that stores spans in memory locally.
type SmartWrapper struct {
	Config         config.Config   `inject:""`
	Metrics        metrics.Metrics `inject:"genericMetrics"`
	Logger         logger.Logger   `inject:""`
	BasicStore     BasicStorer     `inject:""`
	Tracer         trace.Tracer    `inject:"tracer"`
	Clock          clockwork.Clock `inject:""`
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

	w.Metrics.Register("smartstore_span_queue", "histogram")
	w.Metrics.Register("smartstore_span_queue_length", "gauge")
	w.Metrics.Register("smartstore_span_queue_in", "counter")
	w.Metrics.Register("smartstore_span_queue_out", "counter")
	w.Metrics.Register("smartstore_write_span_batch_size", "gauge")

	w.Metrics.Store("SPAN_CHANNEL_CAP", float64(opts.SpanChannelSize))

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

	ctx, spanWrite := otelutil.StartSpanMulti(ctx, w.Tracer, "SmartWrapper.WriteSpan", map[string]interface{}{
		"trace_id":         span.TraceID,
		"span_chan_length": len(w.spanChan),
		"span_chan_cap":    cap(w.spanChan),
	})
	defer spanWrite.End()

	// otherwise, put the span in the channel but don't block
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return fmt.Errorf("span channel closed")
	case w.spanChan <- span:
		w.Metrics.Increment("smartstore_span_queue_in")
	default:
		err := fmt.Errorf("span queue full")
		spanWrite.RecordError(err)
		return err
	}
	return nil
}

// processSpans processes spans from the span channel. This is run in a
// goroutine and runs indefinitely until cancelled by calling Stop().
func (w *SmartWrapper) processSpans(ctx context.Context) {
	for first := range w.spanChan {
		var spans []*CentralSpan
		spans = append(spans, first)

	Remaining:
		for len(spans) < w.Config.GetCentralStoreOptions().WriteSpanBatchSize { // For control maximum size of batch
			select {
			case span, ok := <-w.spanChan:
				if !ok {
					break Remaining
				}
				w.Metrics.Increment("smartstore_span_queue_out")

				spans = append(spans, span)
			default:
				break Remaining
			}
		}

		w.Metrics.Gauge("smartstore_write_span_batch_count", len(spans))
		err := w.BasicStore.WriteSpans(ctx, spans)
		if err != nil {
			w.Logger.Error().Logf("error writing span: %s", err)
		}
	}

	w.doneProcessing <- struct{}{}
}

// helper function for manageStates
func (w *SmartWrapper) manageTimeouts(ctx context.Context, timeout time.Duration, fromState, toState CentralTraceState) error {
	if w.BasicStore == nil {
		return fmt.Errorf("basic store is nil")
	}
	ctx, span := otelutil.StartSpanMulti(ctx, w.Tracer, "SmartWrapper.manageTimeouts", map[string]interface{}{
		"from_state": fromState,
		"to_state":   toState,
	})
	defer span.End()

	// process up to 20% of the channel size
	st, err := w.BasicStore.GetTracesForState(ctx, fromState, w.Config.GetCentralStoreOptions().StateBatchSize)
	if err != nil {
		span.RecordError(err)
		return err
	}
	if len(st) == 0 || st == nil {
		return nil
	}

	statuses, err := w.BasicStore.GetStatusForTraces(ctx, st, fromState)
	if err != nil {
		span.RecordError(err)
		return err
	}
	traceIDsToChange := make([]string, 0)
	for _, status := range statuses {
		if !status.Timestamp.IsZero() && w.Clock.Since(status.Timestamp) > timeout {
			if status.TraceID == "" {
				w.Logger.Warn().Logf("Attempted to change state from %s to %s of empty trace id", fromState, toState)
			} else {
				traceIDsToChange = append(traceIDsToChange, status.TraceID)
			}
		}
	}
	otelutil.AddSpanField(span, "num_trace_ids", len(traceIDsToChange))
	if len(traceIDsToChange) > 0 {
		w.Logger.Debug().Logf("changing %d traces from %s to %s", len(traceIDsToChange), fromState, toState)
		err = w.BasicStore.ChangeTraceStatus(ctx, traceIDsToChange, fromState, toState)
		if err != nil {
			span.RecordError(err)
			return err
		}
	}
	return nil
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
			ctx, span := otelutil.StartSpan(ctx, w.Tracer, "SmartWrapper.manageStates")

			// see if AwaitDecision traces have been waiting too long
			if err := w.manageTimeouts(ctx, time.Duration(options.DecisionTimeout), AwaitingDecision, ReadyToDecide); err != nil {
				span.RecordError(err)
				w.Logger.Error().Logf("error managing timeouts for moving traces from awaiting decision to ready to decide: %s", err)
			}

			// traces that are past SendDelay should be moved to ready for decision
			// the only errors can be syntax errors, so won't happen at runtime
			if err := w.manageTimeouts(ctx, time.Duration(options.SendDelay), DecisionDelay, ReadyToDecide); err != nil {
				span.RecordError(err)
				w.Logger.Error().Logf("error managing timeouts for moving traces from decision delay to ready to decide: %s", err)
			}

			// trace that are past TraceTimeout should be moved to waiting to decide
			if err := w.manageTimeouts(ctx, time.Duration(options.TraceTimeout), Collecting, DecisionDelay); err != nil {
				span.RecordError(err)
				w.Logger.Error().Logf("error managing timeouts for moving traces from collecting to decision delay: %s", err)
			}
			span.End()
		}
	}
}

// GetTraces fetches the current state of a trace (including all its spans) from
// the central store. The trace contains a list of CentralSpans, but note that
// these spans will usually only contain the key fields. The spans returned from
// this call should be used for making the trace decision; they should not be
// sent as telemetry unless AllFields is non-null. If the trace has a root span,
// it will be the first span in the list. GetTraces is intended to be used to
// make a trace decision, so it has the side effect of moving a trace from
// ReadyForDecision to AwaitingDecision. If the trace is not in the
// ReadyForDecision state, its state will not be changed.
func (w *SmartWrapper) GetTraces(ctx context.Context, traceID ...string) ([]*CentralTrace, error) {
	return w.BasicStore.GetTraces(ctx, traceID...)
}

// GetStatusForTraces returns the current state for a list of traces,
// including any reason information if the trace decision has been made and
// it was to keep the trace. If a trace is not found, it will be returned as
// Status:Unknown. This should be considered to be a bug in the central
// store, as the trace should have been created when the first span was
// added. Any traces with a state of DecisionKeep or DecisionDrop should be
// considered to be final and appropriately disposed of; the central store
// will not change the state of these traces after this call.
func (w *SmartWrapper) GetStatusForTraces(ctx context.Context, traceIDs []string, statesToCheck ...CentralTraceState) ([]*CentralTraceStatus, error) {
	return w.BasicStore.GetStatusForTraces(ctx, traceIDs, statesToCheck...)
}

// GetTracesForState returns a list of trace IDs that match the provided status.
func (w *SmartWrapper) GetTracesForState(ctx context.Context, state CentralTraceState, n int) ([]string, error) {
	return w.BasicStore.GetTracesForState(ctx, state, n)
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
	ctx, span := otelutil.StartSpanMulti(ctx, w.Tracer, "SmartWrapper.SetTraceStatuses", map[string]interface{}{
		"num_statuses": len(statuses),
		"num_keeps":    len(keeps),
		"num_drops":    len(drops),
	})
	defer span.End()

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
	if err != nil {
		span.RecordError(err)
	}
	return err
}

// RecordMetrics calls the RecordMetrics method on the BasicStore.
func (w *SmartWrapper) RecordMetrics(ctx context.Context) error {
	// record channel lengths as histogram but also as gauges
	w.Metrics.Histogram("smartstore_span_queue", float64(len(w.spanChan)))
	w.Metrics.Gauge("smartstore_span_queue_length", float64(len(w.spanChan)))

	return w.BasicStore.RecordMetrics(ctx)
}

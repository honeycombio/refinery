package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/honeycombio/refinery/centralstore"
	"github.com/honeycombio/refinery/generics"
	"go.opentelemetry.io/otel/trace"
)

// FakeRefineryInstance tries to do the same things to a store that Refinery is going to do.
// That means it generates traces and sends them to the central store in the same general way.
type FakeRefineryInstance struct {
	store       centralstore.SmartStorer
	traceIDchan chan string
	tracer      trace.Tracer
}

func NewFakeRefineryInstance(store centralstore.SmartStorer, tracer trace.Tracer) *FakeRefineryInstance {
	return &FakeRefineryInstance{
		store:       store,
		traceIDchan: make(chan string, 10),
		tracer:      tracer,
	}
}

func (fri *FakeRefineryInstance) Stop() {
	fri.store.(*centralstore.SmartWrapper).Stop()
}

// runSender runs a node that generates traces and sends them to the central store through
// the specified wrapper. It will generate traces until the runtime is up, and then
// stop.
func (fri *FakeRefineryInstance) runSender(opts CmdLineOptions, nodeIndex int, stopch chan struct{}) {
	out := make(chan *centralstore.CentralSpan)
	// start a goroutine to collect traces from the output channel and send them to the store
	go func() {
		for span := range out {
			fri.store.WriteSpan(span)
		}
	}()

	// start a traceTicker at the granularity to generate traces
	traceTicker := time.NewTicker(time.Duration(opts.TraceIDGranularity))
	defer traceTicker.Stop()

	// calculate when we should stop
	stopTime := time.Now().Add(time.Duration(opts.Runtime))
	for time.Now().Before(stopTime) {
		select {
		case <-stopch:
			return
		case <-traceTicker.C:
			// generate traces until we're done
			tg := NewTraceGenerator(opts, nodeIndex, fri.tracer)
			go tg.generateTrace(out, stopch, fri.traceIDchan)
		}
	}
}

func (fri *FakeRefineryInstance) runDecider(opts CmdLineOptions, nodeIndex int, stopch chan struct{}) {
	// start a ticker to check the status of traces
	// We randomize the interval a bit so that we don't all run at the same time
	statusTicker := time.NewTicker(time.Duration(500+rand.Int63n(1000)) * time.Millisecond)
	defer statusTicker.Stop()
	ctx := context.Background()

	// calculate when we should stop
	stopTime := time.Now().Add(time.Duration(opts.Runtime))
	for time.Now().Before(stopTime) {
		select {
		case <-stopch:
			return
		case <-statusTicker.C:
			ctx, root := fri.tracer.Start(ctx, "decider")
			addSpanField(root, "nodeIndex", nodeIndex)
			// get the current list of trace IDs in the ReadyForDecision state
			// note that this request also changes the state for the traces it returns to AwaitingDecision
			_, span1 := fri.tracer.Start(ctx, "get_traces_needing_decision")
			traceIDs, err := fri.store.GetTracesNeedingDecision(opts.DecisionReqSize)
			if err != nil {
				addException(span1, err)
			}
			addSpanFields(span1, map[string]interface{}{
				"decision_request_limit": opts.DecisionReqSize,
				"trace_ids":              strings.Join(traceIDs, ","),
				"num_trace_ids":          len(traceIDs),
			})
			span1.End()
			if len(traceIDs) == 0 {
				continue
			}

			statCtx, span2 := fri.tracer.Start(ctx, "get_status_for_traces")
			statuses, err := fri.store.GetStatusForTraces(traceIDs)
			if err != nil {
				fmt.Println(err)
			}
			addSpanField(span2, "num_statuses", len(statuses))

			for _, status := range statuses {
				_, span3 := fri.tracer.Start(statCtx, "make_decision")
				addSpanFields(span3, map[string]interface{}{
					"trace_id": status.TraceID,
					"state":    status.State.String(),
				})
				// make a decision on each trace
				if status.State != centralstore.AwaitingDecision {
					// someone else got to it first
					addSpanField(span3, "decision", "not ready")
					span3.End()
					continue
				}
				trace, err := fri.store.GetTrace(status.TraceID)
				if err != nil {
					addException(span3, err)
				}
				// decision criteria:
				// we're going to keep:
				// * traces having status > 500
				// * traces with POST spans having status >= 400 && <= 403
				keep := false
				for _, span := range trace.Spans {
					if span.KeyFields["status"].(int) >= 500 {
						keep = true
						break
					}
					if span.KeyFields["status"].(int) >= 400 && span.KeyFields["status"].(int) <= 403 && span.KeyFields["operation"].(string) == "POST" {
						keep = true
						break
					}
				}

				if keep {
					fmt.Printf("decider %d:  keeping trace %s\n", nodeIndex, status.TraceID)
					status.State = centralstore.DecisionKeep
				} else {
					fmt.Printf("decider %d: dropping trace %s\n", nodeIndex, status.TraceID)
					status.State = centralstore.DecisionDrop
				}
				addSpanField(span3, "decision", status.State.String())
				span3.End()
			}
			span2.End()

			_, span4 := fri.tracer.Start(ctx, "set_trace_statuses")
			err = fri.store.SetTraceStatuses(statuses)
			if err != nil {
				addException(span4, err)
			}
			span4.End()
			root.End()
		}
	}
}

// runProcessor periodically checks the status of traces and drops or sends them
func (fri *FakeRefineryInstance) runProcessor(opts CmdLineOptions, nodeIndex int, stopch chan struct{}) {
	// start a ticker to check the status of traces
	// We randomize the interval a bit so that we don't all run at the same time
	interval := time.Duration(500+rand.Int63n(1000)) * time.Millisecond
	statusTicker := time.NewTicker(interval)
	defer statusTicker.Stop()

	traceIDs := generics.NewSet[string]()

	// calculate when we should stop
	stopTime := time.Now().Add(time.Duration(opts.Runtime))
	for time.Now().Before(stopTime) {
		select {
		case <-stopch:
			return
		case tid := <-fri.traceIDchan:
			fmt.Printf("processor %d: got trace %s\n", nodeIndex, tid)
			traceIDs.Add(tid)
		case <-statusTicker.C:
			ctx, root := fri.tracer.Start(context.Background(), "processor")
			addSpanFields(root, map[string]interface{}{
				"nodeIndex":     nodeIndex,
				"interval":      interval,
				"num_trace_ids": len(traceIDs.Members()),
			})
			tids := traceIDs.Members()
			if len(tids) == 0 {
				continue
			}
			_, span := fri.tracer.Start(ctx, "get_status_for_traces")
			addSpanField(span, "num_trace_ids", len(tids))
			statuses, err := fri.store.GetStatusForTraces(tids)
			if err != nil {
				addException(span, err)
				span.End()
				continue
			}
			span.End()

			for _, status := range statuses {
				_, span := fri.tracer.Start(ctx, "act_on_decision")
				addSpanFields(span, map[string]interface{}{
					"trace_id": status.TraceID,
					"state":    status.State.String(),
				})
				if status.State == centralstore.DecisionKeep {
					fmt.Printf("processor %d: keeping trace %s\n", nodeIndex, status.TraceID)
					if err != nil {
						fmt.Println(err)
					}
					traceIDs.Remove(status.TraceID)
				} else if status.State == centralstore.DecisionDrop {
					fmt.Printf("processor %d: dropping trace %s\n", nodeIndex, status.TraceID)
					traceIDs.Remove(status.TraceID)
				} else {
					fmt.Printf("processor %d: trace %s not ready (%v)\n", nodeIndex, status.TraceID, status.State)
				}
				span.End()
			}
			root.End()
		}
	}
}

package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dgryski/go-wyhash"
	"github.com/honeycombio/otel-config-go/otelconfig"
	"github.com/honeycombio/refinery/centralstore"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/generics"
	"github.com/jessevdk/go-flags"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// This is a test program for the CentralStore. It is designed to generate load
// on a central store. It will write a number of spans to the central store, and
// then ask for the status of the associated traces.
//
// TraceIDs are generated deterministically from the current time to the nearest
// 100 milliseconds. Thus, the same traceIDs will be generated if the program is
// run at the same time on different machines, allowing multiple instances to be
// run together against the same centralstore to make sure that the centralstore
// is handling concurrent requests correctly. SpanIDs are generated randomly per
// instance so that spans from different instances don't collide.
//
// The shape of traces is simple -- a single root span with a number of children
// all claiming the root as parent. The number of children including the number
// of spans, span events, and span links is determined randomly per trace and
// these values are stored in the root span under the names "spanCount",
// "spanEventCount", and "spanLinkCount" respectively. (These values can then be
// compared to the values in "meta" to make sure that the centralstore is
// storing the correct values.)
//
// Note that there should be only one root span per trace, so when we have
// multiple instances, we need to assign each instance a different node number.
// This node number and the total node count are used along with the trace ID to
// decide if the given instance should generate a root span for the trace.
//
// Spans also contain some key fields -- "operation", "path", and "status". The
// "operation" field is a string that is randomly chosen from a list of
// operations. The "path" field is a string that is randomly chosen from a list
// of paths. The "status" field is an integer randomly chosen from from a list
// of statuses.
//
// Other than that, the spans are simple and don't contain any other fields.
//
// We accept Honeycomb API key and dataset as command line arguments. If these
// are provided, we will send the spans to Honeycomb; if they're missing, then
// we dump them to the console.
// Each of the major tasks will decorate its spans with a "task" field.

type Duration time.Duration

func (d Duration) MarshalFlag() (string, error) {
	return time.Duration(d).String(), nil
}

func (d *Duration) UnmarshalFlag(value string) error {
	v, err := time.ParseDuration(value)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// ensure Duration implements the flags.Marshaler interface
var _ flags.Marshaler = Duration(0)

func dur(s string) config.Duration {
	d, _ := time.ParseDuration(s)
	return config.Duration(d)
}

// telemetry helpers

func (fri *FakeRefineryInstance) addException(span trace.Span, err error) {
	span.AddEvent("exception", trace.WithAttributes(
		attribute.KeyValue{Key: "exception.type", Value: attribute.StringValue("error")},
		attribute.KeyValue{Key: "exception.message", Value: attribute.StringValue(err.Error())},
		attribute.KeyValue{Key: "exception.stacktrace", Value: attribute.StringValue("stacktrace")},
		attribute.KeyValue{Key: "exception.escaped", Value: attribute.BoolValue(false)},
	))
}

func addSpanField(span trace.Span, key string, value interface{}) {
	k := attribute.Key(key)
	switch v := value.(type) {
	case string:
		span.SetAttributes(attribute.KeyValue{Key: k, Value: attribute.StringValue(v)})
	case int:
		span.SetAttributes(attribute.KeyValue{Key: k, Value: attribute.IntValue(v)})
	case bool:
		span.SetAttributes(attribute.KeyValue{Key: k, Value: attribute.BoolValue(v)})
	case int64:
		span.SetAttributes(attribute.KeyValue{Key: k, Value: attribute.Int64Value(v)})
	case float64:
		span.SetAttributes(attribute.KeyValue{Key: k, Value: attribute.Float64Value(v)})
	case time.Duration:
		span.SetAttributes(attribute.KeyValue{Key: k, Value: attribute.Int64Value(int64(v))})
	}
}

func addSpanFields(span trace.Span, fields map[string]interface{}) {
	for k, v := range fields {
		addSpanField(span, k, v)
	}
}

// StateTicker should be comfortably more than the ticker in EventuallyWithT
func standardStoreOptions() centralstore.SmartWrapperOptions {
	sopts := centralstore.SmartWrapperOptions{
		SpanChannelSize: 100,
		StateTicker:     dur("150ms"),
		SendDelay:       dur("1s"),
		TraceTimeout:    dur("5s"),
		DecisionTimeout: dur("1s"),
	}
	return sopts
}

func makeRemoteStore(storeType string) centralstore.BasicStorer {
	switch storeType {
	case "local":
		return centralstore.NewLocalRemoteStore()
	default:
		panic("unknown store type " + storeType)
	}
}

type CmdLineOptions struct {
	StoreType          string   `long:"store-type" description:"Type of store to use" default:"local"`
	MinTraceLength     int      `long:"min-trace-length" description:"Minimum number of spans in a trace" default:"1"`
	MaxTraceLength     int      `long:"max-trace-length" description:"Maximum number of spans in a trace" default:"10"`
	MaxTraceDuration   Duration `long:"max-trace-duration" description:"Maximum duration in seconds for a trace" default:"10s"`
	Runtime            Duration `long:"runtime" description:"How long to run the test for" default:"20s"`
	TraceIDGranularity Duration `long:"trace-id-granularity" description:"How often to generate a new traceID" default:"1s"`
	TotalNodeCount     int      `long:"total-node-count" description:"Total number of nodes in the network" default:"1"`
	NodeCount          int      `long:"node-count" description:"Number of nodes in this instance (<= Total)" default:"1"`
	NodeIndex          int      `long:"node-number" description:"Index of this node if Total > 1" default:"0"`
	DecisionReqSize    int      `long:"decision-req-size" description:"Number of traces to request for decision" default:"10"`
	HnyAPIKey          string   `long:"hny-api-key" description:"API key for traces in Honeycomb" default:"" env:"HONEYCOMB_API_KEY"`
	HnyDataset         string   `long:"hny-dataset" description:"Dataset/service name for traces in Honeycomb" default:"refinery-store-test" env:"HONEYCOMB_DATASET"`
}

type traceGenerator struct {
	Granularity      Duration
	MinTraceLength   int
	MaxTraceLength   int
	MaxTraceDuration Duration
	TotalNodeCount   int
	NodeIndex        int
	hashseed         uint64
	tracer           trace.Tracer
}

func NewTraceGenerator(opts CmdLineOptions, index int, tracer trace.Tracer) *traceGenerator {
	return &traceGenerator{
		Granularity:      opts.TraceIDGranularity,
		MinTraceLength:   opts.MinTraceLength,
		MaxTraceLength:   opts.MaxTraceLength,
		MaxTraceDuration: opts.MaxTraceDuration,
		TotalNodeCount:   opts.TotalNodeCount,
		NodeIndex:        index,
		tracer:           tracer,
	}
}

// TraceID is generated deterministically from the current time to the nearest granularity; this
// means that the same traceIDs will be generated if the program is run at the same time on different
// machines. Creating a new traceID sets the hashseed to a value based on the traceID, which gives us
// a deterministic way to generate random numbers for the trace.
func (t *traceGenerator) getTraceID() string {
	ts := time.Now().Truncate(time.Duration(t.Granularity)).UnixMilli()
	h1 := wyhash.Hash([]byte("traceid1"), uint64(ts))
	h2 := wyhash.Hash([]byte("traceid2"), h1)
	t.hashseed = wyhash.Hash([]byte("hashseed"), h2)
	return fmt.Sprintf("%016x%016x", h1, h2)
}

// SpanID is also deterministic.
func (t *traceGenerator) getSpanID(tid string) string {
	ts := t.Uint64()
	h := wyhash.Hash([]byte(tid), uint64(ts))
	return fmt.Sprintf("%016x", h)
}

func (t *traceGenerator) getKeyFields() map[string]interface{} {
	ops := []string{"GET", "PUT", "POST", "DELETE", "HEAD"}
	paths := []string{"/path1", "/path2", "/path3", "/path4", "/path5"}
	statuses := []int{200, 201, 202, 203, 204, 400, 401, 402, 403, 404, 500}
	return map[string]interface{}{
		"operation": ops[rand.Intn(len(ops))],
		"path":      paths[rand.Intn(len(paths))],
		"status":    statuses[rand.Intn(len(statuses))],
	}
}

// mySpan returns true if the span with the given traceID and spanID should be generated by this node.
// If TotalNodeCount is 1, then we always generate the span.
func (t *traceGenerator) mySpan(traceID string, spanID string) bool {
	if t.TotalNodeCount == 1 {
		return true
	}
	h := wyhash.Hash([]byte(traceID), 4523987354)
	h = wyhash.Hash([]byte(spanID), h)
	return (h % uint64(t.TotalNodeCount)) == uint64(t.NodeIndex)
}

// Intn returns a random number between 0 and n-1, given a traceID. This
// function provides a deterministic means to let all the nodes generate
// the same values for the same generated traceID.
func (t *traceGenerator) Intn(n int) int {
	return int(t.Uint64() % uint64(n))
}

func (t *traceGenerator) Uint64() uint64 {
	t.hashseed = wyhash.Hash([]byte("next"), t.hashseed)
	return t.hashseed
}

// generateTrace generates a trace with the specified traceID, a given number of
// spans spread out for the given duration. It puts the spans into a channel and
// expects to be run as a goroutine; it terminates when it sends the root span.
// MinDuration of the trace is always the granularity.
func (t *traceGenerator) generateTrace(out chan *centralstore.CentralSpan, stop <-chan struct{}, traceIDchan chan string) {
	// create a span for our generator
	ctx, root := t.tracer.Start(context.Background(), "sender")
	defer root.End()

	// create a new traceID which also seeds the random number generator
	traceid := t.getTraceID()
	traceIDchan <- traceid

	spanCount := t.Intn(t.MaxTraceLength-t.MinTraceLength) + t.MinTraceLength

	// decide how many special spans to include
	spanEventCount := 0
	spanLinkCount := 0
	if spanCount > 4 {
		nspecials := t.Intn(spanCount - 4)
		switch nspecials {
		case 0:
			// no special spans
		case 1:
			spanEventCount = 1
		case 2:
			spanEventCount = 1
			spanLinkCount = 1
		default:
			spanEventCount = t.Intn(nspecials)
			spanLinkCount = nspecials - spanEventCount
		}
	}

	// decide how long the trace should last
	dur := time.Duration(t.Intn(int(t.MaxTraceDuration-t.Granularity)) + int(t.Granularity))
	trace := &centralstore.CentralTrace{
		TraceID: traceid,
		Spans:   make([]*centralstore.CentralSpan, 0, spanCount),
	}

	addSpanFields(root, map[string]interface{}{
		"trace_id":         traceid,
		"span_count":       spanCount,
		"node_index":       t.NodeIndex,
		"span_event_count": spanEventCount,
		"span_link_count":  spanLinkCount,
		"duration_ms":      dur / time.Millisecond,
	})

	// generate the root span
	rootFields := t.getKeyFields()
	rootFields["spanCount"] = spanCount
	rootFields["spanEventCount"] = spanEventCount
	rootFields["spanLinkCount"] = spanLinkCount
	rootSpan := &centralstore.CentralSpan{
		TraceID:   trace.TraceID,
		SpanID:    t.getSpanID(trace.TraceID),
		Type:      centralstore.SpanTypeNormal,
		KeyFields: rootFields,
		IsRoot:    true,
	}
	trace.Spans = append(trace.Spans, rootSpan)

	// generate the children
	for i := 1; i < spanCount; i++ {
		span := &centralstore.CentralSpan{
			TraceID:   trace.TraceID,
			SpanID:    t.getSpanID(trace.TraceID),
			ParentID:  trace.Spans[i-1].SpanID,
			Type:      centralstore.SpanTypeNormal,
			KeyFields: t.getKeyFields(),
		}
		trace.Spans = append(trace.Spans, span)
	}
	// the first spans after the root span are the span events
	for i := 0; i < spanEventCount; i++ {
		trace.Spans[i+1].Type = centralstore.SpanTypeEvent
	}
	// the next spans are the span links
	for i := spanEventCount; i < spanEventCount+spanLinkCount; i++ {
		trace.Spans[i+1].Type = centralstore.SpanTypeLink
	}

	// Now we have a trace, set up a timer to send all the spans in reverse
	// order spread out over the duration so the root span is last. If we see
	// the stop signal, we stop immediately and won't send any more traces (or
	// the root span).
	ticker := time.NewTicker(dur / time.Duration(spanCount))
	defer ticker.Stop()
	for counter := spanCount; counter > 0; counter-- {
		select {
		case <-stop:
			return
		case <-ticker.C:
			_, span := t.tracer.Start(ctx, "send_span")
			addSpanFields(span, map[string]interface{}{
				"trace_id": traceid,
				"span_id":  trace.Spans[counter-1].SpanID,
				"index":    counter - 1,
			})
			// only send the span if it's ours
			if t.mySpan(traceid, trace.Spans[counter-1].SpanID) {
				addSpanField(span, "sent", true)
				out <- trace.Spans[counter-1]
			} else {
				addSpanField(span, "sent", false)
			}
			span.End()
		}
	}
}

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
				fri.addException(span1, err)
			}
			addSpanFields(span1, map[string]interface{}{
				"decision_request_size": opts.DecisionReqSize,
				"trace_ids":             strings.Join(traceIDs, ","),
				"num_trace_ids":         len(traceIDs),
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
					fri.addException(span3, err)
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
				fri.addException(span4, err)
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
				fri.addException(span, err)
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

var ResourceLibrary = "test_stores"
var ResourceVersion = "dev"

func setupTracing(opts CmdLineOptions) (tracer trace.Tracer, shutdown func()) {
	if opts.HnyAPIKey != "" {
		var protocol otelconfig.Protocol = otelconfig.ProtocolHTTPProto

		otelshutdown, err := otelconfig.ConfigureOpenTelemetry(
			otelconfig.WithExporterProtocol(protocol),
			otelconfig.WithServiceName(opts.HnyDataset),
			otelconfig.WithTracesExporterEndpoint("https://api-dogfood.honeycomb.io:443"),
			otelconfig.WithMetricsEnabled(false),
			otelconfig.WithTracesEnabled(true),
			otelconfig.WithHeaders(map[string]string{
				"x-honeycomb-team": opts.HnyAPIKey,
			}),
		)
		if err != nil {
			log.Fatalf("failure configuring otel: %v", err)
		}
		return otel.Tracer(ResourceLibrary, trace.WithInstrumentationVersion(ResourceVersion)), otelshutdown
	}
	pr := noop.NewTracerProvider()
	return pr.Tracer(ResourceLibrary, trace.WithInstrumentationVersion(ResourceVersion)), func() {}
}

func main() {
	var opts CmdLineOptions

	parser := flags.NewParser(&opts, flags.Default)
	parser.Usage = `[OPTIONS] [FIELD=VALUE]...

	test_stores is a program to test the central store. It generates traces deterministically
	from any number of nodes, either within a single process or across multiple processes, and
	then sends them to the centralStore.
	`

	_, err := parser.Parse()
	if err != nil {
		switch flagsErr := err.(type) {
		case *flags.Error:
			if flagsErr.Type == flags.ErrHelp {
				os.Exit(0)
			}
			os.Exit(1)
		default:
			os.Exit(1)
		}
	}
	if opts.TotalNodeCount <= 1 && opts.NodeCount > 1 {
		opts.TotalNodeCount = opts.NodeCount
	}

	// set up a signal handler to stop the program
	stopch := make(chan struct{})
	sigsToExit := make(chan os.Signal, 1)
	signal.Notify(sigsToExit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigsToExit
		fmt.Println("Exiting on signal")
		close(stopch)
	}()

	// let's set up some OTel tracing
	tracer, shutdown := setupTracing(opts)
	defer shutdown()

	wg := &sync.WaitGroup{}
	sopts := standardStoreOptions()
	store := centralstore.NewSmartWrapper(sopts, makeRemoteStore(opts.StoreType), tracer)
	for i := 0; i < opts.NodeCount; i++ {
		inst := NewFakeRefineryInstance(store, tracer)

		wg.Add(1)
		go func(i int) {
			inst.runSender(opts, opts.NodeIndex+i, stopch)
			wg.Done()
		}(i)

		wg.Add(1)
		go func(i int) {
			inst.runDecider(opts, opts.NodeIndex+i, stopch)
			wg.Done()
		}(i)

		wg.Add(1)
		go func(i int) {
			inst.runProcessor(opts, opts.NodeIndex+i, stopch)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

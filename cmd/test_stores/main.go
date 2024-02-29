package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/honeycombio/refinery/centralstore"
	"github.com/honeycombio/refinery/config"
	"github.com/jessevdk/go-flags"
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

// we need a local duration type so we can marshal it from config
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
	case "redis":
		return centralstore.NewRedisBasicStore(&centralstore.RedisBasicStoreOptions{})
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
	HnyEndpoint        string   `long:"hny-endpoint" description:"Endpoint for traces in Honeycomb" default:"https://api.honeycomb.io" env:"HONEYCOMB_ENDPOINT"`
	HnyDataset         string   `long:"hny-dataset" description:"Dataset/service name for traces in Honeycomb" default:"refinery-store-test" env:"HONEYCOMB_DATASET"`
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

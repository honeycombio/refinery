package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	_ "go.uber.org/automaxprocs"
	"golang.org/x/exp/slices"

	"github.com/dgryski/go-wyhash"
	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
	"github.com/google/uuid"
	"github.com/honeycombio/husky"
	libhoney "github.com/honeycombio/libhoney-go"
	"github.com/honeycombio/libhoney-go/transmission"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"

	"github.com/honeycombio/refinery/app"
	"github.com/honeycombio/refinery/collect"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/configwatcher"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/internal/otelutil"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/service/debug"
	"github.com/honeycombio/refinery/sharder"
	"github.com/honeycombio/refinery/transmit"
)

// set by travis.
var BuildID string
var version string

type graphLogger struct {
}

// TODO: make this log properly
func (g graphLogger) Debugf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
	fmt.Println()
}

func main() {
	opts, err := config.NewCmdEnvOptions(os.Args)
	if err != nil {
		fmt.Printf("Command line parsing error '%s' -- call with --help for usage.\n", err)
		os.Exit(1)
	}

	if BuildID == "" {
		version = "dev"
	} else {
		version = BuildID
	}

	if opts.Version {
		fmt.Println("Version: " + version)
		os.Exit(0)
	}

	if opts.InterfaceNames {
		ifaces, err := net.Interfaces()
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			os.Exit(1)
		}
		// sort interfaces by name since there are a lot of them
		slices.SortFunc(ifaces, func(i, j net.Interface) int {
			return strings.Compare(i.Name, j.Name)
		})
		for _, i := range ifaces {
			fmt.Println(i.Name)
		}
		os.Exit(0)
	}

	// instanceID is a unique identifier for this instance of refinery.
	// We use a hash of a UUID to get a smaller string.
	h := wyhash.Hash([]byte(uuid.NewString()), 356783547862)
	instanceID := fmt.Sprintf("%08.8x", (h&0xFFFF_FFFF)^(h>>32))

	a := app.App{
		Version: version,
	}

	c, err := config.NewConfig(opts)
	if err != nil {
		fmt.Printf("%+v\n", err)
		os.Exit(1)
	}
	if opts.Validate {
		fmt.Println("Config and Rules validated successfully.")
		os.Exit(0)
	}
	// get desired implementation for each dependency to inject
	lgr := logger.GetLoggerImplementation(c)
	collector := collect.GetCollectorImplementation(c)
	metricsSingleton := metrics.GetMetricsImplementation(c)
	shrdr := sharder.GetSharderImplementation(c)
	samplerFactory := &sample.SamplerFactory{}

	// set log level
	logLevel := c.GetLoggerLevel().String()
	if err := lgr.SetLevel(logLevel); err != nil {
		fmt.Printf("unable to set logging level: %v\n", err)
		os.Exit(1)
	}

	// when refinery receives a shutdown signal, we need to
	// immediately let its peers know so they can stop sending
	// data to it.
	done := make(chan struct{})
	// set up the peer management and pubsub implementations
	var peers peer.Peers
	var pubsubber pubsub.PubSub
	ptype := c.GetPeerManagementType()
	switch ptype {
	case "file":
		// In the case of file peers, we do not use Redis for anything, including pubsub, so
		// we use the local pubsub implementation. Even if we have multiple peers, these
		// peers cannot communicate using pubsub.
		peers = &peer.FilePeers{Done: done}
		pubsubber = &pubsub.LocalPubSub{}
	case "redis":
		// if we're using redis, we need to set it up for both peers and pubsub
		peers = &peer.RedisPubsubPeers{Done: done}
		pubsubber = &pubsub.GoRedisPubSub{}
	case "google":
		// if we're using redis, we need to set it up for both peers and pubsub
		peers = &peer.RedisPubsubPeers{Done: done}
		pubsubber = &pubsub.GooglePubSub{}
	default:
		// this should have been caught by validation
		panic("invalid config option 'PeerManagement.Type'")
	}

	// upstreamTransport is the http transport used to send things on to Honeycomb
	upstreamTransport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout: 10 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 15 * time.Second,
		ForceAttemptHTTP2:   true,
	}

	genericMetricsRecorder := metrics.NewMetricsPrefixer("")
	upstreamMetricsRecorder := metrics.NewMetricsPrefixer("libhoney_upstream")
	peerMetricsRecorder := metrics.NewMetricsPrefixer("libhoney_peer")

	userAgentAddition := "refinery/" + version
	upstreamClient, err := libhoney.NewClient(libhoney.ClientConfig{
		Transmission: &transmission.Honeycomb{
			MaxBatchSize:          c.GetTracesConfig().GetMaxBatchSize(),
			BatchTimeout:          time.Duration(c.GetTracesConfig().GetBatchTimeout()),
			MaxConcurrentBatches:  libhoney.DefaultMaxConcurrentBatches,
			PendingWorkCapacity:   uint(c.GetUpstreamBufferSize()),
			UserAgentAddition:     userAgentAddition,
			Transport:             upstreamTransport,
			BlockOnSend:           true,
			EnableMsgpackEncoding: true,
			Metrics:               upstreamMetricsRecorder,
		},
	})
	if err != nil {
		fmt.Printf("unable to initialize upstream libhoney client")
		os.Exit(1)
	}

	var peerSender transmission.Sender
	if c.GetPeerTransmission() == "google" {
		peerSender = &transmit.GooglePeerTransmission{
			EventHandler: a.PeerRouter.ProcessEventDirect,
		}
	} else {
		// peerTransport is the http transport used to send things to a local peer
		peerTransport := &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout: 3 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: 1200 * time.Millisecond,
			ForceAttemptHTTP2:   true,
		}
		peerSender = &transmission.Honeycomb{
			MaxBatchSize:          c.GetTracesConfig().GetMaxBatchSize(),
			BatchTimeout:          time.Duration(c.GetTracesConfig().GetBatchTimeout()),
			MaxConcurrentBatches:  libhoney.DefaultMaxConcurrentBatches,
			PendingWorkCapacity:   uint(c.GetPeerBufferSize()),
			UserAgentAddition:     userAgentAddition,
			Transport:             peerTransport,
			DisableCompression:    !c.GetCompressPeerCommunication(),
			EnableMsgpackEncoding: true,
			Metrics:               peerMetricsRecorder,
		}
	}

	peerClient, err := libhoney.NewClient(libhoney.ClientConfig{
		Transmission: peerSender,
	})
	if err != nil {
		fmt.Printf("unable to initialize peer libhoney client")
		os.Exit(1)
	}

	stressRelief := &collect.StressRelief{Done: done}
	upstreamTransmission := transmit.NewDefaultTransmission(upstreamClient, upstreamMetricsRecorder, "upstream")
	peerTransmission := transmit.NewDefaultTransmission(peerClient, peerMetricsRecorder, "peer")

	// we need to include all the metrics types so we can inject them in case they're needed
	// but we only want to instantiate the ones that are enabled with non-null values
	var legacyMetrics metrics.Metrics = &metrics.NullMetrics{}
	var promMetrics metrics.Metrics = &metrics.NullMetrics{}
	var oTelMetrics metrics.Metrics = &metrics.NullMetrics{}
	if c.GetLegacyMetricsConfig().Enabled {
		legacyMetrics = &metrics.LegacyMetrics{}
	}
	if c.GetPrometheusMetricsConfig().Enabled {
		promMetrics = &metrics.PromMetrics{}
	}
	if c.GetOTelMetricsConfig().Enabled {
		oTelMetrics = &metrics.OTelMetrics{}
	}

	refineryHealth := &health.Health{}

	resourceLib := "refinery"
	resourceVer := version
	tracer := trace.Tracer(noop.Tracer{})
	shutdown := func() {}

	if c.GetOTelTracingConfig().Enabled {
		// let's set up some OTel tracing
		tracer, shutdown = otelutil.SetupTracing(c.GetOTelTracingConfig(), resourceLib, resourceVer)

		// add telemetry callback so husky can enrich spans with attributes
		husky.AddTelemetryAttributeFunc = func(ctx context.Context, key string, value any) {
			span := trace.SpanFromContext(ctx)
			switch v := value.(type) {
			case string:
				span.SetAttributes(attribute.String(key, v))
			case bool:
				span.SetAttributes(attribute.Bool(key, v))
			case int:
				span.SetAttributes(attribute.Int(key, v))
			case float64:
				span.SetAttributes(attribute.Float64(key, v))
			default:
				span.SetAttributes(attribute.String(key, fmt.Sprintf("%v", v)))
			}
		}
	}
	defer shutdown()

	// we need to include all the metrics types so we can inject them in case they're needed
	var g inject.Graph
	if opts.Debug {
		g.Logger = graphLogger{}
	}
	objects := []*inject.Object{
		{Value: c},
		{Value: peers},
		{Value: pubsubber},
		{Value: lgr},
		{Value: upstreamTransport, Name: "upstreamTransport"},
		{Value: upstreamTransmission, Name: "upstreamTransmission"},
		{Value: peerTransmission, Name: "peerTransmission"},
		{Value: shrdr},
		{Value: collector},
		{Value: legacyMetrics, Name: "legacyMetrics"},
		{Value: promMetrics, Name: "promMetrics"},
		{Value: oTelMetrics, Name: "otelMetrics"},
		{Value: tracer, Name: "tracer"}, // we need to use a named injection here because trace.Tracer's struct fields are all private
		{Value: clockwork.NewRealClock()},
		{Value: metricsSingleton, Name: "metrics"},
		{Value: genericMetricsRecorder, Name: "genericMetrics"},
		{Value: upstreamMetricsRecorder, Name: "upstreamMetrics"},
		{Value: peerMetricsRecorder, Name: "peerMetrics"},
		{Value: version, Name: "version"},
		{Value: samplerFactory},
		{Value: stressRelief, Name: "stressRelief"},
		{Value: refineryHealth},
		{Value: &configwatcher.ConfigWatcher{}},
		{Value: &a},
		{Value: instanceID, Name: "instanceID"},
	}
	err = g.Provide(objects...)
	if err != nil {
		fmt.Printf("failed to provide injection graph. error: %+v\n", err)
		os.Exit(1)
	}

	if opts.Debug {
		err = g.Provide(&inject.Object{Value: &debug.DebugService{Config: c}})
		if err != nil {
			fmt.Printf("failed to provide injection graph. error: %+v\n", err)
			os.Exit(1)
		}
	}

	if err := g.Populate(); err != nil {
		fmt.Printf("failed to populate injection graph. error: %+v\n", err)
		os.Exit(1)
	}

	// the logger provided to startstop must be valid before any service is
	// started, meaning it can't rely on injected configs. make a custom logger
	// just for this step (and for shutdown)
	ststLogger := logrus.New()
	// level, _ := logrus.ParseLevel(logLevel)
	ststLogger.SetLevel(logrus.DebugLevel)

	if err := startstop.Start(g.Objects(), ststLogger); err != nil {
		fmt.Printf("failed to start injected dependencies. error: %+v\n", err)
		os.Exit(1)
	}

	// Now that all components are started, we can notify our peers that we are ready
	// to receive data.
	err = peers.Ready()
	if err != nil {
		fmt.Printf("failed to start peer management: %v\n", err)
		os.Exit(1)
	}

	// these have to be done after the injection (of metrics)
	// these are the metrics that libhoney will emit; we preregister them so that they always appear
	for _, metric := range libhoneyMetrics {
		upstreamMetricsRecorder.Register(metric)
		peerMetricsRecorder.Register(metric)
	}

	// Register metrics after the metrics object has been created
	peerTransmission.RegisterMetrics()
	upstreamTransmission.RegisterMetrics()

	metricsSingleton.Store("UPSTREAM_BUFFER_SIZE", float64(c.GetUpstreamBufferSize()))
	metricsSingleton.Store("PEER_BUFFER_SIZE", float64(c.GetPeerBufferSize()))

	// set up signal channel to exit, and allow a second try to kill everything
	// immediately.
	sigsToExit := make(chan os.Signal, 1)
	// this is the signal that the goroutine sends
	exitWait := make(chan struct{})
	// this is the channel the goroutine uses to stop; it has to be unique since it's
	// the last thing we do
	monitorDone := make(chan struct{})
	// the signal gets sent to sigsToExit, which is monitored by the goroutine below
	signal.Notify(sigsToExit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		// first attempt does a normal close
		forceExit := false
		for {
			select {
			case <-sigsToExit:
				// if some part of refinery is already dead, don't
				// attempt an orderly shutdown, just die
				if !refineryHealth.IsAlive() {
					ststLogger.Logf(logrus.ErrorLevel, "at least one subsystem is not alive, exiting immediately")
					os.Exit(1)
				}

				// if this is true they've tried more than once, so get out
				// without trying to be clean about it
				if forceExit {
					ststLogger.Logf(logrus.ErrorLevel, "immediate exit forced by second signal")
					os.Exit(2)
				}
				forceExit = true
				close(exitWait)
			case <-monitorDone:
				return
			}
		}
	}()

	// block on our signal handler to exit
	sig := <-exitWait
	// unregister ourselves before we go
	close(done)
	time.Sleep(100 * time.Millisecond)
	a.Logger.Error().WithField("signal", sig).Logf("Caught OS signal")

	// these are the subsystems that might not shut down properly, so we're
	// going to call this manually so that if something blocks on shutdown, you
	// can still send a signal that will get heard.
	startstop.Stop(g.Objects(), ststLogger)
	close(monitorDone)
	close(sigsToExit)
}

var libhoneyMetrics = []metrics.Metadata{
	{
		Name:        "queue_length",
		Type:        metrics.Gauge,
		Unit:        metrics.Dimensionless,
		Description: "number of events waiting to be sent to destination",
	},
	{
		Name:        "queue_overflow",
		Type:        metrics.Counter,
		Unit:        metrics.Dimensionless,
		Description: "number of events dropped due to queue overflow",
	},
	{
		Name:        "send_errors",
		Type:        metrics.Counter,
		Unit:        metrics.Dimensionless,
		Description: "number of errors encountered while sending events to destination",
	},
	{
		Name:        "send_retries",
		Type:        metrics.Counter,
		Unit:        metrics.Dimensionless,
		Description: "number of times a batch of events was retried",
	},
	{
		Name:        "batches_sent",
		Type:        metrics.Counter,
		Unit:        metrics.Dimensionless,
		Description: "number of batches of events sent to destination",
	},
	{
		Name:        "messages_sent",
		Type:        metrics.Counter,
		Unit:        metrics.Dimensionless,
		Description: "number of messages sent to destination",
	},
	{
		Name:        "response_decode_errors",
		Type:        metrics.Counter,
		Unit:        metrics.Dimensionless,
		Description: "number of errors encountered while decoding responses from destination",
	},
}

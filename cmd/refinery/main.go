package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	_ "go.uber.org/automaxprocs"
	"golang.org/x/exp/slices"

	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
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

	a := app.App{
		Version: version,
	}

	c, err := config.NewConfig(opts, func(err error) {
		if a.Logger != nil {
			a.Logger.Error().WithField("error", err).Logf("error loading config")
		}
	})
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

	// peerTransport is the http transport used to send things to a local peer
	peerTransport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout: 3 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 1200 * time.Millisecond,
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

	peerClient, err := libhoney.NewClient(libhoney.ClientConfig{
		Transmission: &transmission.Honeycomb{
			MaxBatchSize:          c.GetTracesConfig().GetMaxBatchSize(),
			BatchTimeout:          time.Duration(c.GetTracesConfig().GetBatchTimeout()),
			MaxConcurrentBatches:  libhoney.DefaultMaxConcurrentBatches,
			PendingWorkCapacity:   uint(c.GetPeerBufferSize()),
			UserAgentAddition:     userAgentAddition,
			Transport:             peerTransport,
			DisableCompression:    !c.GetCompressPeerCommunication(),
			EnableMsgpackEncoding: true,
			Metrics:               peerMetricsRecorder,
		},
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

	resourceLib := "refinery"
	resourceVer := version
	tracer := trace.Tracer(noop.Tracer{})
	shutdown := func() {}

	if c.GetOTelTracingConfig().Enabled {
		// let's set up some OTel tracing
		tracer, shutdown = otelutil.SetupTracing(c.GetOTelTracingConfig(), resourceLib, resourceVer)
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
		{Value: peerTransport, Name: "peerTransport"},
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
		{Value: &health.Health{}},
		{Value: &configwatcher.ConfigWatcher{}},
		{Value: &a},
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
	// just for this step
	ststLogger := logrus.New()
	// level, _ := logrus.ParseLevel(logLevel)
	ststLogger.SetLevel(logrus.DebugLevel)

	// we can stop all the objects in one call, but we need to start the
	// transmissions manually.
	defer startstop.Stop(g.Objects(), ststLogger)
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
	libhoneyMetricsName := map[string]string{
		"queue_length":           "gauge",
		"queue_overflow":         "counter",
		"send_errors":            "counter",
		"send_retries":           "counter",
		"batches_sent":           "counter",
		"messages_sent":          "counter",
		"response_decode_errors": "counter",
	}
	for name, typ := range libhoneyMetricsName {
		upstreamMetricsRecorder.Register(name, typ)
		peerMetricsRecorder.Register(name, typ)
	}

	metricsSingleton.Store("UPSTREAM_BUFFER_SIZE", float64(c.GetUpstreamBufferSize()))
	metricsSingleton.Store("PEER_BUFFER_SIZE", float64(c.GetPeerBufferSize()))

	// set up signal channel to exit
	sigsToExit := make(chan os.Signal, 1)
	signal.Notify(sigsToExit, syscall.SIGINT, syscall.SIGTERM)

	// block on our signal handler to exit
	sig := <-sigsToExit
	// unregister ourselves before we go
	close(done)
	time.Sleep(100 * time.Millisecond)
	a.Logger.Error().Logf("Caught signal \"%s\"", sig)
}

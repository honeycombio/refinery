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

	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
	libhoney "github.com/honeycombio/libhoney-go"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	_ "go.uber.org/automaxprocs"
	"golang.org/x/exp/slices"

	"github.com/honeycombio/libhoney-go/transmission"
	"github.com/honeycombio/refinery/app"
	"github.com/honeycombio/refinery/centralstore"
	"github.com/honeycombio/refinery/collect"
	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/collect/stressRelief"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/gossip"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/internal/otelutil"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/redis"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/service/debug"
	"github.com/honeycombio/refinery/transmit"
)

// set by CI.
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

	cfg, err := config.NewConfig(opts, func(err error) {
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
	cfg.RegisterReloadCallback(func() {
		if a.Logger != nil {
			a.Logger.Info().Logf("configuration change was detected and the configuration was reloaded")
		}
	})

	// get desired implementation for each dependency to inject
	lgr := logger.GetLoggerImplementation(cfg)
	centralcollector := &collect.CentralCollector{}
	metricsSingleton := metrics.GetMetricsImplementation(cfg)
	samplerFactory := &sample.SamplerFactory{}

	// set log level
	logLevel := cfg.GetLoggerLevel().String()
	if err := lgr.SetLevel(logLevel); err != nil {
		fmt.Printf("unable to set logging level: %v\n", err)
		os.Exit(1)
	}

	done := make(chan struct{})

	if err != nil {
		fmt.Printf("unable to load peers: %+v\n", err)
		os.Exit(1)
	}

	// upstreamTransport is the http transport used to send things on to Honeycomb
	upstreamTransport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout: 10 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 15 * time.Second,
	}

	genericMetricsRecorder := metrics.NewMetricsPrefixer("")
	upstreamMetricsRecorder := metrics.NewMetricsPrefixer("libhoney_upstream")

	userAgentAddition := "refinery/" + version
	upstreamClient, err := libhoney.NewClient(libhoney.ClientConfig{
		Transmission: &transmission.Honeycomb{
			MaxBatchSize:          cfg.GetMaxBatchSize(),
			BatchTimeout:          cfg.GetBatchTimeout(),
			MaxConcurrentBatches:  libhoney.DefaultMaxConcurrentBatches,
			PendingWorkCapacity:   uint(cfg.GetUpstreamBufferSize()),
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

	stressRelief := &stressRelief.StressRelief{}
	upstreamTransmission := transmit.NewDefaultTransmission(upstreamClient, upstreamMetricsRecorder, "upstream")

	// we need to include all the metrics types so we can inject them in case they're needed
	// but we only want to instantiate the ones that are enabled with non-null values
	var legacyMetrics metrics.Metrics = &metrics.NullMetrics{}
	var promMetrics metrics.Metrics = &metrics.NullMetrics{}
	var oTelMetrics metrics.Metrics = &metrics.NullMetrics{}
	if cfg.GetLegacyMetricsConfig().Enabled {
		legacyMetrics = &metrics.LegacyMetrics{}
	}
	if cfg.GetPrometheusMetricsConfig().Enabled {
		promMetrics = &metrics.PromMetrics{}
	}
	if cfg.GetOTelMetricsConfig().Enabled {
		oTelMetrics = &metrics.OTelMetrics{}
	}
	decisionCache := &cache.CuckooSentCache{}

	var basicStore centralstore.BasicStorer
	var channels gossip.Gossiper
	switch cfg.GetCentralStoreOptions().BasicStoreType {
	case "redis":
		basicStore = &centralstore.RedisBasicStore{}
		channels = &gossip.GossipRedis{}
	case "local":
		basicStore = &centralstore.LocalStore{}
		channels = &gossip.InMemoryGossip{}
	default:
		fmt.Printf("unknown basic store type: %s\n", cfg.GetCentralStoreOptions().BasicStoreType)
		os.Exit(1)
	}
	smartStore := &centralstore.SmartWrapper{}

	resourceLib := "refinery"
	resourceVer := version
	var tracer trace.Tracer
	shutdown := func() {}
	switch cfg.GetOTelTracingConfig().Type {
	case "none":
		tracer = trace.Tracer(noop.Tracer{})
	case "otel":
		// let's set up some OTel tracing
		tracer, shutdown = otelutil.SetupTracing(cfg.GetOTelTracingConfig(), resourceLib, resourceVer)
	default:
		fmt.Printf("unknown tracing type: %s\n", cfg.GetOTelTracingConfig().Type)
		os.Exit(1)
	}
	defer shutdown()

	// we need to include all the metrics types so we can inject them in case they're needed
	// The "recorders" are the ones that various parts of the system will use to record metrics.
	// The "singleton" is a wrapper that contains whichever of the specific metrics implementations
	// are enabled. It's used to provide a consistent interface to the rest of the system.
	var g inject.Graph
	if opts.Debug {
		g.Logger = graphLogger{}
	}
	objects := []*inject.Object{
		{Value: cfg},
		{Value: lgr},
		{Value: upstreamTransport, Name: "upstreamTransport"},
		{Value: upstreamTransmission, Name: "upstreamTransmission"},
		{Value: &cache.SpanCache_basic{}},
		{Value: centralcollector, Name: "collector"},
		{Value: decisionCache},
		{Value: legacyMetrics, Name: "legacyMetrics"},
		{Value: promMetrics, Name: "promMetrics"},
		{Value: oTelMetrics, Name: "otelMetrics"},
		{Value: metricsSingleton, Name: "metrics"},
		{Value: genericMetricsRecorder, Name: "genericMetrics"},
		{Value: upstreamMetricsRecorder, Name: "upstreamMetrics"},
		{Value: version, Name: "version"},
		{Value: samplerFactory},
		{Value: channels, Name: "gossip"},
		{Value: stressRelief, Name: "stressRelief"},
		{Value: tracer, Name: "tracer"},
		{Value: clockwork.NewRealClock()},
		{Value: basicStore},
		{Value: smartStore},
		{Value: &health.Health{}},
		{Value: &a},
	}

	if cfg.GetCentralStoreOptions().BasicStoreType == "redis" {
		objects = append(objects, &inject.Object{Value: &redis.DefaultClient{}, Name: "redis"})
	}
	err = g.Provide(objects...)
	if err != nil {
		fmt.Printf("failed to provide injection graph. error: %+v\n", err)
		os.Exit(1)
	}

	if opts.Debug {
		err = g.Provide(&inject.Object{Value: &debug.DebugService{Config: cfg}})
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
	}

	metricsSingleton.Store("UPSTREAM_BUFFER_SIZE", float64(cfg.GetUpstreamBufferSize()))

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

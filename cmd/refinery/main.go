package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "go.uber.org/automaxprocs"

	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
	libhoney "github.com/honeycombio/libhoney-go"
	"github.com/honeycombio/libhoney-go/transmission"
	flag "github.com/jessevdk/go-flags"
	"github.com/sirupsen/logrus"

	"github.com/honeycombio/refinery/app"
	"github.com/honeycombio/refinery/collect"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/service/debug"
	"github.com/honeycombio/refinery/sharder"
	"github.com/honeycombio/refinery/transmit"
)

// set by travis.
var BuildID string
var version string

type Options struct {
	ConfigFile     string `short:"c" long:"config" description:"Path to config file" default:"/etc/refinery/refinery.toml"`
	RulesFile      string `short:"r" long:"rules_config" description:"Path to rules config file" default:"/etc/refinery/rules.toml"`
	Version        bool   `short:"v" long:"version" description:"Print version number and exit"`
	Debug          bool   `short:"d" long:"debug" description:"If enabled, runs debug service (runs on the first open port between localhost:6060 and :6069 by default)"`
	InterfaceNames bool   `long:"interface-names" description:"If set, print system's network interface names and exit."`
}

type graphLogger struct {
}

// TODO: make this log properly
func (g graphLogger) Debugf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
	fmt.Println()
}

func main() {
	var opts Options
	flagParser := flag.NewParser(&opts, flag.Default)
	if extraArgs, err := flagParser.Parse(); err != nil || len(extraArgs) != 0 {
		fmt.Println("command line parsing error - call with --help for usage")
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
		for _, i := range ifaces {
			fmt.Println(i.Name)
		}
		os.Exit(0)
	}

	a := app.App{
		Version: version,
	}

	c, err := config.NewConfig(opts.ConfigFile, opts.RulesFile, func(err error) {
		if a.Logger != nil {
			a.Logger.Error().WithField("error", err).Logf("error reloading config")
		}
	})
	if err != nil {
		fmt.Printf("unable to load config: %+v\n", err)
		os.Exit(1)
	}

	// get desired implementation for each dependency to inject
	lgr := logger.GetLoggerImplementation(c)
	collector := collect.GetCollectorImplementation(c)
	metricsSingleton := metrics.GetMetricsImplementation(c)
	shrdr := sharder.GetSharderImplementation(c)
	samplerFactory := &sample.SamplerFactory{}

	// set log level
	logLevel, err := c.GetLoggingLevel()
	if err != nil {
		fmt.Printf("unable to get logging level from config: %v\n", err)
		os.Exit(1)
	}
	if err := lgr.SetLevel(logLevel); err != nil {
		fmt.Printf("unable to set logging level: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.GetPeerTimeout())
	defer cancel()
	done := make(chan struct{})
	peers, err := peer.NewPeers(ctx, c, done)

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

	// peerTransport is the http transport used to send things to a local peer
	peerTransport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout: 3 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 1200 * time.Millisecond,
	}

	genericMetricsRecorder := metrics.NewMetricsPrefixer("")
	upstreamMetricsRecorder := metrics.NewMetricsPrefixer("libhoney_upstream")
	peerMetricsRecorder := metrics.NewMetricsPrefixer("libhoney_peer")

	userAgentAddition := "refinery/" + version
	upstreamClient, err := libhoney.NewClient(libhoney.ClientConfig{
		Transmission: &transmission.Honeycomb{
			MaxBatchSize:          c.GetMaxBatchSize(),
			BatchTimeout:          c.GetBatchTimeout(),
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
			MaxBatchSize:          c.GetMaxBatchSize(),
			BatchTimeout:          c.GetBatchTimeout(),
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

	var g inject.Graph
	if opts.Debug {
		g.Logger = graphLogger{}
	}
	err = g.Provide(
		&inject.Object{Value: c},
		&inject.Object{Value: peers},
		&inject.Object{Value: lgr},
		&inject.Object{Value: upstreamTransport, Name: "upstreamTransport"},
		&inject.Object{Value: peerTransport, Name: "peerTransport"},
		&inject.Object{Value: transmit.NewDefaultTransmission(upstreamClient, upstreamMetricsRecorder, "upstream"), Name: "upstreamTransmission"},
		&inject.Object{Value: transmit.NewDefaultTransmission(peerClient, peerMetricsRecorder, "peer"), Name: "peerTransmission"},
		&inject.Object{Value: shrdr},
		&inject.Object{Value: collector},
		&inject.Object{Value: metricsSingleton, Name: "metrics"},
		&inject.Object{Value: genericMetricsRecorder, Name: "genericMetrics"},
		&inject.Object{Value: upstreamMetricsRecorder, Name: "upstreamMetrics"},
		&inject.Object{Value: peerMetricsRecorder, Name: "peerMetrics"},
		&inject.Object{Value: version, Name: "version"},
		&inject.Object{Value: samplerFactory},
		&inject.Object{Value: stressRelief, Name: "stressRelief"},
		&inject.Object{Value: &a},
	)
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
	level, _ := logrus.ParseLevel(logLevel)
	ststLogger.SetLevel(level)

	defer startstop.Stop(g.Objects(), ststLogger)
	if err := startstop.Start(g.Objects(), ststLogger); err != nil {
		fmt.Printf("failed to start injected dependencies. error: %+v\n", err)
		os.Exit(1)
	}

	// these have to be done after the injection (of metrics)
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

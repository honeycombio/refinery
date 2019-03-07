package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	libhoney "github.com/honeycombio/libhoney-go"
	"github.com/honeycombio/libhoney-go/transmission"
	statsd "gopkg.in/alexcesaro/statsd.v2"

	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
	flag "github.com/jessevdk/go-flags"
	"github.com/sirupsen/logrus"

	"github.com/honeycombio/samproxy/app"
	"github.com/honeycombio/samproxy/collect"
	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/metrics"
	"github.com/honeycombio/samproxy/sample"
	"github.com/honeycombio/samproxy/sharder"
	"github.com/honeycombio/samproxy/transmit"
)

// set by travis.
var BuildID string
var version string

type Options struct {
	ConfiFile string `short:"c" long:"config" description:"Path to config file" default:"/etc/samproxy/samproxy.toml"`
	Version   bool   `short:"v" long:"version" description:"Print version number and exit"`
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
		version = "0." + BuildID
	}

	if opts.Version {
		fmt.Println("Version: " + version)
		os.Exit(0)
	}

	c := &config.FileConfig{Path: opts.ConfiFile}
	c.Start()

	a := app.App{}

	// get desired implementation for each dependency to inject
	lgr := logger.GetLoggerImplementation(c)
	collector := collect.GetCollectorImplementation(c)
	metricsr := metrics.GetMetricsImplementation(c)
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

	// upstreamTransport is the http transport used to send things on to Honeycomb
	upstreamTransport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout: 10 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 15 * time.Second,
	}

	// peerTransport is the http transport used to send things to a local peer
	peerTransport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout: 3 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 1200 * time.Millisecond,
	}

	sdUpstream, _ := statsd.New(statsd.Prefix("samproxy.upstream"))
	sdPeer, _ := statsd.New(statsd.Prefix("samproxy.peer"))

	userAgentAddition := "samproxy/" + version
	upstreamClient, err := libhoney.NewClient(libhoney.ClientConfig{
		Transmission: &transmission.Honeycomb{
			MaxBatchSize:         500,
			BatchTimeout:         libhoney.DefaultBatchTimeout,
			MaxConcurrentBatches: libhoney.DefaultMaxConcurrentBatches,
			PendingWorkCapacity:  uint(c.GetUpstreamBufferSize()),
			UserAgentAddition:    userAgentAddition,
			Transport:            upstreamTransport,
			BlockOnSend:          true,
			Metrics:              sdUpstream,
		},
	})
	if err != nil {
		fmt.Printf("unable to initialize upstream libhoney client")
		os.Exit(1)
	}

	peerClient, err := libhoney.NewClient(libhoney.ClientConfig{
		Transmission: &transmission.Honeycomb{
			MaxBatchSize:         500,
			BatchTimeout:         libhoney.DefaultBatchTimeout,
			MaxConcurrentBatches: libhoney.DefaultMaxConcurrentBatches,
			PendingWorkCapacity:  uint(c.GetPeerBufferSize()),
			UserAgentAddition:    userAgentAddition,
			Transport:            peerTransport,
			BlockOnSend:          true,
			// gzip compression is expensive, and peers are most likely close to each other
			// so we can turn off gzip when forwarding to peers
			DisableGzipCompression: true,
			Metrics:                sdPeer,
		},
	})
	if err != nil {
		fmt.Printf("unable to initialize upstream libhoney client")
		os.Exit(1)
	}

	var g inject.Graph
	err = g.Provide(
		&inject.Object{Value: c},
		&inject.Object{Value: lgr},
		&inject.Object{Value: upstreamTransport, Name: "upstreamTransport"},
		&inject.Object{Value: peerTransport, Name: "peerTransport"},
		&inject.Object{Value: &transmit.DefaultTransmission{LibhClient: upstreamClient}, Name: "upstreamTransmission"},
		&inject.Object{Value: &transmit.DefaultTransmission{LibhClient: peerClient}, Name: "peerTransmission"},
		&inject.Object{Value: shrdr},
		&inject.Object{Value: collector},
		&inject.Object{Value: metricsr},
		&inject.Object{Value: version, Name: "version"},
		&inject.Object{Value: samplerFactory},
		&inject.Object{Value: &a},
	)
	if err != nil {
		fmt.Printf("failed to provide injection graph. error: %+v\n", err)
		os.Exit(1)
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
}

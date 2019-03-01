package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	libhoney "github.com/honeycombio/libhoney-go"
	"github.com/honeycombio/libhoney-go/transmission"

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
	"github.com/honeycombio/samproxy/types"
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
			Timeout: 1 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 1200 * time.Millisecond,
	}

	userAgentAddition := "samproxy/" + version
	upstreamClient, err := libhoney.NewClient(libhoney.ClientConfig{
		Transmission: &transmission.Honeycomb{
			MaxBatchSize:         libhoney.DefaultMaxBatchSize,
			BatchTimeout:         libhoney.DefaultBatchTimeout,
			MaxConcurrentBatches: libhoney.DefaultMaxConcurrentBatches,
			PendingWorkCapacity:  libhoney.DefaultPendingWorkCapacity,
			UserAgentAddition:    userAgentAddition,
			Transport:            upstreamTransport,
		},
	})
	if err != nil {
		fmt.Printf("unable to initialize upstream libhoney client")
		os.Exit(1)
	}
	go readResponses(upstreamClient, lgr, metricsr)

	peerClient, err := libhoney.NewClient(libhoney.ClientConfig{
		Transmission: &transmission.Honeycomb{
			MaxBatchSize:         libhoney.DefaultMaxBatchSize,
			BatchTimeout:         libhoney.DefaultBatchTimeout,
			MaxConcurrentBatches: libhoney.DefaultMaxConcurrentBatches,
			PendingWorkCapacity:  libhoney.DefaultPendingWorkCapacity,
			UserAgentAddition:    userAgentAddition,
			Transport:            peerTransport,
			// gzip compression is expensive, and peers are most likely close to each other
			// so we can turn off gzip when forwarding to peers
			DisableGzipCompression: true,
		},
	})
	if err != nil {
		fmt.Printf("unable to initialize upstream libhoney client")
		os.Exit(1)
	}
	go readResponses(peerClient, lgr, metricsr)

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

// readResponses reads the responses from the libhoney responses queue and logs
// any errors that come down it
func readResponses(libhC *libhoney.Client, lgr logger.Logger, metricsr metrics.Metrics) {
	metricsr.Register(fmt.Sprintf("libhoney_transmit_failure.%s", types.TargetUnknown), "counter")
	metricsr.Register(fmt.Sprintf("libhoney_transmit_failure.%s", types.TargetPeer), "counter")
	metricsr.Register(fmt.Sprintf("libhoney_transmit_failure.%s", types.TargetUpstream), "counter")
	resps := libhC.TxResponses()
	for resp := range resps {
		if resp.Err != nil || resp.StatusCode > 202 {
			log := lgr.WithFields(map[string]interface{}{
				"status_code": resp.StatusCode,
				"body":        string(resp.Body),
				"duration":    resp.Duration,
			})
			// what kind of event was this? Metadat should be "peer" or "upstream"
			evDetail, ok := resp.Metadata.(map[string]string)
			if ok {
				log.WithFields(map[string]interface{}{
					"type":     evDetail["type"],
					"target":   evDetail["target"],
					"api_host": evDetail["api_host"],
					"dataset":  evDetail["dataset"],
				})
				metricsr.IncrementCounter(fmt.Sprintf("libhoney_transmit_failure.%s", evDetail["target"]))
			} else {
				metricsr.IncrementCounter(fmt.Sprintf("libhoney_transmit_failure.%s", types.TargetUnknown))
			}
			// read response, log if there's an error
			switch {
			case resp.Err != nil:
				log.WithField("error", resp.Err.Error()).Errorf("got an error back trying to send span")
			case resp.StatusCode > 202:
				log.Errorf("got an unexpected status code back trying to send span")
			}
		}
	}
}

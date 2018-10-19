package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

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
	ConfiFile string `short:"c" long:"config" description:"Path to config file" default:"./config.toml"`
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
	sampler := sample.GetSamplerImplementation(c)
	metricsr := metrics.GetMetricsImplementation(c)
	shrdr := sharder.GetSharderImplementation(c)

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

	// upstreamClient is the http client used to send things on to Honeycomb
	upstreamClient := &http.Client{
		Timeout: time.Second * 10,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 5 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: 6 * time.Second,
		},
	}

	var g inject.Graph
	err = g.Provide(
		&inject.Object{Value: c},
		&inject.Object{Value: lgr},
		&inject.Object{Value: upstreamClient, Name: "upstreamClient"},
		&inject.Object{Value: &transmit.DefaultTransmission{}},
		&inject.Object{Value: shrdr},
		&inject.Object{Value: collector},
		&inject.Object{Value: sampler},
		&inject.Object{Value: metricsr},
		&inject.Object{Value: version, Name: "version"},

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

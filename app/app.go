package app

import (
	"context"
	"fmt"
	"net/url"
	"os"

	"github.com/honeycombio/refinery/agent"
	"github.com/honeycombio/refinery/collect"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/tlsconfig"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/route"
	"github.com/honeycombio/refinery/types"
)

type App struct {
	Config         config.Config     `inject:""`
	Logger         logger.Logger     `inject:""`
	IncomingRouter route.Router      `inject:"inline"`
	PeerRouter     route.Router      `inject:"inline"`
	Collector      collect.Collector `inject:""`
	Metrics        metrics.Metrics   `inject:"metrics"`

	// Version is the build ID for Refinery so that the running process may answer
	// requests for the version
	Version string

	// opampAgent communicates with the opamp server configured if OpAMP
	// support is enabled via configuration.
	opampAgent *agent.Agent
}

// Start on the App object should block until the proxy is shutting down. After
// Start exits, Stop will be called on all dependencies then on App then the
// program will exit.
func (a *App) Start() error {
	// little helper function to record the current config and rules hashes; we call it in
	// the callback but also at startup
	record_hashes := func(msg string) {
		cfgHash, rulesHash := a.Config.GetHashes()
		if a.Logger != nil {
			a.Logger.Warn().WithFields(map[string]interface{}{
				"configHash": cfgHash,
				"rulesHash":  rulesHash,
			}).Logf("%s", msg)
		}
		cfgMetric := config.ConfigHashMetrics(cfgHash)
		ruleMetric := config.ConfigHashMetrics(rulesHash)
		a.Metrics.Gauge("config_hash", float64(cfgMetric))
		a.Metrics.Gauge("rule_config_hash", float64(ruleMetric))
	}

	a.Logger.Debug().Logf("Starting up App...")
	for _, metric := range configHashMetrics {
		a.Metrics.Register(metric)
	}
	a.IncomingRouter.SetVersion(a.Version)
	a.PeerRouter.SetVersion(a.Version)
	a.IncomingRouter.SetType(types.RouterTypeIncoming)
	a.PeerRouter.SetType(types.RouterTypePeer)

	record_hashes("loaded configuration at startup")
	a.logUpstreamTLS()
	a.Config.RegisterReloadCallback(func(configHash, rulesHash string) {
		record_hashes("configuration change was detected and the configuration was reloaded.")
	})

	// launch our main routers to listen for incoming event traffic from both peers
	// and external sources
	a.IncomingRouter.LnS()
	a.PeerRouter.LnS()

	// only enable the opamp agent if it's configured
	if a.Config.GetOpAMPConfig().Enabled {
		a.opampAgent = agent.NewAgent(agent.Logger{Logger: a.Logger}, a.Version, a.Config, a.Metrics, a.IncomingRouter.Health)
	}

	return nil
}

func (a *App) Stop() error {
	a.Logger.Debug().Logf("Shutting down App...")
	if a.Config.GetOpAMPConfig().Enabled {
		a.opampAgent.Stop(context.Background())
	}
	return nil
}

var configHashMetrics = []metrics.Metadata{
	metrics.Metadata{
		Name:        "config_hash",
		Type:        metrics.Gauge,
		Unit:        metrics.Dimensionless,
		Description: "The hash of the current configuration",
	},
	metrics.Metadata{
		Name:        "rule_config_hash",
		Type:        metrics.Gauge,
		Unit:        metrics.Dimensionless,
		Description: "The hash of the current rules configuration",
	},
}

// logUpstreamTLS logs the upstream TLS settings when they're configured, and
// warns when a private CA would stop Honeycomb logger output from reaching a
// different host than HoneycombAPI.
func (a *App) logUpstreamTLS() {
	upstreamTLS := a.Config.GetUpstreamTLSConfig()
	if upstreamTLS == (tlsconfig.Options{}) {
		return
	}
	a.Logger.Warn().WithFields(map[string]interface{}{
		"cert_file": upstreamTLS.CertFile,
		"key_file":  upstreamTLS.KeyFile,
		"ca_file":   upstreamTLS.CAFile,
	}).Logf("loaded upstream TLS configuration at startup")

	if upstreamTLS.CAFile == "" || a.Config.GetLoggerType() != "honeycomb" {
		return
	}
	loggerHost := a.Config.GetHoneycombLoggerConfig().APIHost
	loggerURL, err1 := url.Parse(loggerHost)
	apiURL, err2 := url.Parse(a.Config.GetHoneycombAPI())
	if err1 != nil || err2 != nil || loggerURL.Scheme != "https" || loggerURL.Host == apiURL.Host {
		return
	}
	msg := "HoneycombLogger.APIHost is not Network.HoneycombAPI; Honeycomb logger output is verified against UpstreamTLSCAFile and may fail to send"
	a.Logger.Warn().WithFields(map[string]interface{}{
		"ca_file":                  upstreamTLS.CAFile,
		"honeycomb_logger_apihost": loggerHost,
		"honeycomb_api":            a.Config.GetHoneycombAPI(),
	}).Logf("%s", msg)
	// also print it, since the Honeycomb logger may be unable to deliver it
	fmt.Fprintf(os.Stderr, "WARNING: %s (HoneycombLogger.APIHost=%s, Network.HoneycombAPI=%s)\n", msg, loggerHost, a.Config.GetHoneycombAPI())
}

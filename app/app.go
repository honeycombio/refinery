package app

import (
	"github.com/honeycombio/refinery/collect"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/route"
)

type App struct {
	Config         config.Config     `inject:""`
	Logger         logger.Logger     `inject:""`
	IncomingRouter route.Router      `inject:"inline"`
	PeerRouter     route.Router      `inject:"inline"`
	Collector      collect.Collector `inject:""`
	Metrics        metrics.Metrics   `inject:"genericMetrics"`

	// Version is the build ID for Refinery so that the running process may answer
	// requests for the version
	Version string
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
			}).Logf(msg)
		}
		cfgMetric := config.ConfigHashMetrics(cfgHash)
		ruleMetric := config.ConfigHashMetrics(rulesHash)
		a.Metrics.Gauge("config_hash", cfgMetric)
		a.Metrics.Gauge("rule_config_hash", ruleMetric)
	}

	a.Logger.Debug().Logf("Starting up App...")
	a.Metrics.Register("config_hash", "gauge")
	a.Metrics.Register("rule_config_hash", "gauge")
	a.IncomingRouter.SetVersion(a.Version)
	a.PeerRouter.SetVersion(a.Version)

	record_hashes("loaded configuration at startup")
	a.Config.RegisterReloadCallback(func(configHash, rulesHash string) {
		record_hashes("configuration change was detected and the configuration was reloaded.")
	})

	// launch our main routers to listen for incoming event traffic from both peers
	// and external sources
	a.IncomingRouter.LnS("incoming")
	a.PeerRouter.LnS("peer")

	return nil
}

func (a *App) Stop() error {
	a.Logger.Debug().Logf("Shutting down App...")
	return nil
}

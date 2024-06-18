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
	a.Logger.Debug().Logf("Starting up App...")
	a.Metrics.Register("config_hash", "gauge")
	a.Metrics.Register("rule_config_hash", "gauge")

	a.IncomingRouter.SetVersion(a.Version)
	a.PeerRouter.SetVersion(a.Version)

	a.Config.RegisterReloadCallback(func(configHash, rulesHash string) {
		if a.Logger != nil {
			a.Logger.Warn().WithFields(map[string]interface{}{
				"configHash": configHash,
				"rulesHash":  rulesHash,
			}).Logf("configuration change was detected and the configuration was reloaded.")

			cfgMetric, err := config.ConfigHashMetrics(configHash)
			if err != nil {
				a.Logger.Error().Logf("error calculating config hash metrics: %s", err)
			} else {
				a.Metrics.Gauge("config_hash", cfgMetric)
			}

			ruleMetric, err := config.ConfigHashMetrics(rulesHash)
			if err != nil {
				a.Logger.Error().Logf("error calculating config hash metrics: %s", err)
			} else {
				a.Metrics.Gauge("rule_config_hash", ruleMetric)
			}

		}

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

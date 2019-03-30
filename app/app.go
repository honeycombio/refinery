package app

import (
	"os"
	"os/signal"
	"syscall"

	libhoney "github.com/honeycombio/libhoney-go"
	"github.com/honeycombio/samproxy/collect"
	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/metrics"
	"github.com/honeycombio/samproxy/route"
	"github.com/pkg/errors"
)

type App struct {
	Config    config.Config     `inject:""`
	Logger    logger.Logger     `inject:""`
	Router    route.Router      `inject:"inline"`
	Collector collect.Collector `inject:""`
	Metrics   metrics.Metrics   `inject:""`
}

// Start on the App obect should block until the proxy is shutting down. After
// Start exits, Stop will be called on all dependencies then on App then the
// program will exit.
func (a *App) Start() error {
	a.Logger.Debugf("Starting up App...")

	// set up signal channel to exit
	sigsToExit := make(chan os.Signal, 1)
	signal.Notify(sigsToExit, syscall.SIGINT, syscall.SIGTERM)

	// set up signal channel to reload configs on USR1
	sigsToReload := make(chan os.Signal, 1)
	signal.Notify(sigsToReload, syscall.SIGUSR1)
	go a.listenForReload(sigsToReload)

	// Validate configuration
	err := a.validateAPIKeys()
	if err != nil {
		a.Logger.WithField("error", err).Errorf("Failed to validate API key")
		return err
	}

	// launch our main router to listen for incoming event traffic
	go a.Router.LnS("incoming")
	go a.Router.LnS("peer")

	// block on our signal handler to exit
	sig := <-sigsToExit
	a.Logger.Errorf("Caught signal \"%s\"", sig)

	return nil
}

func (a *App) listenForReload(sigs chan os.Signal) {
	for {
		// block on our signal handler to exit
		sig := <-sigs
		a.Logger.Debugf("Caught signal \"%s\"; reloading configs", sig)
		a.Config.ReloadConfig()
	}
}

func (a *App) Stop() error {
	a.Logger.Debugf("Shutting down App...")
	return nil
}

func (a *App) validateAPIKeys() error {
	keys, err := a.Config.GetAPIKeys()
	if err != nil {
		return err
	}
	// if we have the key '*' anywhere in the list, consider all keys valid
	for _, key := range keys {
		if key == "*" {
			return nil
		}
	}
	apiHost, _ := a.Config.GetHoneycombAPI()
	// ok, go ahead and actually validate keys
	for _, key := range keys {
		libhConfig := libhoney.Config{WriteKey: key, APIHost: apiHost}
		team, err := libhoney.VerifyWriteKey(libhConfig)
		if err != nil {
			return errors.Wrapf(err, "failed to validate API key: %s", key)
		}
		a.Logger.WithField("api_key", key).WithField("team", team).Debugf("validated API key")
	}
	return nil
}

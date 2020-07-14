package app

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/honeycombio/samproxy/collect"
	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/metrics"
	"github.com/honeycombio/samproxy/route"
)

type App struct {
	Config         config.Config     `inject:""`
	Logger         logger.Logger     `inject:""`
	IncomingRouter route.Router      `inject:"inline"`
	PeerRouter     route.Router      `inject:"inline"`
	Collector      collect.Collector `inject:""`
	Metrics        metrics.Metrics   `inject:""`

	// Version is the build ID for samproxy so that the running process may answer
	// requests for the version
	Version string
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

	a.IncomingRouter.SetVersion(a.Version)
	a.PeerRouter.SetVersion(a.Version)

	// launch our main routers to listen for incoming event traffic from both peers
	// and external sources
	go a.IncomingRouter.LnS("incoming")
	go a.PeerRouter.LnS("peer")

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

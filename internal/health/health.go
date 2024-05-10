package health

import (
	"sync"
	"time"

	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/jonboulle/clockwork"
)

// We need a Health object that can be used by:
// - internal services to tell it their readiness to receive traffic
// - the router to read back that data for reporting when it receives a health or readiness request
//   either on grpc or on http
// We want that object in its own package so we don't have import cycles

// We register a service with an expected interval for reporting and if it
// doesn't report at all within that interval, we will mark it (and the whole system)
// as unhealthy (not alive). If a system reports not ready, we will mark it as not ready
// and the system as a whole as not ready.

// Services will typically Register during their startup, and then call Ready
// frequently when they are ready to receive traffic. Note that Registration
// does not start the ticker -- it only starts once Ready is called for the
// first time.

// Recorder is the interface used by object that want to record their own health
// status and make it available to the system.
type Recorder interface {
	Register(source string, timeout time.Duration)
	Ready(source string, ready bool)
}

// Reporter is the interface that is used to read back the health status of the system.
type Reporter interface {
	IsAlive() bool
	IsReady() bool
}

// TickerTime is the interval at which we will check the health of the system.
// We will decrement the counters for each service that has registered.
// If a counter reaches 0, we will mark the service as dead.
// This value should be less than the duration of any reporting timeout in the system.
var TickerTime = 100 * time.Millisecond

// The Health object is the main object that services will interact with.
// When services are registered, they will be expected to report in at least once every timeout interval.
// If they don't, they will be marked as not alive.
type Health struct {
	Clock    clockwork.Clock `inject:""`
	Metrics  metrics.Metrics `inject:"genericMetrics"`
	Logger   logger.Logger   `inject:""`
	timeouts map[string]time.Duration
	timeLeft map[string]time.Duration
	readies  map[string]bool
	mut      sync.RWMutex
	done     chan struct{}
	startstop.Starter
	startstop.Stopper
	Recorder
	Reporter
}

func (h *Health) Start() error {
	h.timeouts = make(map[string]time.Duration)
	h.timeLeft = make(map[string]time.Duration)
	h.readies = make(map[string]bool)
	h.done = make(chan struct{})
	go h.ticker()
	return nil
}

func (h *Health) Stop() error {
	close(h.done)
	return nil
}

func (h *Health) ticker() {
	tick := h.Clock.NewTicker(TickerTime)
	for {
		select {
		case <-tick.Chan():
			h.mut.Lock()
			for source, timeLeft := range h.timeLeft {
				// only decrement positive counters since 0 means we're dead
				if timeLeft > 0 {
					h.timeLeft[source] -= TickerTime
					if h.timeLeft[source] < 0 {
						h.timeLeft[source] = 0
					}
				}
			}
			h.mut.Unlock()
		case <-h.done:
			return
		}
	}
}

// Register a service with the health system. The timeout is the maximum
// expected interval between service reports. If Ready is not called within that
// interval (beginning from the time of calling Ready for the first time), it
// (and the entire server) will be marked as not alive.
func (h *Health) Register(source string, timeout time.Duration) {
	h.mut.Lock()
	defer h.mut.Unlock()
	h.timeouts[source] = timeout
	h.readies[source] = false
	// we use a negative value to indicate that we haven't seen a report yet so
	// we don't return "dead" immediately
	h.timeLeft[source] = -1
	fields := map[string]any{
		"source":  source,
		"timeout": timeout,
	}
	h.Logger.Debug().WithFields(fields).Logf("Registered Health ticker", source, timeout)
	if timeout < TickerTime {
		h.Logger.Error().WithFields(fields).Logf("Registering a timeout less than the ticker time")
	}
}

// Ready is called by services to indicate their readiness to receive traffic.
// If any service is not ready, the system as a whole is not ready.
// Even unready services will be marked as alive as long as they report in.
func (h *Health) Ready(source string, ready bool) {
	h.mut.Lock()
	defer h.mut.Unlock()
	if _, ok := h.timeouts[source]; !ok {
		h.Logger.Error().WithField("source", source).Logf("Health.Ready called for unregistered source")
		return
	}
	if h.readies[source] != ready {
		h.Logger.Info().WithFields(map[string]any{
			"source": source,
			"ready":  ready,
		}).Logf("Health.Ready reporting source changing state")
	}
	h.readies[source] = ready
	h.timeLeft[source] = h.timeouts[source]
	h.Metrics.Gauge("is_ready", h.checkReady())
	h.Metrics.Gauge("is_alive", h.checkAlive())
}

// IsAlive returns true if all registered services are alive
func (h *Health) IsAlive() bool {
	h.mut.RLock()
	defer h.mut.RUnlock()
	return h.checkAlive()
}

// checkAlive returns true if all registered services are alive
// only call with the lock held
func (h *Health) checkAlive() bool {
	// if any counter is 0, we're dead
	for source, a := range h.timeLeft {
		if a == 0 {
			h.Logger.Error().WithField("source", source).Logf("IsAlive: source dead due to timeout")
			return false
		}
	}
	return true
}

// IsReady returns true if all registered services are ready
func (h *Health) IsReady() bool {
	h.mut.RLock()
	defer h.mut.RUnlock()
	return h.checkReady()
}

// checkReady returns true if all registered services are ready
// only call with the lock held
func (h *Health) checkReady() bool {
	// if no one has registered yet, we're not ready
	if len(h.readies) == 0 {
		h.Logger.Debug().Logf("IsReady: no one has registered yet")
		return false
	}

	// if any counter is not positive, we're not ready
	for source, counter := range h.timeLeft {
		if counter <= 0 {
			h.Logger.Info().WithFields(map[string]any{
				"source":  source,
				"counter": counter,
			}).Logf("Health.IsReady failed due to counter <= 0")
			return false
		}
	}

	// if any registered service is not ready, we're not ready
	ready := true
	for source, r := range h.readies {
		if !r {
			h.Logger.Info().WithFields(map[string]any{
				"source": source,
				"ready":  ready,
			}).Logf("Health.IsReady reporting source not ready")
		}
		ready = ready && r
	}
	return ready
}

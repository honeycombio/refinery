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
// - internal subsystems to tell it their readiness to receive traffic
// - the router to read back that data for reporting when it receives a health or readiness request
//   either on grpc or on http
// We want that object in its own package so we don't have import cycles

// We register a subsystem with an expected interval for reporting and if it
// doesn't report for a time exceeding the duration of that interval, we will
// mark it (and the whole application) as unhealthy (not alive). A subsystem can
// also report that it is alive but not ready; when this happens, we will mark
// it as not ready and the system as a whole as not ready but still alive. This
// is useful during shutdown.

// Subsystems will typically Register during their startup, and then call Ready
// frequently once they are ready to receive traffic. Note that Registration
// does not start the ticker -- it only starts once Ready is called for the
// first time.

// Recorder is the interface used by object that want to record their own health
// status and make it available to the system.
type Recorder interface {
	Register(subsystem string, timeout time.Duration)
	Unregister(subsystem string)
	Ready(subsystem string, ready bool)
}

// Reporter is the interface that is used to read back the health status of the system.
type Reporter interface {
	IsAlive() bool
	IsReady() bool
}

// TickerTime is the interval at which we will survey health of all of the
// subsystems. We will decrement the counters for each subsystem that has
// registered. If a counter reaches 0, we will mark the subsystem as dead. This
// value should generally be less than the duration of any reporting timeout in
// the system.
var TickerTime = 500 * time.Millisecond

// The Health object is the main object that subsystems will interact with. When
// subsystems are registered, they will be expected to report in at least once
// every timeout interval. If they don't, they will be marked as not alive.
type Health struct {
	Clock    clockwork.Clock `inject:""`
	Metrics  metrics.Metrics `inject:"genericMetrics"`
	Logger   logger.Logger   `inject:""`
	timeouts map[string]time.Duration
	timeLeft map[string]time.Duration
	readies  map[string]bool
	alives   map[string]bool
	mut      sync.RWMutex
	done     chan struct{}
	startstop.Starter
	startstop.Stopper
	Recorder
	Reporter
}

func (h *Health) Start() error {
	// if we don't have a logger or metrics object, we'll use the null ones (makes testing easier)
	if h.Logger == nil {
		h.Logger = &logger.NullLogger{}
	}
	if h.Metrics == nil {
		h.Metrics = &metrics.NullMetrics{}
	}
	h.timeouts = make(map[string]time.Duration)
	h.timeLeft = make(map[string]time.Duration)
	h.readies = make(map[string]bool)
	h.alives = make(map[string]bool)
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
			for subsystem, timeLeft := range h.timeLeft {
				// only decrement positive counters since 0 means we're dead
				if timeLeft > 0 {
					h.timeLeft[subsystem] -= TickerTime
					if h.timeLeft[subsystem] < 0 {
						h.timeLeft[subsystem] = 0
					}
				}
			}
			h.mut.Unlock()
		case <-h.done:
			return
		}
	}
}

// Register a subsystem with the health system. The timeout is the maximum
// expected interval between subsystem reports. If Ready is not called within
// that interval (beginning from the time of calling Ready for the first time),
// it (and the entire server) will be marked as not alive.
func (h *Health) Register(subsystem string, timeout time.Duration) {
	h.mut.Lock()
	defer h.mut.Unlock()
	h.timeouts[subsystem] = timeout
	h.readies[subsystem] = false
	// we use a negative value to indicate that we haven't seen a report yet so
	// we don't return "dead" immediately
	h.timeLeft[subsystem] = -1
	fields := map[string]any{
		"source":  subsystem,
		"timeout": timeout,
	}
	h.Logger.Debug().WithFields(fields).Logf("Registered Health ticker", subsystem, timeout)
	if timeout < TickerTime {
		h.Logger.Error().WithFields(fields).Logf("Registering a timeout less than the ticker time")
	}
}

// Unregister a subsystem with the health system. This marks the subsystem as not
// ready and removes it from the alive tracking. It also means that it no longer
// needs to report in. If it does report in, the report will be ignored.
func (h *Health) Unregister(subsystem string) {
	h.mut.Lock()
	defer h.mut.Unlock()
	delete(h.timeouts, subsystem)
	delete(h.timeLeft, subsystem)
	delete(h.alives, subsystem)

	// we don't remove it from readies, but we mark it as not ready;
	// an unregistered subsystem can never be ready.
	h.readies[subsystem] = false
}

// Ready is called by subsystems with a flag to indicate their readiness to
// receive traffic. If any subsystem is not ready, the system as a whole is not
// ready. Even unready subsystems will be marked as alive as long as they report
// in.
func (h *Health) Ready(subsystem string, ready bool) {
	h.mut.Lock()
	defer h.mut.Unlock()
	if _, ok := h.timeouts[subsystem]; !ok {
		// if a subsystem has an entry in readies but not in timeouts, it means
		// it had called Unregister but is still reporting in. This is not an error.
		if _, ok := h.readies[subsystem]; !ok {
			// but if it was never registered, it IS an error
			h.Logger.Error().WithField("subsystem", subsystem).Logf("Health.Ready called for unregistered subsystem")
		}
		return
	}
	if h.readies[subsystem] != ready {
		h.Logger.Info().WithFields(map[string]any{
			"subsystem": subsystem,
			"ready":     ready,
		}).Logf("Health.Ready reporting subsystem changing state")
	}
	h.readies[subsystem] = ready
	h.timeLeft[subsystem] = h.timeouts[subsystem]
	if !h.alives[subsystem] {
		h.alives[subsystem] = true
		h.Logger.Info().WithField("subsystem", subsystem).Logf("Health.Ready reporting subsystem alive")
	}
	h.Metrics.Gauge("is_ready", h.checkReady())
	h.Metrics.Gauge("is_alive", h.checkAlive())
}

// IsAlive returns true if all registered subsystems are alive
func (h *Health) IsAlive() bool {
	h.mut.Lock()
	defer h.mut.Unlock()
	return h.checkAlive()
}

// checkAlive returns true if all registered subsystems are alive
// only call with a write lock held
func (h *Health) checkAlive() bool {
	// if any counter is 0, we're dead
	for subsystem, a := range h.timeLeft {
		if a == 0 {
			if h.alives[subsystem] {
				h.Logger.Error().WithField("subsystem", subsystem).Logf("IsAlive: subsystem dead due to timeout")
				h.alives[subsystem] = false
			}
			return false
		}
	}
	return true
}

// IsReady returns true if all registered subsystems are ready
func (h *Health) IsReady() bool {
	h.mut.RLock()
	defer h.mut.RUnlock()
	return h.checkReady()
}

// checkReady returns true if all registered subsystems are ready
// only call with the lock held
func (h *Health) checkReady() bool {
	// if no one has registered yet, we're not ready
	if len(h.readies) == 0 {
		h.Logger.Debug().Logf("IsReady: no one has registered yet")
		return false
	}

	// if any counter is not positive, we're not ready
	for subsystem, counter := range h.timeLeft {
		if counter <= 0 {
			h.Logger.Info().WithFields(map[string]any{
				"subsystem": subsystem,
				"counter":   counter,
			}).Logf("Health.IsReady failed due to counter <= 0")
			return false
		}
	}

	// if any registered subsystem is not ready, we're not ready
	ready := true
	for subsystem, r := range h.readies {
		if !r {
			h.Logger.Info().WithFields(map[string]any{
				"subsystem": subsystem,
				"ready":     ready,
			}).Logf("Health.IsReady reporting subsystem not ready")
		}
		ready = ready && r
	}
	return ready
}

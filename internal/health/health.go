package health

import (
	"fmt"
	"sync"
	"time"

	"github.com/facebookgo/startstop"
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
type HealthRecorder interface {
	Register(source string, timeout time.Duration)
	Ready(source string, ready bool)
}

// Reporter is the interface that is used to read back the health status of the system.
type Reporter interface {
	Report() (alive bool, ready bool)
}

// TickerTime is the interval at which we will check the health of the system.
var TickerTime = 1 * time.Second

// The Health object is the main object that services will interact with.
// When services are registered, they will be expected to report in at least once every timeout interval.
// If they don't, they will be marked as not alive.
type Health struct {
	Clock    clockwork.Clock
	timeouts map[string]time.Duration
	timeLeft map[string]time.Duration
	readies  map[string]bool
	mut      sync.RWMutex
	done     chan struct{}
	startstop.Starter
	startstop.Stopper
	HealthRecorder
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
	fmt.Println("starting ticker")
	tick := h.Clock.NewTicker(TickerTime)
	for {
		select {
		case <-tick.Chan():
			fmt.Println("got a tick")
			h.mut.Lock()
			for source, timeLeft := range h.timeLeft {
				// only decrement positive counters since 0 means we're dead
				if timeLeft > 0 {
					fmt.Println("Decrementing timeout for source", source)
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
}

// Ready is called by services to indicate their readiness to receive traffic.
// If any service is not ready, the system as a whole is not ready.
// Even unready services will be marked as alive as long as they report in.
func (h *Health) Ready(source string, ready bool) {
	h.mut.Lock()
	defer h.mut.Unlock()
	h.readies[source] = ready
	h.timeLeft[source] = h.timeouts[source]
}

// Report returns the current health status of the system as a pair of booleans.
// Alive is true at startup; once all services have reported in at least once, it
// is true only if all services have reported within their timeout interval.
func (h *Health) Report() (alive bool, ready bool) {
	h.mut.RLock()
	defer h.mut.RUnlock()
	// if any counter is 0, we're dead
	alive = true
	for name, a := range h.timeLeft {
		fmt.Printf("%s=%d\n", name, a)
		alive = alive && a != 0
	}
	if !alive {
		// can't be ready if we're not alive
		return false, false
	}
	for name, r := range h.readies {
		// can't be ready if any service has not reported yet
		if h.timeLeft[name] < 0 {
			return true, false
		}
		ready = ready || r
	}
	return alive, ready
}

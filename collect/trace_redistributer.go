package collect

import (
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/jonboulle/clockwork"
)

type redistributeNotifier struct {
	clock        clockwork.Clock
	logger       logger.Logger
	initialDelay time.Duration
	maxAttempts  int
	maxDelay     time.Duration
	metrics      metrics.Metrics

	reset     chan struct{}
	done      chan struct{}
	triggered chan struct{}
	once      sync.Once
}

func newRedistributeNotifier(logger logger.Logger, met metrics.Metrics, clock clockwork.Clock) *redistributeNotifier {
	r := &redistributeNotifier{
		initialDelay: 3 * time.Second,
		maxDelay:     30 * time.Second,
		maxAttempts:  5,
		done:         make(chan struct{}),
		clock:        clock,
		logger:       logger,
		metrics:      met,
		triggered:    make(chan struct{}),
		reset:        make(chan struct{}),
	}

	return r
}

func (r *redistributeNotifier) Notify() <-chan struct{} {
	return r.triggered
}

func (r *redistributeNotifier) Reset() {
	var started bool
	r.once.Do(func() {
		go r.run()
		started = true
	})

	if started {
		return
	}

	select {
	case r.reset <- struct{}{}:
	case <-r.done:
		return
	default:
		r.logger.Debug().Logf("A trace redistribution is ongoing. Ignoring reset.")
	}
}

func (r *redistributeNotifier) Stop() {
	close(r.done)
}

// run runs the redistribution notifier loop.
// It will notify the trigger channel when it's time to redistribute traces.
// A notification will be sent every time the backoff timer expires.
// The backoff timer is reset when a reset signal is received.
func (r *redistributeNotifier) run() {
	var attempts int
	currentBackOff := r.initialDelay
	lastBackoff := currentBackOff

	// start a back off timer with the initial delay
	timer := r.clock.NewTimer(currentBackOff)
	for {

		// only reset the timer if we have received
		// a reset signal or we are in the middle of
		// a redistribution cycle.
		if currentBackOff != lastBackoff {
			if !timer.Stop() {
				// drain the timer channel
				select {
				case <-timer.Chan():
				default:
				}
			}
			timer.Reset(currentBackOff)
			lastBackoff = currentBackOff
		}

		select {
		case <-r.done:
			timer.Stop()
			return
		case <-r.reset:
			// reset the backoff timer and attempts
			// if we receive a reset signal.
			currentBackOff = r.initialDelay
			attempts = 0
		case <-timer.Chan():
			if attempts >= r.maxAttempts {
				// if we've reached the max attempts,
				// we will block the goroutine here until
				// we receive a reset signal or refinery starts to shutdown.
				r.metrics.Gauge("trace_redistribution_count", 0)
				select {
				case <-r.done:
					return
				case <-r.reset:
				}
				currentBackOff = r.initialDelay
				attempts = 0
				currentBackOff = r.calculateBackoff(currentBackOff)
				continue
			}

			select {
			case <-r.done:
				return
			case r.triggered <- struct{}{}:
			}

			attempts++
			r.metrics.Gauge("trace_redistribution_count", attempts)
			currentBackOff = r.calculateBackoff(currentBackOff)
		}
	}
}

// calculateBackoff calculates the backoff interval for the next redistribution cycle.
// It uses exponential backoff with a base time and adds jitter to avoid retry collisions.
func (r *redistributeNotifier) calculateBackoff(lastBackoff time.Duration) time.Duration {
	// Calculate the backoff interval using exponential backoff with a base time.
	backoff := time.Duration(math.Min(float64(lastBackoff)*2, float64(r.maxDelay)))
	// Add jitter to the backoff to avoid retry collisions.
	jitter := time.Duration(rand.Float64() * float64(backoff) * 0.5)
	return backoff + jitter
}

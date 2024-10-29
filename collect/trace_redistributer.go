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

func (r *redistributeNotifier) run() {
	var attempts int
	currentBackOff := r.initialDelay
	lastBackoff := currentBackOff

	timer := r.clock.NewTimer(currentBackOff)
	for {

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
			currentBackOff = r.initialDelay
			attempts = 0
		case <-timer.Chan():
			// if we've reached the max attempts,
			if attempts >= r.maxAttempts {
				r.metrics.Gauge("trace_redistribution_count", 0)
				select {
				case <-r.done:
				case <-r.reset:
				}
				currentBackOff = r.initialDelay
				attempts = 0
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

func (r *redistributeNotifier) calculateBackoff(lastBackoff time.Duration) time.Duration {
	// Calculate the backoff interval using exponential backoff with a base time.
	backoff := time.Duration(math.Min(float64(lastBackoff)*2, float64(r.maxDelay)))
	// Add jitter to the backoff to avoid retry collisions.
	jitter := time.Duration(rand.Float64() * float64(backoff) * 0.5)
	return backoff + jitter
}

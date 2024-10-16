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
	lastBackoff := r.initialDelay
	for {
		// if we've reached the max attempts, reset the backoff and attempts
		// only when the reset signal is received.
		if attempts >= r.maxAttempts {
			r.metrics.Gauge("trace_redistribution_count", 0)
			<-r.reset
			lastBackoff = r.initialDelay
			attempts = 0
		}
		select {
		case <-r.done:
			return
		case r.triggered <- struct{}{}:
		}

		attempts++
		r.metrics.Gauge("trace_redistribution_count", attempts)

		// Calculate the backoff interval using exponential backoff with a base time.
		backoff := time.Duration(math.Min(float64(lastBackoff)*2, float64(r.maxDelay)))
		// Add jitter to the backoff to avoid retry collisions.
		jitter := time.Duration(rand.Float64() * float64(backoff) * 0.5)
		nextBackoff := backoff + jitter
		lastBackoff = nextBackoff

		timer := r.clock.NewTimer(nextBackoff)
		select {
		case <-timer.Chan():
			timer.Stop()
		case <-r.reset:
			lastBackoff = r.initialDelay
			attempts = 0
			timer.Stop()
		case <-r.done:
			timer.Stop()
			return
		}
	}
}

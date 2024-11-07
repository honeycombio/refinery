package collect

import (
	"math/rand/v2"
	"sync"
	"time"

	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/jonboulle/clockwork"
)

type redistributeNotifier struct {
	clock   clockwork.Clock
	logger  logger.Logger
	delay   time.Duration
	metrics metrics.Metrics

	reset     chan struct{}
	done      chan struct{}
	triggered chan struct{}
	once      sync.Once
}

func newRedistributeNotifier(logger logger.Logger, met metrics.Metrics, clock clockwork.Clock, delay time.Duration) *redistributeNotifier {
	r := &redistributeNotifier{
		delay:     delay,
		done:      make(chan struct{}),
		clock:     clock,
		logger:    logger,
		metrics:   met,
		triggered: make(chan struct{}),
		reset:     make(chan struct{}),
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
// It will notify the trigger channel when it's time to redistribute traces, which we want
// to happen when the number of peers changes. But we don't want to do it immediately,
// because peer membership changes often happen in bunches, so we wait a while
// before triggering the redistribution.
func (r *redistributeNotifier) run() {
	currentDelay := r.calculateDelay(r.delay)

	// start a back off timer with the initial delay
	timer := r.clock.NewTimer(currentDelay)
	for {
		select {
		case <-r.done:
			timer.Stop()
			return
		case <-r.reset:
			// reset the delay timer when we receive a reset signal.
			currentDelay = r.calculateDelay(r.delay)
			if !timer.Stop() {
				// drain the timer channel
				select {
				case <-timer.Chan():
				default:
				}
			}
			timer.Reset(currentDelay)
		case <-timer.Chan():
			select {
			case <-r.done:
				return
			case r.triggered <- struct{}{}:
			}
		}
	}
}

// calculateBackoff calculates the backoff interval for the next redistribution cycle.
// It uses exponential backoff with a base time and adds jitter to avoid retry collisions.
func (r *redistributeNotifier) calculateDelay(currentDelay time.Duration) time.Duration {
	// Add jitter to the backoff to avoid retry collisions.
	jitter := time.Duration(rand.Float64() * float64(currentDelay) * 0.5)
	return currentDelay + jitter
}

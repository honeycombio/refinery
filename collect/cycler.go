package collect

import (
	"context"
	"time"

	"github.com/jonboulle/clockwork"
)

// Cycle implements a controllable loop that can be paused, resumed, and run once.
// It runs a function at a fixed interval and is stopped by closing the done channel
// passed in by the caller.
type Cycle struct {
	clock    clockwork.Clock
	interval time.Duration
	// shutdown the cycle by closing this channel
	done chan struct{}

	// The following channels are used to control the cycle
	// these should only be used in tests
	runOnce     chan message
	pause       chan struct{}
	continueRun chan struct{}
}

// message is a struct used to signal back to the caller
// that the cycle has finished running once.
type message struct {
	done chan struct{}
}

// NewCycle creates a new Cycle instance.
func NewCycle(clock clockwork.Clock, interval time.Duration, done chan struct{}) *Cycle {
	return &Cycle{
		clock:       clock,
		interval:    interval,
		runOnce:     make(chan message),
		pause:       make(chan struct{}),
		continueRun: make(chan struct{}),
		done:        done,
	}
}

// Run starts the cycle.
// Every interval, the function passed in is called.
func (c *Cycle) Run(ctx context.Context, fn func(ctx context.Context) error) error {
	ticker := c.clock.NewTicker(c.interval)
	defer ticker.Stop()

	if err := fn(ctx); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			return nil

			// run the function once and wait for it to finish
			// after it finishes, close the done channel to signal that it's done
		case wait := <-c.runOnce:
			if err := fn(ctx); err != nil {
				return err
			}

			if wait.done != nil {
				close(wait.done)
			}

			// stop the ticker so that no more cycle iterations are run
		case <-c.pause:
			ticker.Stop()

			// drain the ticker channel to ensure we don't have ticks left
			select {
			case <-ticker.Chan():
			default:
			}
		case <-c.continueRun:
			// start the ticker again
			ticker.Stop()
			ticker = c.clock.NewTicker(c.interval)
		case <-ticker.Chan():
			if err := fn(ctx); err != nil {
				return err
			}
		}
	}
}

// RunOnce runs the function passed in once.
// It only returns after the function has finished running.
func (c *Cycle) RunOnce() {
	done := make(chan struct{})
	select {
	case c.runOnce <- message{done: done}:

	// if the cycle has been stopped, return immediately
	case <-c.done:
		close(done)
		return
	}

	// wait for the function to finish running
	select {
	case <-done:
		return
	// if the cycle has been stopped, return immediately
	case <-c.done:
		return
	}
}

// Pause pauses the cycle.
func (c *Cycle) Pause() {
	c.pause <- struct{}{}
}

// Continue resumes the cycle.
func (c *Cycle) Continue() {
	c.continueRun <- struct{}{}
}

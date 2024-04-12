package collect

import (
	"context"
	"time"

	"github.com/jonboulle/clockwork"
)

type Cycle struct {
	clock       clockwork.Clock
	interval    time.Duration
	runOnce     chan message
	pause       chan struct{}
	continueRun chan struct{}
	done        chan struct{}

	runfinished chan struct{}
}

type message struct {
	done chan struct{}
}

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

func (c *Cycle) Run(ctx context.Context, fn func(ctx context.Context) error) error {
	ticker := c.clock.NewTicker(c.interval)
	defer ticker.Stop()

	if err := fn(ctx); err != nil {
		return err
	}

	for {
		select {
		case <-c.done:
			return nil
		case wait := <-c.runOnce:
			if err := fn(ctx); err != nil {
				return err
			}

			if wait.done != nil {
				close(wait.done)
			}

		case <-c.pause:
			ticker.Stop()

			select {
			case <-ticker.Chan():
			default:
			}
		case <-c.continueRun:
			ticker = c.clock.NewTicker(c.interval)
		case <-ticker.Chan():
			if err := fn(ctx); err != nil {
				return err
			}
		}
	}
}

func (c *Cycle) RunOnce() {
	done := make(chan struct{})
	select {
	case c.runOnce <- message{done: done}:
	case <-c.done:
		close(done)
		return
	}

	select {
	case <-done:
		return
	case <-c.done:
		close(done)
		return
	}
}

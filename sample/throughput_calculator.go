package sample

import (
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
)

// EMAThroughputCalculator encapsulates the logic to calculate a throughput value using an Exponential Moving Average (EMA).
type EMAThroughputCalculator struct {
	Clock clockwork.Clock

	throughputLimit float64
	weight          float64       // Smoothing factor for EMA
	intervalLength  time.Duration // Length of the interval

	mut        sync.RWMutex
	lastEMA    float64 // Previous EMA value
	eventCount int     // Internal count of events in the current interval
	done       chan struct{}
}

// NewEMAThroughputCalculator creates a new instance of EMAThroughputCalculator.
func NewEMAThroughputCalculator(clock clockwork.Clock, weight float64, intervalLength time.Duration, throughputLimit int) *EMAThroughputCalculator {
	c := &EMAThroughputCalculator{
		weight:          weight,
		intervalLength:  intervalLength,
		throughputLimit: float64(throughputLimit),
		lastEMA:         0,
		Clock:           clock,
		done:            make(chan struct{}),
	}

	go func() {
		ticker := c.Clock.NewTicker(c.intervalLength)
		for {
			select {
			case <-c.done:
				return
			case <-ticker.Chan():
				c.updateEMA()
			}
		}

	}()

	return c
}

func (c *EMAThroughputCalculator) Stop() {
	close(c.done)
}

// IncrementEventCount increments the internal event count by a specified amount.
func (c *EMAThroughputCalculator) IncrementEventCount(count int) {
	c.mut.Lock()
	c.eventCount += count
	c.mut.Unlock()
}

// updateEMA calculates the current throughput and updates the EMA.
func (c *EMAThroughputCalculator) updateEMA() {
	c.mut.Lock()

	currentThroughput := float64(c.eventCount) / c.intervalLength.Seconds()
	c.lastEMA = c.weight*currentThroughput + (1-c.weight)*c.lastEMA
	c.eventCount = 0 // Reset the event count for the new interval

	c.mut.Unlock()
}

// GetSamplingRateMultiplier calculates and returns a sampling rate multiplier
// based on the difference between the configured throughput limit and the current throughput.
func (c *EMAThroughputCalculator) GetSamplingRateMultiplier() float64 {
	if c.throughputLimit == 0 {
		return 1.0 // No limit set, so no adjustment needed
	}

	c.mut.RLock()
	currentEMA := c.lastEMA
	c.mut.RUnlock()

	if currentEMA <= c.throughputLimit {
		return 1.0 // Throughput is within the limit, no adjustment needed
	}

	return 1 + (currentEMA-c.throughputLimit)*0.1
}

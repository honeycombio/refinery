package collect

import (
	"testing"
	"time"

	"github.com/honeycombio/refinery/metrics"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
)

// TestRedistributeNotifier tests the timer logic in redistributeNotifier
func TestRedistributeNotifier(t *testing.T) {
	// Set up the notifier with a mock clock

	clock := clockwork.NewFakeClock()
	r := &redistributeNotifier{
		clock:     clock,
		delay:     50 * time.Millisecond, // Set the initial delay
		metrics:   &metrics.NullMetrics{},
		reset:     make(chan struct{}),
		done:      make(chan struct{}),
		triggered: make(chan struct{}, 4), // Buffered to allow easier testing
	}

	defer r.Stop()

	go r.run()

	assert.Len(t, r.triggered, 0)
	// Test that the notifier is not triggered before the initial delay
	clock.BlockUntil(1)
	clock.Advance(20 * time.Millisecond)
	assert.Len(t, r.triggered, 0)

	// Test that the notifier is triggered after the initial delay
	currentBackOff := r.delay
	clock.BlockUntil(1)
	currentBackOff = r.calculateDelay(currentBackOff)
	clock.Advance(currentBackOff + 100*time.Millisecond) // Advance the clock by the backoff time plus a little extra

	// Check that the notifier has been triggered
	assert.Eventually(t, func() bool {
		return len(r.triggered) == 1
	}, 200*time.Millisecond, 10*time.Millisecond, "Expected to be triggered %d times", 1)

	// Once we receive another reset signal, the timer should start again
	r.Reset()
	clock.BlockUntil(1)
	clock.Advance(500 * time.Millisecond)
	assert.Eventually(t, func() bool {
		return len(r.triggered) == 2
	}, 200*time.Millisecond, 10*time.Millisecond, "Expected to be triggered 4 times")

}

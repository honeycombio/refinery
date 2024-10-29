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
		clock:        clock,
		initialDelay: 50 * time.Millisecond, // Set the initial delay
		maxAttempts:  3,
		metrics:      &metrics.NullMetrics{},
		maxDelay:     200 * time.Millisecond,
		reset:        make(chan struct{}),
		done:         make(chan struct{}),
		triggered:    make(chan struct{}, 4), // Buffered to allow easier testing
	}

	defer r.Stop()

	go r.run()

	// Test that the notifier is not triggered before the initial delay
	clock.BlockUntil(1)
	clock.Advance(20 * time.Millisecond)

	assert.Eventually(t, func() bool {
		return len(r.triggered) == 0
	}, 200*time.Millisecond, 10*time.Millisecond)

	// Test that the notifier is triggered after the initial delay
	currentBackOff := r.initialDelay
	for i := 0; i < r.maxAttempts; i++ {
		clock.BlockUntil(1)
		currentBackOff = r.calculateBackoff(currentBackOff)
		clock.Advance(currentBackOff + 100*time.Millisecond) // Advance the clock by the backoff time plus a little extra

		// Check that the notifier has been triggered
		assert.Eventually(t, func() bool {
			return len(r.triggered) == i+1
		}, 200*time.Millisecond, 10*time.Millisecond, "Expected to be triggered %d times", i+1)
	}

	// Make sure once we hit maxAttempts, we stop trying to trigger
	clock.BlockUntil(1)
	clock.Advance(500 * time.Millisecond)
	assert.Eventually(t, func() bool {
		return len(r.triggered) == r.maxAttempts
	}, 200*time.Millisecond, 10*time.Millisecond, "Expected to be triggered 3 times")

	// Once we receive another reset signal, the timer should start again
	r.Reset()
	clock.BlockUntil(1)
	clock.Advance(500 * time.Millisecond)
	assert.Eventually(t, func() bool {
		return len(r.triggered) == r.maxAttempts+1
	}, 200*time.Millisecond, 10*time.Millisecond, "Expected to be triggered 4 times")

}

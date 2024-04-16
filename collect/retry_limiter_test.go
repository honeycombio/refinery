package collect

import (
	"errors"
	"sync"
	"testing"
)

func TestRetryLimiter(t *testing.T) {
	// Create a new limiter with a limit of 3.
	limiter := newLimiter("test", 3)

	// Create a new wait group.
	var wg sync.WaitGroup

	// Add 3 to the wait group.
	wg.Add(3)

	limiter.Go(func() error {
		defer wg.Done()
		return errors.New("test")
	})

	// Wait for the wait group to finish.
	wg.Wait()

	// Close the limiter.
	limiter.Close()

	// Check if the retryExhausted channel is closed.
	select {
	case <-limiter.RetryExhausted():
		t.Log("Retry exhausted")
	default:
		t.Error("Retry exhausted channel should be closed")
	}
}

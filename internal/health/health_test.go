package health

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
)

func TestHealthStartup(t *testing.T) {
	// Create a new Health object
	cl := clockwork.NewFakeClock()
	h := &Health{
		Clock: cl,
	}
	// Start the Health object
	h.Start()

	// at time 0 with no registrations, it should be alive and not ready
	alive, ready := h.Report()
	assert.True(t, alive)
	assert.False(t, ready)
	// Stop the Health object
	h.Stop()
}

func TestHealthRegistrationNotReady(t *testing.T) {
	// Create a new Health object
	cl := clockwork.NewFakeClock()
	h := &Health{
		Clock: cl,
	}
	// Start the Health object
	h.Start()
	// at time 0 with no registrations, it should be alive and not ready
	alive, ready := h.Report()
	assert.True(t, alive)
	assert.False(t, ready)

	// register a service that will never report in
	h.Register("foo", 1500*time.Millisecond)
	// now it should also be alive and not ready
	alive, ready = h.Report()
	assert.True(t, alive)
	assert.False(t, ready)

	// and even after the timeout, it should still be alive and not ready
	for i := 0; i < 10; i++ {
		cl.Advance(500 * time.Millisecond)
		time.Sleep(1 * time.Millisecond) // give goroutines time to run
	}
	alive, ready = h.Report()
	assert.True(t, alive)
	assert.False(t, ready)
	// Stop the Health object
	h.Stop()
}

func TestHealthRegistrationAndReady(t *testing.T) {
	// Create a new Health object
	cl := clockwork.NewFakeClock()
	h := &Health{
		Clock: cl,
	}
	// Start the Health object
	h.Start()
	// register a service
	h.Register("foo", 1500*time.Millisecond)
	cl.Advance(500 * time.Millisecond)
	// Tell h we're ready
	h.Ready("foo", true)
	// now h should also be alive and ready
	alive, ready := h.Report()
	assert.True(t, alive)
	assert.True(t, ready)

	// make some periodic ready calls, it should stay alive and ready
	for i := 0; i < 10; i++ {
		h.Ready("foo", true)
		cl.Advance(500 * time.Millisecond)
		time.Sleep(1 * time.Millisecond) // give goroutines time to run
		alive, ready = h.Report()
		assert.True(t, alive)
		assert.True(t, ready)
	}

	// now run for a bit with no ready calls, it should be dead and not ready
	for i := 0; i < 10; i++ {
		cl.Advance(500 * time.Millisecond)
		time.Sleep(1 * time.Millisecond) // give goroutines time to run
	}
	alive, ready = h.Report()
	assert.False(t, alive)
	assert.False(t, ready)
	// Stop the Health object
	h.Stop()
}

func TestHealthReadyFalse(t *testing.T) {
	// Create a new Health object
	cl := clockwork.NewFakeClock()
	h := &Health{
		Clock: cl,
	}
	// Start the Health object
	h.Start()
	// register a service
	h.Register("foo", 1500*time.Millisecond)
	h.Ready("foo", true)

	cl.Advance(500 * time.Millisecond)
	time.Sleep(1 * time.Millisecond) // give goroutines time to run
	alive, ready := h.Report()
	assert.True(t, alive)
	assert.True(t, ready)

	// tell it we're not ready
	h.Ready("foo", false)
	cl.Advance(500 * time.Millisecond)
	time.Sleep(1 * time.Millisecond) // give goroutines time to run
	alive, ready = h.Report()
	assert.True(t, alive)
	assert.False(t, ready)
	// Stop the Health object
	h.Stop()
}

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
	assert.True(t, h.IsAlive())
	assert.False(t, h.IsReady())
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
	assert.True(t, h.IsAlive())
	assert.False(t, h.IsReady())

	// register a service that will never report in
	h.Register("foo", 1500*time.Millisecond)
	// now it should also be alive and not ready
	assert.True(t, h.IsAlive())
	assert.False(t, h.IsReady())

	// and even after the timeout, it should still be alive and not ready
	for i := 0; i < 10; i++ {
		cl.Advance(500 * time.Millisecond)
		time.Sleep(1 * time.Millisecond) // give goroutines time to run
	}
	assert.True(t, h.IsAlive())
	assert.False(t, h.IsReady())
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
	assert.True(t, h.IsAlive())
	assert.True(t, h.IsReady())

	// make some periodic ready calls, it should stay alive and ready
	for i := 0; i < 10; i++ {
		h.Ready("foo", true)
		cl.Advance(500 * time.Millisecond)
		time.Sleep(1 * time.Millisecond) // give goroutines time to run
		assert.True(t, h.IsAlive())
		assert.True(t, h.IsReady())
	}

	// now run for a bit with no ready calls, it should be dead and not ready
	for i := 0; i < 10; i++ {
		cl.Advance(500 * time.Millisecond)
		time.Sleep(1 * time.Millisecond) // give goroutines time to run
	}
	assert.False(t, h.IsAlive())
	assert.False(t, h.IsReady())
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
	assert.True(t, h.IsAlive())
	assert.True(t, h.IsReady())

	// tell it we're not ready
	h.Ready("foo", false)
	cl.Advance(500 * time.Millisecond)
	time.Sleep(1 * time.Millisecond) // give goroutines time to run
	assert.True(t, h.IsAlive())
	assert.False(t, h.IsReady())
	// Stop the Health object
	h.Stop()
}

func TestNotReadyFromOneService(t *testing.T) {
	// Create a new Health object
	cl := clockwork.NewFakeClock()
	h := &Health{
		Clock: cl,
	}
	// Start the Health object
	h.Start()
	h.Register("foo", 1500*time.Millisecond)
	h.Register("bar", 1500*time.Millisecond)
	h.Register("baz", 1500*time.Millisecond)
	h.Ready("foo", true)
	h.Ready("bar", true)
	h.Ready("baz", true)
	assert.True(t, h.IsAlive())
	assert.True(t, h.IsReady())

	// make bar not ready
	h.Ready("bar", false)
	cl.Advance(500 * time.Millisecond)
	time.Sleep(1 * time.Millisecond) // give goroutines time to run
	assert.True(t, h.IsAlive())
	assert.False(t, h.IsReady())
	// Stop the Health object
	h.Stop()
}

package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/metrics"
	"github.com/honeycombio/samproxy/types"
)

// TestCacheSetGet sets a value then fetches it back
func TestCacheSetGet(t *testing.T) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := &DefaultInMemCache{
		Config:  CacheConfig{10},
		Metrics: s,
		Logger:  &logger.NullLogger{},
	}
	c.Start()

	trace := &types.Trace{
		TraceID: "abc123",
	}
	c.Set(trace)
	tr := c.Get(trace.TraceID)
	assert.Equal(t, trace, tr, "fetched trace should equal what we put in")
}

// TestBufferOverrun verifies that when we have more in-flight traces than the
// size of the buffer, we get a buffer overrun metric emitted
func TestBufferOverrun(t *testing.T) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := &DefaultInMemCache{
		Config:  CacheConfig{2},
		Metrics: s,
		Logger:  &logger.NullLogger{},
	}
	c.Start()

	traces := []*types.Trace{
		&types.Trace{TraceID: "abc123"},
		&types.Trace{TraceID: "def456"},
		&types.Trace{TraceID: "ghi789"},
	}

	c.Set(traces[0])
	c.Set(traces[1])
	assert.Equal(t, 0, s.CounterIncrements["collect_cache_buffer_overrun"], "buffer should not yet have overrun")
	c.Set(traces[2])
	assert.Equal(t, 1, s.CounterIncrements["collect_cache_buffer_overrun"], "buffer should have overrun")
}

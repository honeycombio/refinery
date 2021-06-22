// +build all race

package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

// TestCacheSetGet sets a value then fetches it back
func TestCacheSetGet(t *testing.T) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := NewInMemCache(10, s, &logger.NullLogger{})

	trace := &types.Trace{
		TraceID: "abc123",
		Dataset: "test-dataset",
	}
	c.Set(trace)
	tr := c.Get(trace.Dataset, trace.TraceID)
	assert.Equal(t, trace, tr, "fetched trace should equal what we put in")
}

// TestBufferOverrun verifies that when we have more in-flight traces than the
// size of the buffer, we get a buffer overrun metric emitted
func TestBufferOverrun(t *testing.T) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := NewInMemCache(2, s, &logger.NullLogger{})

	traces := []*types.Trace{
		{TraceID: "abc123", Dataset: "test-dataset"},
		{TraceID: "def456", Dataset: "test-dataset"},
		{TraceID: "ghi789", Dataset: "test-dataset"},
	}

	c.Set(traces[0])
	c.Set(traces[1])
	assert.Equal(t, 0, s.CounterIncrements["collect_cache_buffer_overrun"], "buffer should not yet have overrun")
	c.Set(traces[2])
	assert.Equal(t, 1, s.CounterIncrements["collect_cache_buffer_overrun"], "buffer should have overrun")
}

func TestTakeExpiredTraces(t *testing.T) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := NewInMemCache(10, s, &logger.NullLogger{})

	now := time.Now()
	traces := []*types.Trace{
		{TraceID: "1", Dataset: "test-dataset", SendBy: now.Add(-time.Minute), Sent: true},
		{TraceID: "2", Dataset: "test-dataset", SendBy: now.Add(-time.Minute)},
		{TraceID: "3", Dataset: "test-dataset", SendBy: now.Add(time.Minute)},
		{TraceID: "4", Dataset: "test-dataset"},
	}
	for _, t := range traces {
		c.Set(t)
	}

	expired := c.TakeExpiredTraces(now)
	assert.Equal(t, 3, len(expired))
	assert.Equal(t, traces[0], expired[0])
	assert.Equal(t, traces[1], expired[1])
	assert.Equal(t, traces[3], expired[2])

	assert.Equal(t, 1, len(c.cache))

	all := c.GetAll()
	assert.Equal(t, 1, len(all))
	for i := range all {
		assert.Equal(t, traces[2], all[i])
	}
}

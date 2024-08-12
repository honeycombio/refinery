package cache

import (
	"fmt"
	"testing"
	"time"

	"github.com/honeycombio/refinery/generics"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
	"github.com/stretchr/testify/assert"
)

// TestCacheSetGet sets a value then fetches it back
func TestCacheSetGet(t *testing.T) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := NewInMemCache(10, s, &logger.NullLogger{})

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
	c := NewInMemCache(2, s, &logger.NullLogger{})

	traces := []*types.Trace{
		{TraceID: "abc123"},
		{TraceID: "def456"},
		{TraceID: "ghi789"},
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
		{TraceID: "1", SendBy: now.Add(-time.Minute), Sent: true},
		{TraceID: "2", SendBy: now.Add(-time.Minute)},
		{TraceID: "3", SendBy: now.Add(time.Minute)},
		{TraceID: "4"},
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

func TestRemoveSentTraces(t *testing.T) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := NewInMemCache(10, s, &logger.NullLogger{})

	now := time.Now()
	traces := []*types.Trace{
		{TraceID: "1", SendBy: now.Add(-time.Minute), Sent: true},
		{TraceID: "2", SendBy: now.Add(-time.Minute)},
		{TraceID: "3", SendBy: now.Add(time.Minute)},
		{TraceID: "4"},
	}
	for _, t := range traces {
		c.Set(t)
	}

	deletes := generics.NewSet("1", "3", "4", "5")
	c.RemoveTraces(deletes)

	all := c.GetAll()
	assert.Equal(t, 1, len(all))
	assert.Equal(t, traces[1], all[0])
}

func TestSkipOldUnsentTraces(t *testing.T) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := NewInMemCache(4, s, &logger.NullLogger{})

	now := time.Now()
	traces := []*types.Trace{
		{TraceID: "1", SendBy: now.Add(-time.Minute), Sent: true},
		{TraceID: "2", SendBy: now.Add(time.Minute)},
		{TraceID: "3", SendBy: now.Add(-time.Minute)},
		{TraceID: "4", SendBy: now.Add(time.Minute)},
	}
	for _, tr := range traces {
		c.Set(tr)
	}

	// this should remove traces 1 and 3
	expired := c.TakeExpiredTraces(now)
	assert.Equal(t, 2, len(expired))
	assert.Equal(t, traces[0], expired[0])
	assert.Equal(t, traces[2], expired[1])

	assert.Equal(t, 2, len(c.cache))

	// fill up those slots now, which requires skipping over the old traces
	newTraces := []*types.Trace{
		{TraceID: "5", SendBy: now.Add(time.Minute)},
		{TraceID: "6", SendBy: now.Add(time.Minute)},
	}

	for _, tr := range newTraces {
		prev := c.Set(tr)
		assert.Nil(t, prev)
	}

	// now we should have traces 2, 5, 4 and 6, and 4 is next to be examined
	prev := c.Set(&types.Trace{TraceID: "7", SendBy: now})
	// make sure we kicked out #4
	assert.Equal(t, traces[3], prev)
}

// Benchamark the cache's Set method
func BenchmarkCache_Set(b *testing.B) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := NewInMemCache(100000, s, &logger.NullLogger{})
	now := time.Now()
	traces := make([]*types.Trace, 0, b.N)
	for i := 0; i < b.N; i++ {
		traces = append(traces, &types.Trace{
			TraceID: "trace" + fmt.Sprint(i),
			SendBy:  now.Add(time.Duration(i) * time.Second),
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(traces[i])
	}
}

// Benchmark the cache's Get method
func BenchmarkCache_Get(b *testing.B) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := NewInMemCache(100000, s, &logger.NullLogger{})
	now := time.Now()
	traces := make([]*types.Trace, 0, b.N)
	for i := 0; i < b.N; i++ {
		traces = append(traces, &types.Trace{
			TraceID: "trace" + fmt.Sprint(i),
			SendBy:  now.Add(time.Duration(i) * time.Second),
		})
		c.Set(traces[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(traces[i].TraceID)
	}
}

// Benchmark the cache's TakeExpiredTraces method
func BenchmarkCache_TakeExpiredTraces(b *testing.B) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := NewInMemCache(100000, s, &logger.NullLogger{})
	now := time.Now()
	traces := make([]*types.Trace, 0, b.N)
	for i := 0; i < b.N; i++ {
		traces = append(traces, &types.Trace{
			TraceID: "trace" + fmt.Sprint(i),
			SendBy:  now.Add(time.Duration(i) * time.Second),
		})
		c.Set(traces[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.TakeExpiredTraces(now.Add(time.Duration(i) * time.Second))
	}
}

// Benchmark the cache's RemoveTraces method
func BenchmarkCache_RemoveTraces(b *testing.B) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := NewInMemCache(100000, s, &logger.NullLogger{})
	now := time.Now()
	traces := make([]*types.Trace, 0, b.N)
	for i := 0; i < b.N; i++ {
		traces = append(traces, &types.Trace{
			TraceID: "trace" + fmt.Sprint(i),
			SendBy:  now.Add(time.Duration(i) * time.Second),
		})
		c.Set(traces[i])
	}

	deletes := generics.NewSetWithCapacity[string](b.N / 2)
	for i := 0; i < b.N/2; i++ {
		deletes.Add("trace" + fmt.Sprint(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.RemoveTraces(deletes)
	}
}

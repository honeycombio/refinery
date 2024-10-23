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
		{TraceID: "1", SendBy: now.Add(-time.Minute), Sent: true}, // expired
		{TraceID: "2", SendBy: now.Add(-time.Minute)},             // expired
		{TraceID: "3", SendBy: now.Add(time.Minute)},              // not expired
		{TraceID: "4", SendBy: now.Add(time.Minute * 2)},          // not expired
	}
	for _, t := range traces {
		c.Set(t)
	}

	expired := c.TakeExpiredTraces(now, 0)
	assert.Equal(t, 2, len(expired))
	assert.Contains(t, expired, traces[0])
	assert.Contains(t, expired, traces[1])

	assert.Equal(t, 2, c.GetCacheEntryCount())

	all := c.GetAll()
	assert.Equal(t, 2, len(all))
	assert.Contains(t, all, traces[2])
	assert.Contains(t, all, traces[3])
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
		{TraceID: "2", SendBy: now.Add(-time.Minute)},
		{TraceID: "3", SendBy: now.Add(time.Minute)},
		{TraceID: "4", SendBy: now.Add(time.Minute)},
	}
	for _, tr := range traces {
		c.Set(tr)
	}

	// this should remove traces 1 and 3
	expired := c.TakeExpiredTraces(now, 0)
	assert.Equal(t, 2, len(expired))
	assert.Equal(t, traces[0], expired[0])
	assert.Equal(t, traces[1], expired[1])

	assert.Equal(t, 2, c.GetCacheEntryCount())

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
	assert.Equal(t, traces[2], prev)
}

func TestSettingTheSameTraceDoesNotReaddItsIDToTheQueue(t *testing.T) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := NewInMemCache(4, s, &logger.NullLogger{})
	now := time.Now()

	trace := &types.Trace{
		TraceID: "1",
		SendBy:  now.Add(-time.Minute),
	}

	for i := 0; i < 10; i++ {
		c.Set(trace)
		assert.Len(t, c.GetAll(), 1)
		assert.Len(t, c.queue, 1)
	}
}

// Benchamark the cache's Set method
func BenchmarkCache_Set(b *testing.B) {
	metrics := &metrics.MockMetrics{}
	metrics.Start()
	_, traces := generateTraces(b.N)

	c := NewInMemCache(b.N, metrics, &logger.NullLogger{})

	// setup is expensive, so reset timer and report allocations
	b.ResetTimer()
	b.ReportAllocs()

	populateCache(c, traces)
}

// Benchmark the cache's Get method
func BenchmarkCache_Get(b *testing.B) {
	metrics := &metrics.MockMetrics{}
	metrics.Start()
	_, traces := generateTraces(b.N)

	c := NewInMemCache(b.N, metrics, &logger.NullLogger{})
	populateCache(c, traces)

	// setup is expensive, so reset timer and report allocations
	b.ResetTimer()
	b.ReportAllocs()

	for traceID, _ := range traces {
		c.Get(traceID)
	}
}

// Benchmark the cache's TakeExpiredTraces method
func BenchmarkCache_TakeExpiredTraces(b *testing.B) {
	metrics := &metrics.MockMetrics{}
	metrics.Start()
	now, traces := generateTraces(b.N)

	c := NewInMemCache(b.N, metrics, &logger.NullLogger{})
	populateCache(c, traces)

	// setup is expensive, so reset timer and report allocations
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		c.TakeExpiredTraces(now.Add(time.Duration(i)*time.Second), 0)
	}
}

// Benchmark the cache's RemoveTraces method
func BenchmarkCache_RemoveTraces(b *testing.B) {
	metrics := &metrics.MockMetrics{}
	metrics.Start()
	_, traces := generateTraces(b.N)

	deletes := generics.NewSetWithCapacity[string](b.N / 2)
	for i := 0; i < b.N/2; i++ {
		deletes.Add("trace" + fmt.Sprint(i))
	}

	c := NewInMemCache(b.N, metrics, &logger.NullLogger{})
	populateCache(c, traces)

	// setup is expensive, so reset timer and report allocations
	b.ResetTimer()
	b.ReportAllocs()

	c.RemoveTraces(deletes)
}

func generateTraces(n int) (time.Time, map[string]*types.Trace) {
	now := time.Now()
	traces := make(map[string]*types.Trace, n)
	for i := 0; i < n; i++ {
		traceID := "trace" + fmt.Sprint(i)
		traces[traceID] = &types.Trace{
			TraceID: "trace" + fmt.Sprint(i),
			SendBy:  now.Add(time.Duration(i) * time.Second),
		}
	}
	return now, traces
}

func populateCache(c Cache, traces map[string]*types.Trace) {
	for _, trace := range traces {
		c.Set(trace)
	}
}

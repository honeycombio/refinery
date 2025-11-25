package cache

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/honeycombio/refinery/generics"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

// TestCacheSetGet sets a value then fetches it back
func TestCacheSetGet(t *testing.T) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := NewInMemCache(s, &logger.NullLogger{})

	trace := &types.Trace{
		TraceID: "abc123",
	}
	c.Set(trace)
	tr := c.Get(trace.TraceID)
	assert.Equal(t, trace, tr, "fetched trace should equal what we put in")
}

func TestTakeExpiredTraces(t *testing.T) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := NewInMemCache(s, &logger.NullLogger{})

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

	expired := c.TakeExpiredTraces(now, 100, 0, func(trace *types.Trace) bool {
		return trace.ID() != "1"
	})
	assert.Equal(t, 1, len(expired))
	assert.Contains(t, expired, traces[1], expired[0])
	assert.NotContains(t, expired, traces[0])

	assert.Equal(t, 3, c.GetCacheEntryCount())

	all := c.GetAll()
	assert.Equal(t, 3, len(all))
	assert.Contains(t, all, traces[0])
	assert.Contains(t, all, traces[2])
	assert.Contains(t, all, traces[3])
}

func TestTakeExpiredTraces_SpanLimitHitBeforeTraceLimit(t *testing.T) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := NewInMemCache(s, &logger.NullLogger{})

	now := time.Now()
	// Create 5 expired traces, each with 30 spans (total 150 spans)
	// Use different SendBy times for predictable ordering
	traces := make([]*types.Trace, 5)
	for i := 0; i < 5; i++ {
		trace := &types.Trace{
			TraceID: fmt.Sprintf("trace%d", i),
			SendBy:  now.Add(-time.Minute + time.Duration(i)*time.Second),
		}
		// Add 30 spans to each trace
		for j := 0; j < 30; j++ {
			trace.AddSpan(&types.Span{
				Event:   &types.Event{},
				TraceID: trace.TraceID,
			})
		}
		traces[i] = trace
		c.Set(trace)
	}

	// Set trace limit to 10 (high) and span limit to 100
	// Should return only 3 traces (90 spans) because span limit is hit first
	expired := c.TakeExpiredTraces(now, 10, 100, nil)

	assert.Equal(t, 3, len(expired), "should return 3 traces (90 spans) before hitting span limit of 100")

	// Verify total span count
	totalSpans := 0
	for _, trace := range expired {
		totalSpans += int(trace.DescendantCount())
	}
	assert.Equal(t, 90, totalSpans, "should have exactly 90 spans")

	// Verify remaining traces are still in cache
	assert.Equal(t, 2, c.GetCacheEntryCount(), "should have 2 traces left in cache")
}

func TestTakeExpiredTraces_TraceLimitHitBeforeSpanLimit(t *testing.T) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := NewInMemCache(s, &logger.NullLogger{})

	now := time.Now()
	// Create 10 expired traces, each with 5 spans (total 50 spans)
	// Use different SendBy times for predictable ordering
	traces := make([]*types.Trace, 10)
	for i := 0; i < 10; i++ {
		trace := &types.Trace{
			TraceID: fmt.Sprintf("trace%d", i),
			SendBy:  now.Add(-time.Minute + time.Duration(i)*time.Second),
		}
		// Add 5 spans to each trace
		for j := 0; j < 5; j++ {
			trace.AddSpan(&types.Span{
				Event:   &types.Event{},
				TraceID: trace.TraceID,
			})
		}
		traces[i] = trace
		c.Set(trace)
	}

	// Set trace limit to 3 (low) and span limit to 1000 (high)
	// Should return only 3 traces (15 spans) because trace limit is hit first
	expired := c.TakeExpiredTraces(now, 3, 1000, nil)

	assert.Equal(t, 3, len(expired), "should return 3 traces before hitting trace limit")

	// Verify total span count
	totalSpans := 0
	for _, trace := range expired {
		totalSpans += int(trace.DescendantCount())
	}
	assert.Equal(t, 15, totalSpans, "should have exactly 15 spans (3 traces * 5 spans)")

	// Verify remaining traces are still in cache
	assert.Equal(t, 7, c.GetCacheEntryCount(), "should have 7 traces left in cache")
}

func TestTakeExpiredTraces_MixedTraceSizes(t *testing.T) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := NewInMemCache(s, &logger.NullLogger{})

	now := time.Now()

	// Create traces with varying sizes: 10, 50, 100, 20, 30 spans
	// Use different SendBy times to ensure predictable ordering
	spanCounts := []int{10, 50, 100, 20, 30}
	traces := make([]*types.Trace, len(spanCounts))

	for i, spanCount := range spanCounts {
		trace := &types.Trace{
			TraceID: fmt.Sprintf("trace%d", i),
			SendBy:  now.Add(-time.Minute + time.Duration(i)*time.Second), // Different times for ordering
		}
		for j := 0; j < spanCount; j++ {
			trace.AddSpan(&types.Span{
				Event:   &types.Event{},
				TraceID: trace.TraceID,
			})
		}
		traces[i] = trace
		c.Set(trace)
	}

	// Set span limit to 150
	// Should get traces 0 (10 spans) and 1 (50 spans) and 2 (100 spans) = 160, but 160 > 150
	// So should get only traces 0 and 1 = 60 spans
	expired := c.TakeExpiredTraces(now, 10, 150, nil)

	assert.Equal(t, 2, len(expired), "should return 2 traces before hitting span limit")

	totalSpans := 0
	for _, trace := range expired {
		totalSpans += int(trace.DescendantCount())
	}
	assert.Equal(t, 60, totalSpans, "should have exactly 60 spans")

	// Verify the large trace (100 spans) is still in cache and can be retrieved next tick
	assert.Equal(t, 3, c.GetCacheEntryCount(), "should have 3 traces left in cache")

	// Next tick should get the remaining 3 traces: 100, 20, 30 = 150 spans total
	expired2 := c.TakeExpiredTraces(now, 10, 150, nil)
	assert.Equal(t, 3, len(expired2), "should return all 3 remaining traces in second call")

	totalSpans2 := 0
	for _, trace := range expired2 {
		totalSpans2 += int(trace.DescendantCount())
	}
	assert.Equal(t, 150, totalSpans2, "should have exactly 150 spans in second batch")
}

func TestTakeExpiredTraces_SpanLimitZeroMeansNoLimit(t *testing.T) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := NewInMemCache(s, &logger.NullLogger{})

	now := time.Now()
	// Create 5 expired traces, each with 100 spans
	for i := 0; i < 5; i++ {
		trace := &types.Trace{
			TraceID: fmt.Sprintf("trace%d", i),
			SendBy:  now.Add(-time.Minute),
		}
		for j := 0; j < 100; j++ {
			trace.AddSpan(&types.Span{
				Event:   &types.Event{},
				TraceID: trace.TraceID,
			})
		}
		c.Set(trace)
	}

	// Set span limit to 0 (no limit) and trace limit to 10
	// Should return all 5 traces
	expired := c.TakeExpiredTraces(now, 10, 0, nil)

	assert.Equal(t, 5, len(expired), "should return all 5 traces when span limit is 0")

	totalSpans := 0
	for _, trace := range expired {
		totalSpans += int(trace.DescendantCount())
	}
	assert.Equal(t, 500, totalSpans, "should have all 500 spans")
}

func TestTakeExpiredTraces_TracePutBackWhenSpanLimitWouldBeExceeded(t *testing.T) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := NewInMemCache(s, &logger.NullLogger{})

	now := time.Now()

	// Create 3 traces: 40, 40, 80 spans with different SendBy times for predictable ordering
	spanCounts := []int{40, 40, 80}
	for i, spanCount := range spanCounts {
		trace := &types.Trace{
			TraceID: fmt.Sprintf("trace%d", i),
			SendBy:  now.Add(-time.Minute + time.Duration(i)*time.Second),
		}
		for j := 0; j < spanCount; j++ {
			trace.AddSpan(&types.Span{
				Event:   &types.Event{},
				TraceID: trace.TraceID,
			})
		}
		c.Set(trace)
	}

	// Set span limit to 100
	// Should get first 2 traces (80 spans), then stop when third trace would exceed limit
	expired := c.TakeExpiredTraces(now, 10, 100, nil)

	assert.Equal(t, 2, len(expired), "should return 2 traces")

	totalSpans := 0
	for _, trace := range expired {
		totalSpans += int(trace.DescendantCount())
	}
	assert.Equal(t, 80, totalSpans, "should have exactly 80 spans")

	// The third trace should still be in cache
	assert.Equal(t, 1, c.GetCacheEntryCount(), "should have 1 trace left in cache")
	remaining := c.Get("trace2")
	assert.NotNil(t, remaining, "large trace should still be in cache")
	assert.Equal(t, 80, int(remaining.DescendantCount()), "remaining trace should have 80 spans")
}

func TestRemoveSentTraces(t *testing.T) {
	s := &metrics.MockMetrics{}
	s.Start()
	c := NewInMemCache(s, &logger.NullLogger{})

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

// Benchamark the cache's Set method
func BenchmarkCache_Set(b *testing.B) {
	metrics := &metrics.MockMetrics{}
	metrics.Start()
	_, traces := generateTraces(b.N)

	c := NewInMemCache(metrics, &logger.NullLogger{})

	// setup is expensive, so reset timer and report allocations
	b.ReportAllocs()
	b.ResetTimer()

	populateCache(c, traces)
}

// Benchmark the cache's Get method
func BenchmarkCache_Get(b *testing.B) {
	metrics := &metrics.MockMetrics{}
	metrics.Start()
	_, traces := generateTraces(b.N)

	c := NewInMemCache(metrics, &logger.NullLogger{})
	populateCache(c, traces)

	// setup is expensive, so reset timer and report allocations
	b.ReportAllocs()
	b.ResetTimer()

	for traceID, _ := range traces {
		c.Get(traceID)
	}
}

// Benchmark the cache's TakeExpiredTraces method
func BenchmarkCache_TakeExpiredTracesWithoutFilter(b *testing.B) {
	metrics := &metrics.MockMetrics{}
	metrics.Start()
	now, traces := generateTraces(b.N)

	c := NewInMemCache(metrics, &logger.NullLogger{})
	populateCache(c, traces)

	// setup is expensive, so reset timer and report allocations
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.TakeExpiredTraces(now.Add(time.Duration(i)*time.Second), 0, 0, nil)
	}
}

func BenchmarkCache_TakeExpiredTracesWithFilter(b *testing.B) {
	metrics := &metrics.MockMetrics{}
	metrics.Start()
	now, traces := generateTraces(b.N)

	c := NewInMemCache(metrics, &logger.NullLogger{})
	populateCache(c, traces)

	// setup is expensive, so reset timer and report allocations
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.TakeExpiredTraces(now.Add(time.Duration(i)*time.Second), 0, 0, func(trace *types.Trace) bool {
			// filter out 20% of traces
			return rand.Float32() > 0.2
		})
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

	c := NewInMemCache(metrics, &logger.NullLogger{})
	populateCache(c, traces)

	// setup is expensive, so reset timer and report allocations
	b.ReportAllocs()
	b.ResetTimer()

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

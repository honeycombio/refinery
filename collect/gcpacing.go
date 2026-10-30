package collect

import (
	"math"
	"os"
	"runtime/debug"
	rtmetrics "runtime/metrics"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/metrics"
)

// availableMemoryPercentage leaves room under the available memory for memory
// the Go runtime does not track.
const availableMemoryPercentage = 95

// liveHeapRefreshInterval bounds the forced GCs that refresh the live heap
// when it reads over budget: with GOGC off, nothing else would collect
// between a load drop and the next limit-driven GC, so the reading could stay
// stale-high and evict traces that no longer need to go.
const liveHeapRefreshInterval = time.Second

// ConfigureGCPacing paces GC by GOMEMLIMIT at availableMemoryPercentage of
// AvailableMemory, with GOGC off, so that uncollected garbage is not mistaken
// for traces buffered in the cache (see InMemCollector.checkAlloc). An explicit
// GOMEMLIMIT or GOGC env var wins. It does nothing when AvailableMemory is not
// set, or when the memory budget leaves no headroom under the limit, since a
// live heap above a GC limit with GOGC off means back-to-back GCs. Returns the
// limit set, or 0.
func ConfigureGCPacing(c config.CollectionConfig) int64 {
	if _, ok := os.LookupEnv("GOMEMLIMIT"); ok {
		return 0
	}
	limit := int64(c.AvailableMemory) / 100 * availableMemoryPercentage
	if limit <= 0 || int64(c.GetMaxAlloc()) >= limit {
		return 0
	}
	debug.SetMemoryLimit(limit)
	// Only turn GOGC off after the limit is set, so that a mistake here cannot
	// leave the heap unbounded.
	if _, ok := os.LookupEnv("GOGC"); !ok {
		debug.SetGCPercent(-1)
	}
	return limit
}

const (
	rtMetricLiveHeap    = "/gc/heap/live:bytes"
	rtMetricMemoryLimit = "/gc/gomemlimit:bytes"
)

// memory is a reading of the Go heap.
type memory struct {
	// heapObjects is all heap objects, including garbage not yet collected.
	heapObjects uint64
	// liveHeap is the heap marked live by the last GC.
	liveHeap uint64
	// paced is whether a Go memory limit is set, which is when liveHeap rather
	// than heapObjects reflects the memory the cache is holding.
	paced bool
}

// used is the reading to compare against the memory budget.
func (m memory) used() uint64 {
	if m.paced {
		return m.liveHeap
	}
	return m.heapObjects
}

func newMemorySamples() []rtmetrics.Sample {
	return []rtmetrics.Sample{
		{Name: metrics.RtMetricNameMemory},
		{Name: rtMetricLiveHeap},
		{Name: rtMetricMemoryLimit},
	}
}

func readMemory(samples []rtmetrics.Sample) memory {
	rtmetrics.Read(samples)
	return memory{
		heapObjects: samples[0].Value.Uint64(),
		liveHeap:    samples[1].Value.Uint64(),
		paced:       samples[2].Value.Uint64() < math.MaxInt64,
	}
}

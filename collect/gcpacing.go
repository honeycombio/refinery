package collect

import (
	"math"
	"os"
	"runtime/debug"
	rtmetrics "runtime/metrics"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/metrics"
)

// availableMemoryPercentage leaves room under the available memory for memory
// the Go runtime does not track.
const availableMemoryPercentage = 95

// maxBudgetLimitPercentage is how much of the Go memory limit the memory
// budget can take before a live heap near the budget leaves too little
// garbage headroom, and GC with GOGC off runs nearly back to back.
const maxBudgetLimitPercentage = 90

// ConfigureGCPacing paces GC by GOMEMLIMIT at availableMemoryPercentage of
// AvailableMemory, with GOGC off, so that uncollected garbage is not mistaken
// for traces buffered in the cache (see InMemCollector.checkAlloc). An explicit
// GOMEMLIMIT or GOGC env var wins. It does nothing when AvailableMemory is not
// set, or when the memory budget leaves no headroom under the limit, since a
// live heap above a GC limit with GOGC off means back-to-back GCs. Returns the
// limit set, or 0. It does nothing unless EnableGCPacing is set.
func ConfigureGCPacing(c config.CollectionConfig) int64 {
	if !c.EnableGCPacing {
		return 0
	}
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
	rtMetricGCCycles    = "/gc/cycles/total:gc-cycles"
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
	// gcCycles counts completed GCs, so a reading can tell whether liveHeap
	// has been refreshed since the last one.
	gcCycles uint64
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
		{Name: rtMetricGCCycles},
	}
}

// readMemory marks the reading paced only if enableGCPacing is set, so an
// operator's own GOMEMLIMIT does not change what is compared to the budget.
func readMemory(samples []rtmetrics.Sample, enableGCPacing bool) memory {
	rtmetrics.Read(samples)
	return memory{
		heapObjects: samples[0].Value.Uint64(),
		liveHeap:    samples[1].Value.Uint64(),
		paced:       enableGCPacing && samples[2].Value.Uint64() < math.MaxInt64,
		gcCycles:    samples[3].Value.Uint64(),
	}
}

// warnOnBudgetNearGCLimit warns when GC is paced and the memory budget is above
// maxBudgetLimitPercentage of the Go memory limit.
func (i *InMemCollector) warnOnBudgetNearGCLimit() {
	if !i.enableGCPacing {
		return
	}
	limit := debug.SetMemoryLimit(-1)
	budget := int64(i.Config.GetCollectionConfig().GetMaxAlloc())
	if limit == math.MaxInt64 || budget <= limit/100*maxBudgetLimitPercentage {
		return
	}
	i.Logger.Warn().
		WithField("memory_budget_bytes", budget).
		WithField("gomemlimit_bytes", limit).
		Logf("memory budget is close to GOMEMLIMIT, so GC can run nearly back to back; lower MaxMemoryPercentage")
}

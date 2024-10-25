package cache

import (
	"math"
	"time"

	"github.com/honeycombio/refinery/generics"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
	"github.com/rdleal/go-priorityq/kpq"
	"golang.org/x/exp/maps"
)

// Cache is a non-threadsafe cache. It must not be used for concurrent access.
type Cache interface {
	// Set adds the trace to the cache. If it is kicking out a trace from the cache
	// that has not yet been sent, it will return that trace. Otherwise returns nil.
	Set(trace *types.Trace)
	Get(traceID string) *types.Trace
	// GetAll is used during shutdown to get all in-flight traces to flush them
	GetAll() []*types.Trace

	// GetCacheCapacity returns the number of traces that can be stored in the cache
	GetCacheCapacity() int

	// GetCacheEntryCount returns the number of traces currently stored in the cache
	GetCacheEntryCount() int

	// Retrieve and remove all traces which are past their SendBy date.
	// Does not check whether they've been sent.
	TakeExpiredTraces(now time.Time, max int, filter func(*types.Trace) bool) []*types.Trace

	// RemoveTraces accepts a set of trace IDs and removes any matching ones from
	RemoveTraces(toDelete generics.Set[string])
}

var _ Cache = (*DefaultInMemCache)(nil)

// DefaultInMemCache keeps a bounded number of entries to avoid growing memory
// forever. Traces are expunged from the cache in insertion order (not access
// order) so it is important to have a cache larger than trace throughput *
// longest trace.
type DefaultInMemCache struct {
	Metrics metrics.Metrics
	Logger  logger.Logger

	pq    *kpq.KeyedPriorityQueue[string, time.Time]
	cache map[string]*types.Trace
}

const DefaultInMemCacheCapacity = 10000

var collectCacheMetrics = []metrics.Metadata{
	{Name: "collect_cache_buffer_overrun", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "The number of times the trace overwritten in the circular buffer has not yet been sent"},
	{Name: "collect_cache_capacity", Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "The number of traces that can be stored in the cache"},
	{Name: "collect_cache_entries", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "The number of traces currently stored in the cache"},
}

func NewInMemCache(
	capacity int,
	met metrics.Metrics,
	logger logger.Logger,
) *DefaultInMemCache {
	logger.Debug().Logf("Starting DefaultInMemCache")
	defer func() { logger.Debug().Logf("Finished starting DefaultInMemCache") }()

	for _, metadata := range collectCacheMetrics {
		met.Register(metadata)
	}

	cmp := func(v1, v2 time.Time) bool {
		return v1.Before(v2)
	}

	return &DefaultInMemCache{
		Metrics: met,
		Logger:  logger,
		pq:      kpq.NewKeyedPriorityQueue[string](cmp),
		cache:   make(map[string]*types.Trace),
	}
}

func (d *DefaultInMemCache) GetCacheCapacity() int {
	return math.MaxInt32
}

func (d *DefaultInMemCache) GetCacheEntryCount() int {
	return len(d.cache)
}

func (d *DefaultInMemCache) Set(trace *types.Trace) {
	// we need to dereference the trace ID so skip bad inserts to avoid panic
	if trace == nil {
		return
	}

	// update the cache and priority queue
	if d.cache[trace.TraceID] != nil {
		d.pq.Push(trace.TraceID, trace.SendBy)
	} else {
		d.pq.Update(trace.TraceID, trace.SendBy)
	}
	d.cache[trace.TraceID] = trace
	return
}

func (d *DefaultInMemCache) Get(traceID string) *types.Trace {
	return d.cache[traceID]
}

// GetAll is not thread safe and should only be used when that's ok
// Returns all non-nil trace entries.
func (d *DefaultInMemCache) GetAll() []*types.Trace {
	return maps.Values(d.cache)
}

var anyTraceFilter = func(*types.Trace) bool { return true }

// TakeExpiredTraces should be called to decide which traces are past their expiration time;
// It removes and returns them.
// If a filter is provided, it will be called with each trace to determine if it should be skipped.
func (d *DefaultInMemCache) TakeExpiredTraces(now time.Time, max int, filter func(*types.Trace) bool) []*types.Trace {
	d.Metrics.Histogram("collect_cache_entries", float64(len(d.cache)))

	if filter == nil {
		filter = anyTraceFilter
	}

	var traces []*types.Trace
	for !d.pq.IsEmpty() && len(traces) < max {
		// peek the first sendBy in the queue to see if we should try to send it
		sendBy, ok := d.pq.PeekValue()
		if !ok || now.Before(sendBy) {
			break
		}

		// dequeue the traceID
		traceID, _, _ := d.pq.Pop()

		// if the trace is no longer in the cache, skip it
		if d.cache[traceID] == nil || !filter(d.cache[traceID]) {
			continue
		}

		// add the trace to the list of expired traces and remove it from the cache
		traces = append(traces, d.cache[traceID])
		delete(d.cache, traceID)
	}
	return traces
}

// RemoveTraces accepts a set of trace IDs and removes any matching ones from
// the insertion list. This is used in the case of a cache overrun.
func (d *DefaultInMemCache) RemoveTraces(toDelete generics.Set[string]) {
	d.Metrics.Histogram("collect_cache_entries", float64(len(d.cache)))
	for _, traceID := range toDelete.Members() {
		d.pq.Remove(traceID)
		delete(d.cache, traceID)
	}
}

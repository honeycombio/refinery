package cache

import (
	"math"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/honeycombio/refinery/generics"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

// Cache is a non-threadsafe cache. It must not be used for concurrent access.
type Cache interface {
	// Set adds the trace to the cache. If it is kicking out a trace from the cache
	// that has not yet been sent, it will return that trace. Otherwise returns nil.
	Set(trace *types.Trace) *types.Trace
	Get(traceID string) *types.Trace
	// GetAll is used during shutdown to get all in-flight traces to flush them
	GetAll() []*types.Trace

	// GetCacheCapacity returns the number of traces that can be stored in the cache
	GetCacheCapacity() int

	// GetCacheEntryCount returns the number of traces currently stored in the cache
	GetCacheEntryCount() int

	// Retrieve and remove all traces which are past their SendBy date.
	// Does not check whether they've been sent.
	TakeExpiredTraces(now time.Time) []*types.Trace

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

	cache    *lru.Cache[string, *types.Trace]
	capacity int
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

	// if using the default capacity, allow the cache to grow really large by using math.MaxInt32 (2147483647)
	if capacity == DefaultInMemCacheCapacity {
		capacity = math.MaxInt32
	}
	cache, err := lru.New[string, *types.Trace](capacity)
	if err != nil {
		logger.Error().Logf("Failed to create LRU cache: %s", err)
		return nil
	}

	return &DefaultInMemCache{
		Metrics:  met,
		Logger:   logger,
		cache:    cache,
		capacity: capacity,
	}
}

func (d *DefaultInMemCache) GetCacheCapacity() int {
	return d.capacity
}

func (d *DefaultInMemCache) GetCacheEntryCount() int {
	return d.cache.Len()
}

// Set adds the trace to the ring. When the ring wraps around and hits a trace
// that has not been sent, it will try up to 5 times to skip that entry and find
// a slot that is available. If it is unable to do so, it will kick out the
// trace it is overwriting and return that trace. Otherwise returns nil.
func (d *DefaultInMemCache) Set(trace *types.Trace) *types.Trace {
	// we need to dereference the trace ID so skip bad inserts to avoid panic
	if trace == nil {
		return nil
	}

	// set retTrace to a trace if it is getting kicked out without having been
	// sent. Leave it nil if we're not kicking out an unsent trace.
	var retTrace *types.Trace
	if d.cache.Len() >= d.capacity {
		_, retTrace, _ = d.cache.RemoveOldest()
		// if it hasn't already been sent,
		// record that we're overrunning the buffer
		if !retTrace.Sent {
			d.Metrics.Increment("collect_cache_buffer_overrun")
		}
	}

	d.cache.Add(trace.TraceID, trace)
	return retTrace
}

func (d *DefaultInMemCache) Get(traceID string) *types.Trace {
	trace, _ := d.cache.Get(traceID)
	return trace
}

// GetAll is not thread safe and should only be used when that's ok
// Returns all non-nil trace entries.
func (d *DefaultInMemCache) GetAll() []*types.Trace {
	return d.cache.Values()
}

// TakeExpiredTraces should be called to decide which traces are past their expiration time;
// It removes and returns them.
func (d *DefaultInMemCache) TakeExpiredTraces(now time.Time) []*types.Trace {
	d.Metrics.Gauge("collect_cache_capacity", float64(d.capacity))
	d.Metrics.Histogram("collect_cache_entries", float64(d.cache.Len()))

	var res []*types.Trace
	for _, t := range d.cache.Values() {
		if now.After(t.SendBy) {
			res = append(res, t)
			d.cache.Remove(t.TraceID)
			continue
		}
		break
	}
	return res
}

// RemoveTraces accepts a set of trace IDs and removes any matching ones from
// the insertion list. This is used in the case of a cache overrun.
func (d *DefaultInMemCache) RemoveTraces(toDelete generics.Set[string]) {
	d.Metrics.Gauge("collect_cache_capacity", float64(d.capacity))
	d.Metrics.Histogram("collect_cache_entries", float64(d.cache.Len()))

	for _, traceID := range toDelete.Members() {
		d.cache.Remove(traceID)
	}
}

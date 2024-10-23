package cache

import (
	"time"

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
	TakeExpiredTraces(now time.Time, max int) []*types.Trace

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

	cache    map[string]*types.Trace
	queue    []string // used as a FIFO queue of trace IDs
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

	return &DefaultInMemCache{
		Metrics:  met,
		Logger:   logger,
		capacity: capacity,
		cache:    make(map[string]*types.Trace, capacity),
		queue:    make([]string, 0, capacity),
	}
}

func (d *DefaultInMemCache) GetCacheCapacity() int {
	return d.capacity
}

func (d *DefaultInMemCache) GetCacheEntryCount() int {
	return len(d.cache)
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

	var evictedTrace *types.Trace
	_, exists := d.cache[trace.TraceID]
	if !exists {
		// if we're at capacity, we need to evict the oldest trace
		if len(d.cache) >= d.capacity {
			for len(d.queue) > 0 {
				// get the oldest trace from the queue
				traceID := d.queue[0]

				// get the trace from the cache
				evictedTrace = d.cache[traceID]
				if evictedTrace != nil {
					// if it hasn't already been sent,
					// record that we're overrunning the buffer
					if !evictedTrace.Sent {
						d.Metrics.Increment("collect_cache_buffer_overrun")
					}

					// remove the trace from the cache and queue
					delete(d.cache, traceID)
					d.queue = d.queue[1:]
					break
				}
			}

			// check that we found a trace to evict
			if evictedTrace == nil {
				d.Logger.Warn().Logf("Failed to evict a trace from the cache when at capacity")
			}
		}

		// add new trace to queue
		d.queue = append(d.queue, trace.TraceID)
	}

	// add new trace to cache
	d.cache[trace.TraceID] = trace
	return evictedTrace
}

func (d *DefaultInMemCache) Get(traceID string) *types.Trace {
	return d.cache[traceID]
}

// GetAll is not thread safe and should only be used when that's ok
// Returns all non-nil trace entries.
func (d *DefaultInMemCache) GetAll() []*types.Trace {
	items := make([]*types.Trace, 0, len(d.cache))
	for _, trace := range d.cache {
		items = append(items, trace)
	}
	return items
}

// TakeExpiredTraces should be called to decide which traces are past their expiration time;
// It removes and returns them.
func (d *DefaultInMemCache) TakeExpiredTraces(now time.Time, max int) []*types.Trace {
	d.Metrics.Gauge("collect_cache_capacity", float64(d.capacity))
	d.Metrics.Histogram("collect_cache_entries", float64(len(d.cache)))

	var expired []*types.Trace
	for len(d.queue) > 0 {
		// get oldest trace ID from queue
		traceID := d.queue[0]

		// lookup trace in cache
		trace := d.cache[traceID]
		if trace != nil {
			// check if trace has expired
			if now.Before(trace.SendBy) {
				// not expired, we stop looking as the queue is ordered by insertion time
				break
			}

			// trace has expired
			expired = append(expired, trace)

			// remove the trace from the cache
			delete(d.cache, traceID)
		}

		// remove the trace ID from the queue
		d.queue = d.queue[1:]
	}
	return expired
}

// RemoveTraces accepts a set of trace IDs and removes any matching ones from
// the insertion list. This is used in the case of a cache overrun.
func (d *DefaultInMemCache) RemoveTraces(toDelete generics.Set[string]) {
	d.Metrics.Gauge("collect_cache_capacity", float64(d.capacity))
	d.Metrics.Histogram("collect_cache_entries", float64(len(d.cache)))

	for _, traceID := range toDelete.Members() {
		delete(d.cache, traceID)
	}
}

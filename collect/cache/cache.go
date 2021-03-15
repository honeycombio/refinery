package cache

import (
	"time"

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

	// Retrieve and remove all traces which are past their SendBy date.
	// Does not check whether they've been sent.
	TakeExpiredTraces(now time.Time) []*types.Trace
}

// DefaultInMemCache keeps a bounded number of entries to avoid growing memory
// forever. Traces are expunged from the cache in insertion order (not access
// order) so it is important to have a cache larger than trace throughput *
// longest trace.
type DefaultInMemCache struct {
	Metrics metrics.Metrics
	Logger  logger.Logger

	cache map[string]*types.Trace

	// insertionOrder is a circular buffer of currently stored traces
	insertionOrder []*types.Trace
	// insertPoint is the current location in the circle.
	insertPoint int
}

const DefaultInMemCacheCapacity = 10000

func NewInMemCache(
	capacity int,
	metrics metrics.Metrics,
	logger logger.Logger,
) *DefaultInMemCache {
	logger.Debug().Logf("Starting DefaultInMemCache")
	defer func() { logger.Debug().Logf("Finished starting DefaultInMemCache") }()

	// buffer_overrun increments when the trace overwritten in the circular
	// buffer has not yet been sent
	metrics.Register("collect_cache_buffer_overrun", "counter")
	metrics.Register("collect_cache_capacity", "gauge")
	metrics.Register("collect_cache_entries", "histogram")

	if capacity == 0 {
		capacity = DefaultInMemCacheCapacity
	}

	return &DefaultInMemCache{
		Metrics:        metrics,
		Logger:         logger,
		cache:          make(map[string]*types.Trace, capacity),
		insertionOrder: make([]*types.Trace, capacity),
	}

}

func (d *DefaultInMemCache) GetCacheSize() int {
	return len(d.insertionOrder)
}

// Set adds the trace to the ring. If it is kicking out a trace from the ring
// that has not yet been sent, it will return that trace. Otherwise returns nil.
func (d *DefaultInMemCache) Set(trace *types.Trace) *types.Trace {

	// set retTrace to a trace if it is getting kicked out without having been
	// sent. Leave it nil if we're not kicking out an unsent trace.
	var retTrace *types.Trace

	// we need to dereference the trace ID so skip bad inserts to avoid panic
	if trace == nil {
		return nil
	}

	// increment the trace pointer when we're done
	defer func() { d.insertPoint++ }()

	// loop insert point when we get to the end of the ring
	if d.insertPoint >= len(d.insertionOrder) {
		d.insertPoint = 0
	}

	// store the trace
	d.cache[trace.TraceID] = trace

	// expunge the trace in the current spot in the insertion ring
	oldTrace := d.insertionOrder[d.insertPoint]
	if oldTrace != nil {
		delete(d.cache, oldTrace.TraceID)
		if !oldTrace.Sent {
			// if it hasn't already been sent,
			// record that we're overrunning the buffer
			d.Metrics.Increment("collect_cache_buffer_overrun")
			// and return the trace so it can be sent.
			retTrace = oldTrace
		}
	}
	// record the trace in the insertion ring
	d.insertionOrder[d.insertPoint] = trace
	return retTrace
}

func (d *DefaultInMemCache) Get(traceID string) *types.Trace {
	return d.cache[traceID]
}

// GetAll is not thread safe and should only be used when that's ok
// Returns all non-nil trace entries.
func (d *DefaultInMemCache) GetAll() []*types.Trace {
	tmp := make([]*types.Trace, 0, len(d.insertionOrder))
	for _, t := range d.insertionOrder {
		if t != nil {
			tmp = append(tmp, t)
		}
	}
	return tmp
}

func (d *DefaultInMemCache) TakeExpiredTraces(now time.Time) []*types.Trace {
	d.Metrics.Gauge("collect_cache_capacity", float64(len(d.insertionOrder)))
	d.Metrics.Histogram("collect_cache_entries", float64(len(d.cache)))

	var res []*types.Trace
	for i, t := range d.insertionOrder {
		if t != nil && now.After(t.SendBy) {
			res = append(res, t)
			d.insertionOrder[i] = nil
			delete(d.cache, t.TraceID)
		}
	}
	return res
}

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

	cache map[string]*types.Trace

	// traceBuffer is a circular buffer of currently stored traces
	traceBuffer []*types.Trace
	// currentIndex is the current location in the circle.
	currentIndex int
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
		Metrics:     metrics,
		Logger:      logger,
		cache:       make(map[string]*types.Trace, capacity),
		traceBuffer: make([]*types.Trace, capacity),
	}

}

func (d *DefaultInMemCache) GetCacheCapacity() int {
	return len(d.traceBuffer)
}

// looks for an insertion point by trying the next N slots in the circular buffer
// returns the index of the first empty slot it finds, or the first slot that
// has a trace that has already been sent. If it doesn't find anything, it
// returns the index of the last slot it looked at.
func (d *DefaultInMemCache) findNextInsertionPoint(maxtries int) int {
	ip := d.currentIndex
	for i := 0; i < maxtries; i++ {
		ip++
		if ip >= len(d.traceBuffer) {
			ip = 0
		}
		oldTrace := d.traceBuffer[ip]
		if oldTrace == nil || oldTrace.Sent {
			break
		}
	}
	// we didn't find anything we can overwrite, so we have to kick one out
	return ip
}

// Set adds the trace to the ring. When the ring wraps around and hits a trace
// that has not been sent, it will try up to 5 times to skip that entry and find
// a slot that is available. If it is unable to do so, it will kick out the
// trace it is overwriting and return that trace. Otherwise returns nil.
func (d *DefaultInMemCache) Set(trace *types.Trace) *types.Trace {

	// set retTrace to a trace if it is getting kicked out without having been
	// sent. Leave it nil if we're not kicking out an unsent trace.
	var retTrace *types.Trace

	// we need to dereference the trace ID so skip bad inserts to avoid panic
	if trace == nil {
		return nil
	}

	// store the trace
	d.cache[trace.TraceID] = trace

	// figure out where to put it; try 5 times to find an empty slot
	ip := d.findNextInsertionPoint(5)
	// make sure we will record the trace in the right place
	defer func() { d.currentIndex = ip }()
	// expunge the trace at this point in the insertion ring, if necessary
	oldTrace := d.traceBuffer[ip]
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
	d.traceBuffer[ip] = trace
	return retTrace
}

func (d *DefaultInMemCache) Get(traceID string) *types.Trace {
	return d.cache[traceID]
}

// GetAll is not thread safe and should only be used when that's ok
// Returns all non-nil trace entries.
func (d *DefaultInMemCache) GetAll() []*types.Trace {
	tmp := make([]*types.Trace, 0, len(d.traceBuffer))
	for _, t := range d.traceBuffer {
		if t != nil {
			tmp = append(tmp, t)
		}
	}
	return tmp
}

// TakeExpiredTraces should be called to decide which traces are past their expiration time;
// It removes and returns them.
func (d *DefaultInMemCache) TakeExpiredTraces(now time.Time) []*types.Trace {
	d.Metrics.Gauge("collect_cache_capacity", float64(len(d.traceBuffer)))
	d.Metrics.Histogram("collect_cache_entries", float64(len(d.cache)))

	var res []*types.Trace
	for i, t := range d.traceBuffer {
		if t != nil && now.After(t.SendBy) {
			res = append(res, t)
			d.traceBuffer[i] = nil
			delete(d.cache, t.TraceID)
		}
	}
	return res
}

// RemoveTraces accepts a set of trace IDs and removes any matching ones from
// the insertion list. This is used in the case of a cache overrun.
func (d *DefaultInMemCache) RemoveTraces(toDelete generics.Set[string]) {
	d.Metrics.Gauge("collect_cache_capacity", float64(len(d.traceBuffer)))
	d.Metrics.Histogram("collect_cache_entries", float64(len(d.cache)))

	for i, t := range d.traceBuffer {
		if t != nil {
			if toDelete.Contains(t.TraceID) {
				d.traceBuffer[i] = nil
				delete(d.cache, t.TraceID)
			}
		}
	}
}

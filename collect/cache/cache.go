package cache

import (
	"sync"

	"github.com/honeycombio/malinois/metrics"
	"github.com/honeycombio/malinois/types"
)

type Cache interface {
	Set(trace *types.Trace)
	Get(traceID string) *types.Trace
	// GetAll is used during shutdown to get all in-flight traces to flush them
	GetAll() []*types.Trace
}

// DefaultInMemCache keeps a bounded number of entries to avoid growing memory
// forever. Traces are expunged from the cache in insertion order (not access
// order) so it is important to have a cache larger than trace throughput *
// longest trace.
type DefaultInMemCache struct {
	Config  CacheConfig
	Metrics metrics.Metrics

	lock  sync.Mutex
	cache map[string]*types.Trace

	// insertionOrder is a circular buffer of currently stored traces
	insertionOrder []*types.Trace
	// insertPoint is the current location in the circle.
	insertPoint int
}

type CacheConfig struct {
	CacheCapacity int
}

const DefaultInMemCacheCapacity = 10000

func (d *DefaultInMemCache) Start() error {
	if d.Config.CacheCapacity == 0 {
		d.Config.CacheCapacity = DefaultInMemCacheCapacity
	}
	d.cache = make(map[string]*types.Trace, d.Config.CacheCapacity)
	d.insertionOrder = make([]*types.Trace, d.Config.CacheCapacity)

	// register statistics sent by this module

	// buffer_overrun increments when the trace overwritten in the circular
	// buffer has not yet been sent
	d.Metrics.Register("collect_cache_buffer_overrun", "counter")

	return nil
}

func (d *DefaultInMemCache) GetCacheSize() int {
	return d.Config.CacheCapacity
}

func (d *DefaultInMemCache) Set(trace *types.Trace) {
	d.lock.Lock()
	defer d.lock.Unlock()

	// we need to dereference the trace ID so skip bad inserts to avoid panic
	if trace == nil {
		return
	}

	// increment the trace pointer when we're done
	defer func() { d.insertPoint++ }()

	// loop insert point when we get to the end of the ring
	if d.insertPoint >= d.Config.CacheCapacity {
		d.insertPoint = 0
	}

	// store the trace
	d.cache[trace.TraceID] = trace

	// expunge the trace in the current spot in the insertion ring
	oldTrace := d.insertionOrder[d.insertPoint]
	if oldTrace != nil {
		delete(d.cache, oldTrace.TraceID)
		// and record if we're overrunning the circle
		if !oldTrace.GetSent() {
			d.Metrics.IncrementCounter("collect_cache_buffer_overrun")
		}
	}
	// record the trace in the insertion ring
	d.insertionOrder[d.insertPoint] = trace
}

func (d *DefaultInMemCache) Get(traceID string) *types.Trace {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.cache[traceID]
}

// GetAll is not thread safe and should only be used when that's ok
func (d *DefaultInMemCache) GetAll() []*types.Trace {
	d.lock.Lock()
	defer d.lock.Unlock()
	// make a copy so it doesn't get modified for the poor soul trying to use
	// this list after it's returned
	tmp := make([]*types.Trace, len(d.insertionOrder))
	copy(tmp, d.insertionOrder)
	return tmp
}

package cache

import (
	"fmt"
	"time"

	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"

	fibheap "github.com/starwander/GoFibonacciHeap"
)

// // Cache is a non-threadsafe cache. It must not be used for concurrent access.
// type Cache interface {
// 	// Set adds the trace to the cache. If it is kicking out a trace from the cache
// 	// that has not yet been sent, it will return that trace. Otherwise returns nil.
// 	Set(trace *types.Trace) *types.Trace
// 	Get(traceID string) *types.Trace
// 	// GetAll is used during shutdown to get all in-flight traces to flush them
// 	GetAll() []*types.Trace

// 	// Retrieve and remove all traces which are past their SendBy date.
// 	// Does not check whether they've been sent.
// 	TakeExpiredTraces(now time.Time) []*types.Trace
// }

// FibonacciCache keeps a bounded number of entries to avoid growing memory
// forever. It uses a fibonacci heap to keep track of the expiration times of
// the entries.
type FibonacciCache struct {
	Metrics metrics.Metrics
	Logger  logger.Logger

	heap     *fibheap.FibHeap
	capacity int
}

const FibonacciCacheCapacity = 10000

func NewFibonacciCache(capacity int, metrics metrics.Metrics, logger logger.Logger) *FibonacciCache {
	logger.Debug().Logf("Starting FibonacciCache")
	defer func() { logger.Debug().Logf("Finished starting FibonacciCache") }()

	// buffer_overrun increments when the trace overwritten in the circular
	// buffer has not yet been sent
	metrics.Register("collect_cache_buffer_overrun", "counter")
	metrics.Register("collect_cache_capacity", "gauge")
	metrics.Register("collect_cache_entries", "histogram")

	if capacity == 0 {
		capacity = FibonacciCacheCapacity
	}

	return &FibonacciCache{
		Metrics:  metrics,
		Logger:   logger,
		heap:     fibheap.NewFibHeap(),
		capacity: capacity,
	}

}

func (f *FibonacciCache) GetCacheSize() int {
	return int(f.heap.Num())
}

// Set adds the trace to the cach. If it is kicking out a trace from the cache
// that has not yet been sent, it will return that trace. Otherwise returns nil.
func (f *FibonacciCache) Set(trace *types.Trace) *types.Trace {

	// set retTrace to a trace if it is getting kicked out without having been
	// sent. Leave it nil if we're not kicking out an unsent trace.
	var retTrace *types.Trace

	if int(f.heap.Num()) >= f.capacity {
		// the heap is full, so prepare to return the oldest trace
		// (if it hasn't been sent already)
		oldTrace := f.heap.ExtractMinValue().(*types.Trace)
		if !oldTrace.Sent {
			// if it hasn't already been sent,
			// record that we're overrunning the buffer
			f.Metrics.Increment("collect_cache_buffer_overrun")
			// and return the trace so it can be sent.
			retTrace = oldTrace
		}
	}

	// store the trace
	f.heap.InsertValue(trace)
	return retTrace
}

func (f *FibonacciCache) Get(traceID string) *types.Trace {
	return f.heap.GetValue(traceID).(*types.Trace)
}

// GetAll is not thread safe and should only be used when that's ok
// Returns all non-nil trace entries.
func (f *FibonacciCache) GetAll() []*types.Trace {
	tmp := make([]*types.Trace, 0, f.GetCacheSize())
	for v := f.heap.ExtractMinValue(); v != nil; v = f.heap.ExtractMinValue() {
		t := v.(*types.Trace)
		fmt.Println(t.Key())
		tmp = append(tmp, v.(*types.Trace))
	}
	return tmp
}

// TakeExpiredTraces should be called to decide which traces are past their expiration time;
// It removes and returns them.
func (f *FibonacciCache) TakeExpiredTraces(now time.Time) []*types.Trace {
	f.Metrics.Gauge("collect_cache_capacity", float64(f.capacity))
	f.Metrics.Histogram("collect_cache_entries", float64(f.heap.Num()))

	var res []*types.Trace
	// repeat while we're not empty and the minimum value is expired
	for t := f.heap.MinimumValue().(*types.Trace); t != nil && now.After(t.SendBy); t = f.heap.MinimumValue().(*types.Trace) {
		res = append(res, t)
		f.heap.DeleteValue(t)
	}
	return res
}

// RemoveTraces accepts a set of trace IDs and removes any matching ones from
// the insertion list. This is used in the case of a cache overrun.
func (f *FibonacciCache) RemoveTraces(toDelete map[string]struct{}) {
	f.Metrics.Gauge("collect_cache_capacity", float64(f.capacity))
	f.Metrics.Histogram("collect_cache_entries", float64(f.heap.Num()))

	for k := range toDelete {
		f.heap.Delete(k)
	}
}

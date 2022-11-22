package cache

import (
	"sync"

	"github.com/honeycombio/refinery/metrics"
	cuckoo "github.com/panmari/cuckoofilter"
)

// These are the names of metrics tracked for the cuckoo filter
const (
	CurrentLoadFactor = "cuckoo_current_load_factor"
	FutureLoadFactor  = "cuckoo_future_load_factor"
	CurrentCapacity   = "cuckoo_current_capacity"
)

// This wraps a cuckoo filter implementation in a way that lets us keep it running forever
// without filling up.
// A cuckoo filter can't be emptied (you can delete individual items if you know what they are,
// but you can't get their names from the filter. Consequently, what we do is keep *two* filters,
// current and future. The current one is the one we use to check against, and when we add, we
// add to both. But the future one is started *after* the current one, so that when the current
// gets too full, we can discard it, replace it with future, and then start a new, empty future.
// This is why the future filter is nil until the current filter reaches .5.
// You must call Maintain() periodically, most likely from a goroutine. The call is cheap,
// and the timing isn't very critical. The effect of going above "capacity" is an increased
// false positive rate, but the filter continues to function.
type CuckooTraceChecker struct {
	current  *cuckoo.Filter
	future   *cuckoo.Filter
	mut      sync.RWMutex
	capacity uint
	met      metrics.Metrics
}

func NewCuckooTraceChecker(capacity uint, m metrics.Metrics) *CuckooTraceChecker {
	return &CuckooTraceChecker{
		capacity: capacity,
		current:  cuckoo.NewFilter(capacity),
		future:   nil,
		met:      m,
	}
}

// Add puts a traceID into the filter.
func (c *CuckooTraceChecker) Add(traceID string) {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.current.Insert([]byte(traceID))
	// don't add anything to future if it doesn't exist yet
	if c.future != nil {
		c.future.Insert([]byte(traceID))
	}
}

// Check tests if a traceID is (very probably) in the filter.
func (c *CuckooTraceChecker) Check(traceID string) bool {
	b := []byte(traceID)
	c.mut.RLock()
	defer c.mut.RUnlock()
	return c.current.Lookup(b)
}

// Maintain should be called periodically; if the current filter is full, it replaces
// it with the future filter and creates a new future filter.
func (c *CuckooTraceChecker) Maintain() {
	c.mut.RLock()
	currentLoadFactor := c.current.LoadFactor()
	c.met.Gauge(CurrentLoadFactor, currentLoadFactor)
	if c.future != nil {
		c.met.Gauge(FutureLoadFactor, c.future.LoadFactor())
	}
	c.met.Gauge(CurrentCapacity, c.capacity)
	c.mut.RUnlock()

	// once the current one is half loaded, we can start using the future one too
	if c.future == nil && currentLoadFactor > 0.5 {
		c.mut.Lock()
		c.future = cuckoo.NewFilter(c.capacity)
		c.mut.Unlock()
	}

	// if the current one is full, cycle the filters
	if currentLoadFactor > 0.99 {
		c.mut.Lock()
		defer c.mut.Unlock()
		c.current = c.future
		c.future = cuckoo.NewFilter(c.capacity)
	}
}

// SetNextCapacity adjusts the capacity that will be set for the future filter on the next replacement.
func (c *CuckooTraceChecker) SetNextCapacity(capacity uint) {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.capacity = capacity
}

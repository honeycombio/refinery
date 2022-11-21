package cache

import (
	"sync"

	cuckoo "github.com/panmari/cuckoofilter"
)

// This wraps a cuckoo filter implementation in a way that lets us keep it running forever
// without filling up.
// You must call Maintain() periodically, most likely from a goroutine.
// We maintain two filters that are out of sync; when current is full, future is half full
// and we then discard current and replace it with the future, and then create a new, empty future.
// This means that we need to use the firstTime flag to avoid putting anything into future
// until after current reaches .5.
type CuckooTraceChecker struct {
	current   *cuckoo.Filter
	future    *cuckoo.Filter
	mut       sync.RWMutex
	capacity  uint
	firstTime bool
}

func NewCuckooTraceChecker(capacity uint) *CuckooTraceChecker {
	return &CuckooTraceChecker{
		capacity: capacity,
		current:  cuckoo.NewFilter(capacity),
		future:   cuckoo.NewFilter(capacity),
	}
}

// Add puts a traceID into the filter.
func (c *CuckooTraceChecker) Add(traceID string) {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.current.Insert([]byte(traceID))
	// don't add anything to future until we're no longer in the 'firstTime' section.
	if !c.firstTime {
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
	dropFactor := c.current.LoadFactor()
	c.mut.RUnlock()

	// once the current one is half full, we can drop the firstTime check
	if (c.firstTime && dropFactor > 0.5) || dropFactor > 0.99 {
		c.mut.Lock()
		defer c.mut.Unlock()
		c.firstTime = false
		// if the current one is full, cycle the filters
		if dropFactor > 0.99 {
			c.current = c.future
			c.future = cuckoo.NewFilter(c.capacity)
		}
	}
}

// SetNextCapacity adjusts the capacity that will be set for the future filter on the next replacement.
func (c *CuckooTraceChecker) SetNextCapacity(capacity uint) {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.capacity = capacity
}

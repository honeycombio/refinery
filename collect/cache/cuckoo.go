package cache

import (
	"sync"

	cuckoo "github.com/panmari/cuckoofilter"
)

// This wraps a cuckoo filter implementation in a way that lets us keep it running forever without filling up.
// You must call Maintain() periodically, most likely from a goroutine.
// We maintain two filters that are out of sync; when current is full, future is half full and we then discard current
// and replace it with the future, and start with a new empty future.

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

func (c *CuckooTraceChecker) Add(traceID string) {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.current.Insert([]byte(traceID))
	if !c.firstTime {
		c.future.Insert([]byte(traceID))
	}
}

func (c *CuckooTraceChecker) Check(traceID string) bool {
	b := []byte(traceID)
	c.mut.RLock()
	defer c.mut.RUnlock()
	return c.current.Lookup(b)
}

func (c *CuckooTraceChecker) Maintain() {
	c.mut.RLock()
	dropFactor := c.current.LoadFactor()
	c.mut.RUnlock()
	// once the current one is half full, we can drop the firstTime check
	if c.firstTime && dropFactor > 0.5 {
		c.mut.Lock()
		defer c.mut.Unlock()
		c.firstTime = false
	}
	// if the current one is full, cycle the filters
	if dropFactor > 0.99 {
		c.mut.Lock()
		defer c.mut.Unlock()
		c.current = c.future
		c.future = cuckoo.NewFilter(c.capacity)
	}
}

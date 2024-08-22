package cache

import (
	"sync"
	"time"

	"github.com/honeycombio/refinery/metrics"
	cuckoo "github.com/panmari/cuckoofilter"
)

// These are the names of metrics tracked for the cuckoo filter
const (
	CurrentLoadFactor = "cuckoo_current_load_factor"
	FutureLoadFactor  = "cuckoo_future_load_factor"
	CurrentCapacity   = "cuckoo_current_capacity"
	AddQueueFull      = "cuckoo_addqueue_full"
	AddQueueLockTime  = "cuckoo_addqueue_locktime_uS"
)

// This wraps a cuckoo filter implementation in a way that lets us keep it running forever
// without filling up.
// A cuckoo filter can't be emptied (you can delete individual items if you know what they are,
// but you can't get their names from the filter). Consequently, what we do is keep *two* filters,
// current and future. The current one is the one we use to check against, and when we add, we
// add to both. But the future one is started *after* the current one, so that when the current
// gets too full, we can discard it, replace it with future, and then start a new, empty future.
// This is why the future filter is nil until the current filter reaches .5.
// You must call Maintain() periodically, most likely from a goroutine. The call is cheap,
// and the timing isn't very critical. The effect of going above "capacity" is an increased
// false positive rate and slightly reduced performance, but the filter continues to function.
type CuckooTraceChecker struct {
	current  *cuckoo.Filter
	future   *cuckoo.Filter
	mut      sync.RWMutex
	capacity uint
	met      metrics.Metrics
	addch    chan string
}

const (
	// This is how many items can be in the Add Queue before we start blocking on Add.
	AddQueueDepth = 1000
	// This is how long we'll sleep between possible lock cycles.
	AddQueueSleepTime = 100 * time.Microsecond
)

func NewCuckooTraceChecker(capacity uint, m metrics.Metrics) *CuckooTraceChecker {
	c := &CuckooTraceChecker{
		capacity: capacity,
		current:  cuckoo.NewFilter(capacity),
		future:   nil,
		met:      m,
		addch:    make(chan string, AddQueueDepth),
	}

	// To try to avoid blocking on Add, we have a goroutine that pulls from a
	// channel and adds to the filter.
	go func() {
		ticker := time.NewTicker(AddQueueSleepTime)
		for range ticker.C {
			// as long as there's anything still in the channel, keep trying to drain it
			for len(c.addch) > 0 {
				c.drain()
			}
		}
	}()

	return c
}

// This function records all the traces that were in the channel at the start of
// the call. The idea is to add as many as possible under a single lock. We do
// limit our lock hold time to 1ms, so if we can't add them all in that time, we
// stop and let the next call pick up the rest. We track a histogram metric
// about lock time.
func (c *CuckooTraceChecker) drain() {
	n := len(c.addch)
	if n == 0 {
		return
	}
	c.mut.Lock()
	// we don't start the timer until we have the lock, because we don't want to be counting
	// the time we're waiting for the lock.
	lockStart := time.Now()
	timeout := time.NewTimer(1 * time.Millisecond)
outer:
	for i := 0; i < n; i++ {
		select {
		case t := <-c.addch:
			s := []byte(t)
			c.current.Insert(s)
			// don't add anything to future if it doesn't exist yet
			if c.future != nil {
				c.future.Insert(s)
			}
		case <-timeout.C:
			break outer
		default:
			// if the channel is empty, stop
			break outer
		}
	}
	c.mut.Unlock()
	timeout.Stop()
	qlt := time.Since(lockStart)
	c.met.Histogram(AddQueueLockTime, qlt.Microseconds())
}

// Add puts a traceID into the filter. We need this to be fast
// and not block, so we have a channel and a goroutine that
// drains it. If the channel is full, we drop this traceID.
func (c *CuckooTraceChecker) Add(traceID string) {
	select {
	case c.addch <- traceID:
	default:
		// if the channel is full, count this in a metric
		// but drop it on the floor; we're running so hot
		// that we can't keep up.
		c.met.Up(AddQueueFull)
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
	// make sure it's drained first; this just makes sure we record as much as
	// possible before we start messing with the filters.
	c.drain()

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

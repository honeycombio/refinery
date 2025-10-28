package cache

import (
	"sync"
	"time"

	cuckoo "github.com/panmari/cuckoofilter"

	"github.com/honeycombio/refinery/metrics"
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
	mut      sync.Mutex
	capacity uint
	met      metrics.Metrics
	addch    chan string

	done       chan struct{}
	shutdownWG sync.WaitGroup
}

const (
	// This is how many items can be in the Add Queue before we start blocking on Add.
	AddQueueDepth = 1000
	// Number of worker goroutines processing inserts
	NumInsertWorkers = 4
)

var cuckooTraceCheckerMetrics = []metrics.Metadata{
	{Name: CurrentCapacity, Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "current capacity of the cuckoo filter"},
	{Name: FutureLoadFactor, Type: metrics.Gauge, Unit: metrics.Percent, Description: "the fraction of slots occupied in the future cuckoo filter"},
	{Name: CurrentLoadFactor, Type: metrics.Gauge, Unit: metrics.Percent, Description: "the fraction of slots occupied in the current cuckoo filter"},
	{Name: AddQueueFull, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "the number of times the add queue was full and a drop decision was dropped"},
	{Name: AddQueueLockTime, Type: metrics.Histogram, Unit: metrics.Microseconds, Description: "the time spent holding the add queue lock"},
}

var batchPool = sync.Pool{
	New: func() interface{} {
		return make([]string, 0, AddQueueDepth)
	},
}

func NewCuckooTraceChecker(capacity uint, m metrics.Metrics) *CuckooTraceChecker {
	c := &CuckooTraceChecker{
		capacity: capacity,
		current:  cuckoo.NewFilter(capacity),
		future:   nil,
		met:      m,
		addch:    make(chan string, AddQueueDepth),
		done:     make(chan struct{}),
	}
	for _, metric := range cuckooTraceCheckerMetrics {
		m.Register(metric)
	}

	// Start multiple worker goroutines to process inserts.
	// Multiple workers increase the chances that at least one can acquire
	// the lock when competing with many Check() operations.
	c.shutdownWG.Add(NumInsertWorkers)
	for i := 0; i < NumInsertWorkers; i++ {
		go func() {
			defer c.shutdownWG.Done()

			for {
				select {
				case t := <-c.addch:
					batch := batchPool.Get().([]string)
					batch = append(batch, t)
					// Batch up to 1000 items to reduce lock acquisitions
					for len(batch) < 1000 && len(c.addch) > 0 {
						batch = append(batch, <-c.addch)
					}

					c.insertBatch(batch)

				case <-c.done:
					return
				}
			}
		}()
	}

	return c
}

func (c *CuckooTraceChecker) Stop() {
	// stop the goroutine that drains the add channel
	close(c.done)
	close(c.addch)
	c.shutdownWG.Wait()

	// make sure we drain the channel one last time
	for len(c.addch) > 0 {
		c.drain()
	}
}

// insertBatch inserts a batch of traceIDs into the filters.
// once the batch has been inserted, the batch slice is reset and returned
// to the sync.Pool
func (c *CuckooTraceChecker) insertBatch(batch []string) {
	if len(batch) == 0 {
		batchPool.Put(batch)
		return
	}

	c.mut.Lock()
	lockStart := time.Now()

	for _, b := range batch {
		c.current.Insert([]byte(b))
		// don't add anything to future if it doesn't exist yet
		if c.future != nil {
			c.future.Insert([]byte(b))
		}
	}
	c.mut.Unlock()

	qlt := time.Since(lockStart)
	c.met.Histogram(AddQueueLockTime, float64(qlt.Microseconds()))

	// Reset batch and return to pool
	batch = batch[:0]
	batchPool.Put(batch)
}

// drain collects all pending items from the channel and inserts them.
// This is called by Maintain() and Stop() to ensure all queued items are processed.
func (c *CuckooTraceChecker) drain() {
	n := len(c.addch)
	if n == 0 {
		return
	}

	batch := batchPool.Get().([]string)
	for i := 0; i < n; i++ {
		select {
		case t, ok := <-c.addch:
			// if the channel is closed, we will stop processing
			if !ok {
				break
			}
			batch = append(batch, t)
		default:
			// if the channel is empty, stop
			break
		}
	}

	c.insertBatch(batch)
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
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.current.Lookup([]byte(traceID))
}

// Maintain should be called periodically; if the current filter is full, it replaces
// it with the future filter and creates a new future filter.
func (c *CuckooTraceChecker) Maintain() {
	// make sure it's drained first; this just makes sure we record as much as
	// possible before we start messing with the filters.
	c.drain()

	var futureLoadFactor float64
	c.mut.Lock()
	currentLoadFactor := c.current.LoadFactor()
	if c.future != nil {
		futureLoadFactor = c.future.LoadFactor()
	}
	currentCapacity := c.capacity
	c.mut.Unlock()

	// once the current one is half loaded, we can start using the future one too
	if futureLoadFactor == 0 && currentLoadFactor > 0.5 {
		c.mut.Lock()
		if c.future == nil {
			c.future = cuckoo.NewFilter(c.capacity)
		}
		c.mut.Unlock()
	}

	// if the current one is full, cycle the filters
	if currentLoadFactor > 0.99 {
		c.mut.Lock()
		c.current = c.future
		c.future = cuckoo.NewFilter(c.capacity)
		c.mut.Unlock()
	}

	c.met.Gauge(FutureLoadFactor, futureLoadFactor)
	c.met.Gauge(CurrentLoadFactor, currentLoadFactor)
	c.met.Gauge(CurrentCapacity, float64(currentCapacity))

}

// SetNextCapacity adjusts the capacity that will be set for the future filter on the next replacement.
func (c *CuckooTraceChecker) SetNextCapacity(capacity uint) {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.capacity = capacity
}

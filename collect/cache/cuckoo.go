package cache

import (
	"sync"
	"time"

	cuckoo "github.com/panmari/cuckoofilter"
	"github.com/sourcegraph/conc/pool"

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
	mut      sync.RWMutex
	capacity uint
	met      metrics.Metrics
	addch    chan string

	workerPool *pool.Pool
	done       chan struct{}
	shutdownWG sync.WaitGroup
}

const (
	// This is how many items can be in the Add Queue before we start blocking on Add.
	AddQueueDepth = 10000
	// Maximum number of concurrent workers processing batches
	MaxNumWorkers = 50
)

var batchPool = sync.Pool{
	New: func() any {
		return make([]string, 0, AddQueueDepth)
	},
}

func newBatch() []string {
	batch := batchPool.Get().([]string)
	return batch[:0]
}

var cuckooTraceCheckerMetrics = []metrics.Metadata{
	{Name: CurrentCapacity, Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "current capacity of the cuckoo filter"},
	{Name: FutureLoadFactor, Type: metrics.Gauge, Unit: metrics.Percent, Description: "the fraction of slots occupied in the future cuckoo filter"},
	{Name: CurrentLoadFactor, Type: metrics.Gauge, Unit: metrics.Percent, Description: "the fraction of slots occupied in the current cuckoo filter"},
	{Name: AddQueueFull, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "the number of times the add queue was full and a drop decision was dropped"},
	{Name: AddQueueLockTime, Type: metrics.Histogram, Unit: metrics.Microseconds, Description: "the time spent holding the add queue lock"},
}

func NewCuckooTraceChecker(capacity uint, m metrics.Metrics) *CuckooTraceChecker {
	c := &CuckooTraceChecker{
		capacity:   capacity,
		current:    cuckoo.NewFilter(capacity),
		future:     nil,
		met:        m,
		addch:      make(chan string, AddQueueDepth),
		done:       make(chan struct{}),
		workerPool: pool.New().WithMaxGoroutines(MaxNumWorkers),
	}
	for _, metric := range cuckooTraceCheckerMetrics {
		m.Register(metric)
	}

	// Start distributor goroutine that accumulates batches and spawns workers
	c.shutdownWG.Add(1)
	go func() {
		defer c.shutdownWG.Done()
		defer c.drainAddChannel()

		batch := newBatch()

		for {
			select {
			case traceID, ok := <-c.addch:
				if !ok {
					// Channel closed, process remaining batch and drain any buffered items
					if len(batch) > 0 {
						c.spawnWorker(batch)
					} else {
						batchPool.Put(batch)
					}
					return
				}
				batch = append(batch, traceID)

				// accumulate more items from the channel up to BatchSize
				for i := 0; i < len(c.addch); i++ {
					select {
					case nextID, ok := <-c.addch:
						if !ok {
							// Channel closed, spawn worker with current batch and drain
							c.spawnWorker(batch)
							return
						}
						batch = append(batch, nextID)
					default:
						// No more items immediately available, break and spawn worker
						break
					}
				}
				c.spawnWorker(batch)
				batch = newBatch()

			case <-c.done:
				// Shutdown signal, process remaining batch and drain channel
				if len(batch) > 0 {
					c.spawnWorker(batch)
				} else {
					batchPool.Put(batch)
				}
				return
			}
		}
	}()

	return c
}

// drainAddChannel drains any remaining items from addch and processes them.
// It blocks until the channel is closed and all buffered items are processed.
func (c *CuckooTraceChecker) drainAddChannel() {
	batch := newBatch()

	for {
		select {
		case traceID, ok := <-c.addch:

			if !ok {
				// Channel closed and no more items
				if len(batch) > 0 {
					c.spawnWorker(batch)
				} else {
					batchPool.Put(batch)
				}
				return
			}
			batch = append(batch, traceID)
		default:
			if len(batch) > 0 {
				c.spawnWorker(batch)
			} else {
				batchPool.Put(batch)
			}
			return
		}
	}
}

// spawnWorker spawns a new worker goroutine to process the batch.
// It uses the worker pool to limit the number of concurrent workers.
func (c *CuckooTraceChecker) spawnWorker(batch []string) {
	c.workerPool.Go(func() {
		c.insertBatch(batch)
	})
}

// insertBatch grabs the write lock and inserts all items in the batch into
// the current and future cuckoo filters. It has a 1ms timeout to avoid holding
// the lock for too long.
func (c *CuckooTraceChecker) insertBatch(batch []string) {
	if len(batch) == 0 {
		batchPool.Put(batch)
		return
	}

	c.mut.Lock()
	// we don't start the timer until we have the lock, because we don't want to be counting
	// the time we're waiting for the lock.
	lockStart := time.Now()
	for _, traceID := range batch {

		c.current.Insert([]byte(traceID))
		// don't add anything to future if it doesn't exist yet
		if c.future != nil {
			c.future.Insert([]byte(traceID))
		}
	}
	c.mut.Unlock()

	qlt := time.Since(lockStart)
	c.met.Histogram(AddQueueLockTime, float64(qlt.Microseconds()))

	batchPool.Put(batch)
}

func (c *CuckooTraceChecker) Stop() {
	// signal shutdown and close the add channel
	close(c.done)
	close(c.addch)

	// wait for distributor to finish
	c.shutdownWG.Wait()

	// wait for all workers in the pool to finish
	c.workerPool.Wait()
}

// Add puts a traceID into the filter. We need this to be fast
// and not block, so we have a channel that feeds a distributor
// which accumulates batches and spawns workers. If the channel
// is full, we drop this traceID.
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
	c.mut.RLock()
	currentLoadFactor := c.current.LoadFactor()
	c.met.Gauge(CurrentLoadFactor, currentLoadFactor)
	if c.future != nil {
		c.met.Gauge(FutureLoadFactor, c.future.LoadFactor())
	}
	c.met.Gauge(CurrentCapacity, float64(c.capacity))
	c.mut.RUnlock()

	// once the current one is half loaded, we can start using the future one too
	if c.future == nil && currentLoadFactor > 0.5 {
		c.mut.Lock()
		c.future = cuckoo.NewFilter(c.capacity)
		c.mut.Unlock()
	}

	// if the current one is full, cycle the filters
	// The write lock ensures no workers are mid-insert when we swap filters
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

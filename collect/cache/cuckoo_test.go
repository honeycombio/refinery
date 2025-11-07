package cache

import (
	"math/rand"
	"testing"
	"time"

	"github.com/dgryski/go-wyhash"
	"github.com/sourcegraph/conc/pool"

	"github.com/honeycombio/refinery/metrics"
)

// genID returns a random hex string of length numChars
var seed = 3565269841805
var rng = wyhash.Rng(seed)

const charset = "abcdef0123456789"

func genID(numChars int) string {
	id := make([]byte, numChars)
	for i := 0; i < numChars; i++ {
		id[i] = charset[int(rng.Next()%uint64(len(charset)))]
	}
	return string(id)
}

// Benchmark the Add function
func BenchmarkCuckooTraceChecker_Add(b *testing.B) {
	traceIDs := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		traceIDs[i] = genID(32)
	}

	c := NewCuckooTraceChecker(1000000, &metrics.NullMetrics{})
	defer c.Stop()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Add(traceIDs[i])
	}
}

func BenchmarkCuckooTraceChecker_AddParallel(b *testing.B) {
	traceIDs := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		traceIDs[i] = genID(32)
	}
	const numGoroutines = 70

	p := pool.New().WithMaxGoroutines(numGoroutines + 1)
	stop := make(chan struct{})
	p.Go(func() {
		select {
		case <-stop:
			return
		default:
			rand.Intn(100)
		}
	})

	c := NewCuckooTraceChecker(1000000, &metrics.NullMetrics{})
	defer c.Stop()
	ch := make(chan int, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		p.Go(func() {
			for n := range ch {
				if i%10000 == 0 {
					c.Maintain()
				}
				c.Add(traceIDs[n])
			}
		})
	}
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		ch <- j
		if j%1000 == 0 {
			// just give things a moment to run
			time.Sleep(1 * time.Microsecond)
		}
	}
	close(ch)
	close(stop)
	p.Wait()
}

func BenchmarkCuckooTraceChecker_Check(b *testing.B) {
	traceIDs := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		traceIDs[i] = genID(32)
	}

	c := NewCuckooTraceChecker(1000000, &metrics.NullMetrics{})
	defer c.Stop()
	// add every other one to the filter
	for i := 0; i < b.N; i += 2 {
		if i%10000 == 0 {
			c.Maintain()
		}
		c.Add(traceIDs[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Check(traceIDs[i])
	}
}

func BenchmarkCuckooTraceChecker_CheckParallel(b *testing.B) {
	traceIDs := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		traceIDs[i] = genID(32)
	}

	c := NewCuckooTraceChecker(1000000, &metrics.NullMetrics{})
	defer c.Stop()
	for i := 0; i < b.N; i += 2 {
		if i%10000 == 0 {
			c.Maintain()
		}
		c.Add(traceIDs[i])
	}

	const numGoroutines = 70

	p := pool.New().WithMaxGoroutines(numGoroutines + 1)
	stop := make(chan struct{})
	p.Go(func() {
		n := 0
		select {
		case <-stop:
			return
		default:
			if n&1 == 0 {
				c.Add(traceIDs[n])
			}
			n++
			if n >= b.N {
				n = 0
			}
		}
	})

	ch := make(chan int, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		p.Go(func() {
			for n := range ch {
				c.Check(traceIDs[n])
			}
		})
	}
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		ch <- j
	}
	close(ch)
	close(stop)
	p.Wait()
}

func BenchmarkCuckooTraceChecker_CheckAddParallel(b *testing.B) {
	traceIDs := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		traceIDs[i] = genID(32)
	}

	met := &metrics.MockMetrics{}
	met.Start()
	c := NewCuckooTraceChecker(1000000, met)
	defer c.Stop()
	const numCheckers = 30
	const numAdders = 30

	p := pool.New().WithMaxGoroutines(numCheckers + numAdders)
	stop := make(chan struct{})
	addch := make(chan int, numCheckers)
	checkch := make(chan int, numCheckers)
	for i := 0; i < numAdders; i++ {
		p.Go(func() {
			for n := range addch {
				if n%10000 == 0 {
					c.Maintain()
				}
				c.Add(traceIDs[n])
			}
		})
	}
	for i := 0; i < numCheckers; i++ {
		p.Go(func() {
			for n := range checkch {
				c.Check(traceIDs[n])
			}
		})
	}
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		addch <- j
		checkch <- j
	}
	close(addch)
	close(checkch)
	close(stop)
	p.Wait()
	b.StopTimer()
}

// Test that items are correctly added and can be checked
func TestCuckooTraceChecker_AddAndCheck(t *testing.T) {
	c := NewCuckooTraceChecker(10000, &metrics.NullMetrics{})
	defer c.Stop()

	// Add a bunch of items
	ids := make([]string, 2000)
	for i := 0; i < 2000; i++ {
		ids[i] = genID(32)
		c.Add(ids[i])
	}

	// Give time for distributor and workers to process
	time.Sleep(50 * time.Millisecond)

	// Check that the items are in the filter
	for i := 0; i < 2000; i++ {
		if !c.Check(ids[i]) {
			t.Errorf("Expected to find trace ID %s in filter", ids[i])
		}
	}
}

// Test batch accumulation behavior
func TestCuckooTraceChecker_BatchAccumulation(t *testing.T) {
	c := NewCuckooTraceChecker(100000, &metrics.NullMetrics{})
	defer c.Stop()

	// Add exactly BatchSize items
	ids := make([]string, AddQueueDepth)
	for i := 0; i < AddQueueDepth; i++ {
		ids[i] = genID(32)
		c.Add(ids[i])
	}

	// Give time for distributor and workers to process
	time.Sleep(50 * time.Millisecond)

	// All items should be in the filter
	for i := 0; i < AddQueueDepth; i++ {
		if !c.Check(ids[i]) {
			t.Errorf("Expected to find trace ID %s in filter after batch", ids[i])
		}
	}
}

// Test concurrent adding and checking
func TestCuckooTraceChecker_ConcurrentAccess(t *testing.T) {
	c := NewCuckooTraceChecker(100000, &metrics.NullMetrics{})

	const numGoroutines = 10
	const itemsPerGoroutine = 500

	p := pool.New().WithMaxGoroutines(numGoroutines * 2)

	// Pre-generate all IDs
	allIDs := make([][]string, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		allIDs[i] = make([]string, itemsPerGoroutine)
		for j := 0; j < itemsPerGoroutine; j++ {
			allIDs[i][j] = genID(32)
		}
	}

	// Spawn goroutines to add items
	for i := 0; i < numGoroutines; i++ {
		idx := i
		p.Go(func() {
			for j := 0; j < itemsPerGoroutine; j++ {
				c.Add(allIDs[idx][j])
			}
		})
	}

	// Wait for adds to complete
	p.Wait()

	// Stop will wait for all pending work to complete
	c.Stop()

	// Verify all items were added
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < itemsPerGoroutine; j++ {
			if !c.Check(allIDs[i][j]) {
				t.Errorf("Expected to find trace ID %s in filter", allIDs[i][j])
			}
		}
	}
}

// Test that Stop properly drains all pending work
func TestCuckooTraceChecker_StopDrainsPendingWork(t *testing.T) {
	c := NewCuckooTraceChecker(100000, &metrics.NullMetrics{})

	// Add items
	ids := make([]string, 3000)
	for i := 0; i < 3000; i++ {
		ids[i] = genID(32)
		c.Add(ids[i])
	}

	// Stop immediately - should wait for workers to finish
	c.Stop()

	// All items should be processed
	for i := 0; i < 3000; i++ {
		if !c.Check(ids[i]) {
			t.Errorf("Expected to find trace ID %s in filter after Stop()", ids[i])
		}
	}
}

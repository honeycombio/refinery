package cache

import (
	"math/rand"
	"testing"
	"time"

	"github.com/dgryski/go-wyhash"
	"github.com/honeycombio/refinery/metrics"
	"github.com/sourcegraph/conc/pool"
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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Add(traceIDs[i])
	}
	c.drain()
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
	c.drain()
}

func BenchmarkCuckooTraceChecker_Check(b *testing.B) {
	traceIDs := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		traceIDs[i] = genID(32)
	}

	c := NewCuckooTraceChecker(1000000, &metrics.NullMetrics{})
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
	c.drain()
}

func BenchmarkCuckooTraceChecker_CheckParallel(b *testing.B) {
	traceIDs := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		traceIDs[i] = genID(32)
	}

	c := NewCuckooTraceChecker(1000000, &metrics.NullMetrics{})
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
	c.drain()
}

func BenchmarkCuckooTraceChecker_CheckAddParallel(b *testing.B) {
	traceIDs := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		traceIDs[i] = genID(32)
	}

	met := &metrics.MockMetrics{}
	met.Start()
	c := NewCuckooTraceChecker(1000000, met)
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
	c.drain()
	b.StopTimer()
}

package metrics

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// These tests do a concurrency check for the getOrAdd lock semantics, and generally verify that getOrAdd
// is functional under load.
func Test_getOrAdd_counter(t *testing.T) {
	var lock *sync.RWMutex = &sync.RWMutex{}
	var metrics map[string]*counter = make(map[string]*counter)

	const nthreads = 5

	wg := sync.WaitGroup{}

	for i := 0; i < nthreads; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 1000; j++ {
				name := "foo"
				var ctr *counter = getOrAdd(lock, name, metrics, createCounter)
				ctr.lock.Lock()
				ctr.val++
				ctr.lock.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()

	var ctr *counter = getOrAdd(lock, "foo", metrics, createCounter)
	assert.Equal(t, nthreads*1000, ctr.val)
}

func Test_getOrAdd_gauge(t *testing.T) {
	var lock *sync.RWMutex = &sync.RWMutex{}
	var metrics map[string]*gauge = make(map[string]*gauge)

	const nthreads = 5

	wg := sync.WaitGroup{}

	for i := 0; i < nthreads; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 1000; j++ {
				name := "foo"
				var g *gauge = getOrAdd(lock, name, metrics, createGauge)
				g.lock.Lock()
				g.val++
				g.lock.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()

	var g *gauge = getOrAdd(lock, "foo", metrics, createGauge)
	assert.Equal(t, float64(nthreads*1000), g.val)
}

func Test_getOrAdd_histogram(t *testing.T) {
	var lock *sync.RWMutex = &sync.RWMutex{}
	var metrics map[string]*histogram = make(map[string]*histogram)

	const nthreads = 5

	wg := sync.WaitGroup{}

	for i := 0; i < nthreads; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 1000; j++ {
				name := "foo"
				var h *histogram = getOrAdd(lock, name, metrics, createHistogram)
				h.lock.Lock()
				if len(h.vals) == 0 {
					h.vals = append(h.vals, 0)
				}
				h.vals[0]++
				h.lock.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()

	var h *histogram = getOrAdd(lock, "foo", metrics, createHistogram)
	assert.Equal(t, float64(nthreads*1000), h.vals[0])
}

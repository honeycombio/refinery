package metrics

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
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
				atomic.AddInt64(&ctr.val, 1)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	var ctr *counter = getOrAdd(lock, "foo", metrics, createCounter)
	assert.Equal(t, int64(nthreads*1000), ctr.get())
}

func Test_getOrAdd_gauge(t *testing.T) {
	var lock *sync.RWMutex = &sync.RWMutex{}
	var metrics map[string]*gauge = make(map[string]*gauge)

	const nthreads = 5

	wg := sync.WaitGroup{}

	for i := 0; i < nthreads; i++ {
		wg.Add(1)
		go func(idx int) {
			for j := 0; j < 1000; j++ {
				name := "foo"
				var g *gauge = getOrAdd(lock, name, metrics, createGauge)

				g.store(float64(idx*1000 + j))
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	var g *gauge = getOrAdd(lock, "foo", metrics, createGauge)
	// it should end with 999 regardless of the thread index
	val := int64(g.get())
	assert.Equal(t, int64(999), val%1000)
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

func Test_getOrAdd_updown(t *testing.T) {
	var lock *sync.RWMutex = &sync.RWMutex{}
	var metrics map[string]*updown = make(map[string]*updown)

	const nthreads = 6 // must be even, since half count up and the other half count down

	wg := sync.WaitGroup{}

	for i := 0; i < nthreads; i++ {
		wg.Add(1)
		go func(direction bool) {
			for j := 0; j < 1000; j++ {
				name := "foo"
				var ctr *updown = getOrAdd(lock, name, metrics, createUpdown)
				if direction {
					atomic.AddInt64(&ctr.val, 1)
				} else {
					atomic.AddInt64(&ctr.val, -1)
				}
			}
			wg.Done()
		}(i%2 == 0)
	}
	wg.Wait()

	var ctr *updown = getOrAdd(lock, "foo", metrics, createUpdown)
	assert.Equal(t, int64(0), ctr.get())
}

func TestMetricsUpdown(t *testing.T) {
	conf := &config.MockConfig{}
	m := LegacyMetrics{
		Config: conf,
		Logger: &logger.NullLogger{},
	}
	m.Start()
	m.Register(Metadata{
		Name: "foo",
		Type: UpDown,
	})
	m.Up("foo")
	m.Up("foo")
	m.Down("foo")
	assert.Equal(t, int64(1), m.updowns["foo"].val)
}

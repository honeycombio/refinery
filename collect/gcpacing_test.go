package collect

import (
	"context"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/honeycombio/refinery/config"
)

func restoreGCSettings(t *testing.T) {
	limit := debug.SetMemoryLimit(-1)
	gcPercent := debug.SetGCPercent(-1)
	debug.SetGCPercent(gcPercent)
	t.Cleanup(func() {
		debug.SetMemoryLimit(limit)
		debug.SetGCPercent(gcPercent)
	})
}

func newPacingTestCollector(t *testing.T, maxAlloc config.MemorySize, clock clockwork.Clock) *InMemCollector {
	return newTestCollector(t, &config.MockConfig{
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(time.Second),
		},
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			TraceTimeout: config.Duration(5 * time.Millisecond),
		},
		GetCollectionConfigVal: config.CollectionConfig{MaxAlloc: maxAlloc},
	}, clock)
}

func TestConfigureGCPacing(t *testing.T) {
	const gib = 1 << 30
	tests := []struct {
		name          string
		env           map[string]string
		conf          config.CollectionConfig
		wantReturn    int64
		wantLimit     int64
		wantGCPercent int
	}{
		{
			name:          "no AvailableMemory leaves defaults",
			conf:          config.CollectionConfig{MaxAlloc: 10 * gib},
			wantLimit:     math.MaxInt64,
			wantGCPercent: 100,
		},
		{
			name:          "AvailableMemory sets limit to 95% and turns GOGC off",
			conf:          config.CollectionConfig{AvailableMemory: 100 * gib, MaxMemoryPercentage: 75},
			wantReturn:    95 * gib,
			wantLimit:     95 * gib,
			wantGCPercent: -1,
		},
		{
			name:          "explicit GOGC is kept",
			env:           map[string]string{"GOGC": "100"},
			conf:          config.CollectionConfig{AvailableMemory: 100 * gib, MaxMemoryPercentage: 75},
			wantReturn:    95 * gib,
			wantLimit:     95 * gib,
			wantGCPercent: 100,
		},
		{
			name:          "explicit GOMEMLIMIT wins",
			env:           map[string]string{"GOMEMLIMIT": "10GiB"},
			conf:          config.CollectionConfig{AvailableMemory: 100 * gib, MaxMemoryPercentage: 75},
			wantLimit:     math.MaxInt64,
			wantGCPercent: 100,
		},
		{
			name:          "budget at the limit would thrash GC, so leave defaults",
			conf:          config.CollectionConfig{AvailableMemory: 100 * gib, MaxMemoryPercentage: 95},
			wantLimit:     math.MaxInt64,
			wantGCPercent: 100,
		},
		{
			name:          "budget just under the limit is paced",
			conf:          config.CollectionConfig{AvailableMemory: 100 * gib, MaxMemoryPercentage: 94},
			wantReturn:    95 * gib,
			wantLimit:     95 * gib,
			wantGCPercent: -1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			restoreGCSettings(t)
			debug.SetMemoryLimit(math.MaxInt64)
			debug.SetGCPercent(100)
			for _, key := range []string{"GOGC", "GOMEMLIMIT"} {
				t.Setenv(key, "")
				os.Unsetenv(key)
			}
			for key, value := range tt.env {
				t.Setenv(key, value)
			}

			assert.Equal(t, tt.wantReturn, ConfigureGCPacing(tt.conf))
			assert.Equal(t, tt.wantLimit, debug.SetMemoryLimit(-1))
			assert.Equal(t, tt.wantGCPercent, debug.SetGCPercent(-1))
		})
	}
}

func TestReadMemory(t *testing.T) {
	restoreGCSettings(t)
	debug.SetGCPercent(-1)
	samples := newMemorySamples()

	debug.SetMemoryLimit(math.MaxInt64)
	assert.False(t, readMemory(samples).paced)
	debug.SetMemoryLimit(1 << 40)
	assert.True(t, readMemory(samples).paced)

	const size = 64 << 20
	retained := make([]byte, size)
	runtime.GC()
	unmarked := make([]byte, size)
	mem := readMemory(samples)
	runtime.KeepAlive(retained)
	runtime.KeepAlive(unmarked)

	assert.GreaterOrEqual(t, mem.liveHeap, uint64(size))
	assert.Greater(t, mem.heapObjects, mem.liveHeap+size/2)
	assert.Equal(t, mem.liveHeap, mem.used())
	mem.paced = false
	assert.Equal(t, mem.heapObjects, mem.used())
}

// Garbage that has not been collected yet must not count against the budget
// when GC is paced by a memory limit.
func TestCheckAllocIgnoresGarbageWhenPaced(t *testing.T) {
	restoreGCSettings(t)
	debug.SetMemoryLimit(1 << 40)
	debug.SetGCPercent(-1)

	const garbage = 64 << 20
	runtime.GC()
	maxAlloc := readMemory(newMemorySamples()).liveHeap + garbage/2
	sink := make([]byte, garbage)
	runtime.KeepAlive(sink)
	sink = nil

	c := newPacingTestCollector(t, config.MemorySize(maxAlloc), clockwork.NewFakeClock())
	c.checkAlloc(context.Background())

	got, ok := c.Metrics.Get(NUMERATOR_MEMORY_HEAP_ALLOC)
	require.True(t, ok)
	assert.Less(t, got, float64(maxAlloc))
	_, evicted := c.Metrics.Get("collector_cache_eviction")
	assert.False(t, evicted)

	// Unpaced, the same heap is over budget.
	debug.SetMemoryLimit(math.MaxInt64)
	c.checkAlloc(context.Background())
	_, evicted = c.Metrics.Get("collector_cache_eviction")
	assert.True(t, evicted)
}

// Over-budget live heap readings force a GC to refresh them, at most once a second.
func TestCheckAllocRefreshesLiveHeapAtBoundedRate(t *testing.T) {
	restoreGCSettings(t)
	debug.SetMemoryLimit(1 << 40)
	debug.SetGCPercent(-1)

	// The fake clock is never advanced, so the monitor goroutine never ticks.
	clock := clockwork.NewFakeClock()
	c := newPacingTestCollector(t, 1, clock)
	numGC := func() uint64 {
		var s debug.GCStats
		debug.ReadGCStats(&s)
		return uint64(s.NumGC)
	}

	// Each over-budget check evicts, which forces its own GC and restarts the interval.
	before := numGC()
	c.checkAlloc(context.Background())
	afterFirst := numGC()
	assert.Greater(t, afterFirst, before)

	// Within the interval only the eviction GC runs, not a refresh GC as well.
	c.checkAlloc(context.Background())
	withinInterval := numGC() - afterFirst

	c.lastGC = clock.Now().Add(-liveHeapRefreshInterval)
	before = numGC()
	c.checkAlloc(context.Background())
	pastInterval := numGC() - before

	assert.Greater(t, pastInterval, withinInterval, "a refresh GC should run once the interval has passed")
}

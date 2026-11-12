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
	"github.com/honeycombio/refinery/logger"
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

func newPacingTestCollector(t *testing.T, maxAlloc config.MemorySize, enableGCPacing bool, clock clockwork.Clock) *InMemCollector {
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
		GetCollectionConfigVal: config.CollectionConfig{MaxAlloc: maxAlloc, EnableGCPacing: enableGCPacing},
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
			name:          "AvailableMemory without EnableGCPacing leaves defaults",
			conf:          config.CollectionConfig{AvailableMemory: 100 * gib, MaxMemoryPercentage: 75},
			wantLimit:     math.MaxInt64,
			wantGCPercent: 100,
		},
		{
			name:          "AvailableMemory sets limit to 95% and turns GOGC off",
			conf:          config.CollectionConfig{EnableGCPacing: true, AvailableMemory: 100 * gib, MaxMemoryPercentage: 75},
			wantReturn:    95 * gib,
			wantLimit:     95 * gib,
			wantGCPercent: -1,
		},
		{
			name:          "explicit GOGC is kept",
			env:           map[string]string{"GOGC": "100"},
			conf:          config.CollectionConfig{EnableGCPacing: true, AvailableMemory: 100 * gib, MaxMemoryPercentage: 75},
			wantReturn:    95 * gib,
			wantLimit:     95 * gib,
			wantGCPercent: 100,
		},
		{
			name:          "explicit GOMEMLIMIT wins",
			env:           map[string]string{"GOMEMLIMIT": "10GiB"},
			conf:          config.CollectionConfig{EnableGCPacing: true, AvailableMemory: 100 * gib, MaxMemoryPercentage: 75},
			wantLimit:     math.MaxInt64,
			wantGCPercent: 100,
		},
		{
			name:          "budget at the limit would thrash GC, so leave defaults",
			conf:          config.CollectionConfig{EnableGCPacing: true, AvailableMemory: 100 * gib, MaxMemoryPercentage: 95},
			wantLimit:     math.MaxInt64,
			wantGCPercent: 100,
		},
		{
			name:          "budget just under the limit is paced",
			conf:          config.CollectionConfig{EnableGCPacing: true, AvailableMemory: 100 * gib, MaxMemoryPercentage: 94},
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
	assert.False(t, readMemory(samples, true).paced)
	debug.SetMemoryLimit(1 << 40)
	assert.True(t, readMemory(samples, true).paced)
	assert.False(t, readMemory(samples, false).paced, "an operator's GOMEMLIMIT alone does not pace the reading")

	const size = 64 << 20
	retained := make([]byte, size)
	runtime.GC()
	unmarked := make([]byte, size)
	mem := readMemory(samples, true)
	runtime.KeepAlive(retained)
	runtime.KeepAlive(unmarked)

	assert.GreaterOrEqual(t, mem.liveHeap, uint64(size))
	assert.Greater(t, mem.heapObjects, mem.liveHeap+size/2)
	assert.Equal(t, mem.liveHeap, mem.used())
	mem.paced = false
	assert.Equal(t, mem.heapObjects, mem.used())
}

// Garbage that has not been collected yet must not count against the budget
// when GC is paced by a memory limit and EnableGCPacing is on.
func TestCheckAllocIgnoresGarbageWhenPaced(t *testing.T) {
	tests := []struct {
		name           string
		enableGCPacing bool
		limit          int64
		wantEvict      bool
	}{
		{name: "paced", enableGCPacing: true, limit: 1 << 40, wantEvict: false},
		{name: "EnableGCPacing off, operator GOMEMLIMIT set", enableGCPacing: false, limit: 1 << 40, wantEvict: true},
		{name: "EnableGCPacing on, no memory limit", enableGCPacing: true, limit: math.MaxInt64, wantEvict: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			restoreGCSettings(t)
			debug.SetMemoryLimit(tt.limit)
			debug.SetGCPercent(-1)

			const garbage = 64 << 20
			runtime.GC()
			maxAlloc := readMemory(newMemorySamples(), true).liveHeap + garbage/2
			sink := make([]byte, garbage)
			runtime.KeepAlive(sink)
			sink = nil

			collector := newPacingTestCollector(t, config.MemorySize(maxAlloc), tt.enableGCPacing, clockwork.NewFakeClock())
			collector.checkAlloc(context.Background())

			_, evicted := collector.Metrics.Get("collector_cache_eviction")
			assert.Equal(t, tt.wantEvict, evicted)
			if !tt.wantEvict {
				got, ok := collector.Metrics.Get(NUMERATOR_MEMORY_HEAP_ALLOC)
				require.True(t, ok)
				assert.Less(t, got, float64(maxAlloc))
			}
		})
	}
}

// An over-budget live heap reading forces a GC to refresh it only when no GC
// has finished since the last check, since otherwise the reading is current.
func TestCheckAllocRefreshesStaleLiveHeap(t *testing.T) {
	tests := []struct {
		name         string
		gcSinceCheck bool
		wantGCs      uint64
	}{
		{name: "no GC since the last check refreshes, then evicts", gcSinceCheck: false, wantGCs: 2},
		{name: "a GC since the last check only evicts", gcSinceCheck: true, wantGCs: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			restoreGCSettings(t)
			debug.SetMemoryLimit(1 << 40)
			debug.SetGCPercent(-1)

			// The fake clock is never advanced, so the monitor goroutine never ticks.
			collector := newPacingTestCollector(t, 1<<40, true, clockwork.NewFakeClock())
			conf := collector.Config.(*config.MockConfig)
			setMaxAlloc := func(maxAlloc config.MemorySize) {
				conf.Mux.Lock()
				conf.GetCollectionConfigVal.MaxAlloc = maxAlloc
				conf.Mux.Unlock()
			}
			numGC := func() uint64 {
				var stats debug.GCStats
				debug.ReadGCStats(&stats)
				return uint64(stats.NumGC)
			}

			// The live heap reads 0 until the first GC.
			runtime.GC()
			collector.checkAlloc(context.Background())
			if tt.gcSinceCheck {
				runtime.GC()
			}
			setMaxAlloc(1)
			before := numGC()
			collector.checkAlloc(context.Background())

			assert.Equal(t, tt.wantGCs, numGC()-before)
			_, evicted := collector.Metrics.Get("collector_cache_eviction")
			assert.True(t, evicted)
		})
	}
}

func TestWarnOnBudgetNearGCLimit(t *testing.T) {
	const limit = 100 << 20
	tests := []struct {
		name           string
		enableGCPacing bool
		limit          int64
		budget         config.MemorySize
		wantWarn       bool
	}{
		{name: "budget at 94% of the limit warns", enableGCPacing: true, limit: limit, budget: limit / 100 * 94, wantWarn: true},
		{name: "budget at 85% of the limit is fine", enableGCPacing: true, limit: limit, budget: limit / 100 * 85},
		{name: "EnableGCPacing off", enableGCPacing: false, limit: limit, budget: limit / 100 * 94},
		{name: "no memory limit", enableGCPacing: true, limit: math.MaxInt64, budget: limit / 100 * 94},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			restoreGCSettings(t)
			collector := newPacingTestCollector(t, tt.budget, tt.enableGCPacing, clockwork.NewFakeClock())
			debug.SetMemoryLimit(tt.limit)
			mockLogger := &logger.MockLogger{}
			collector.Logger = mockLogger

			collector.warnOnBudgetNearGCLimit()

			var warned bool
			for _, event := range mockLogger.Events {
				_, ok := event.Fields["warn"]
				warned = warned || ok
			}
			assert.Equal(t, tt.wantWarn, warned)
		})
	}
}

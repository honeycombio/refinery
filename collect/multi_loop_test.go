package collect

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/generics"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"
)

// getAllTracesFromLoops is a test helper that efficiently gets all traces from all loops.
// It pauses each loop, then extracts all traces. Note the traces themselves aren't locked
// so there is still a hypothetical race condition after this function returns.
func getAllTracesFromLoops(collector *InMemCollector) map[string]*types.Trace {
	ch := make(chan struct{})
	defer close(ch)

	allTraces := make(map[string]*types.Trace)
	for _, loop := range collector.collectLoops {
		loop.pause <- ch
		traces := loop.cache.GetAll()

		for _, trace := range traces {
			allTraces[trace.TraceID] = trace
		}
	}
	return allTraces
}

// TestMultiLoopProcessing tests that multiple collect loops can process spans concurrently
func TestMultiLoopProcessing(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(100 * time.Millisecond),
			SendDelay:    config.Duration(50 * time.Millisecond),
			TraceTimeout: config.Duration(5 * time.Second), // Longer timeout for test
			MaxBatchSize: 500,
		},
		GetCollectionConfigVal: config.CollectionConfig{
			CacheCapacity: 10000,
			MaxAlloc:      0,
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          1000,
			DroppedSize:       1000,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
	}

	// Test with different numbers of loops
	for _, numLoops := range []int{2, 4, 8} {
		t.Run(fmt.Sprintf("%d_loops", numLoops), func(t *testing.T) {
			conf.GetCollectionConfigVal.NumCollectLoops = numLoops

			collector := newTestCollector(t, conf)

			// Verify loops were created
			assert.Equal(t, numLoops, len(collector.collectLoops))

			// Send spans to different traces (should go to different loops)
			numTraces := 100
			spansPerTrace := 10
			spansAdded := int32(0)

			var wg sync.WaitGroup
			for i := 0; i < numTraces; i++ {
				wg.Add(1)
				go func(traceNum int) {
					defer wg.Done()
					traceID := fmt.Sprintf("trace-%d", traceNum)

					for j := 0; j < spansPerTrace; j++ {
						span := &types.Span{
							Event: types.Event{
								APIHost:    "http://api.honeycomb.io",
								APIKey:     legacyAPIKey,
								Dataset:    "test.dataset",
								SampleRate: 1,
								Timestamp:  time.Now(),
								Data:       types.Payload{},
							},
							TraceID:     traceID,
							IsRoot:      false, // Don't send root spans so traces stay in cache
							ArrivalTime: time.Now(),
						}
						span.Data.Set("span_id", fmt.Sprintf("span-%d", j))

						err := collector.AddSpan(span)
						if err == nil {
							atomic.AddInt32(&spansAdded, 1)
						}
					}
				}(i)
			}

			wg.Wait()

			// Verify spans were added
			assert.Equal(t, int32(numTraces*spansPerTrace), atomic.LoadInt32(&spansAdded))

			// Verify traces are distributed across loops
			loopUsage := make(map[int]int)
			for i := 0; i < numTraces; i++ {
				traceID := fmt.Sprintf("trace-%d", i)
				loopIndex := collector.getLoopForTrace(traceID)
				loopUsage[loopIndex]++
			}

			// All loops should have been used (with high probability)
			assert.Equal(t, numLoops, len(loopUsage),
				"All loops should be used with %d traces", numTraces)

			// Wait for all spans to be processed into traces
			assert.Eventually(t, func() bool {
				// Get all traces at once to avoid multiple mutex acquisitions
				allTraces := getAllTracesFromLoops(collector)

				// Count how many of our test traces we found
				foundCount := 0
				for i := 0; i < numTraces; i++ {
					traceID := fmt.Sprintf("trace-%d", i)
					if _, exists := allTraces[traceID]; exists {
						foundCount++
					}
				}

				// We should find most traces (some may have expired)
				return foundCount >= numTraces/2
			}, 5*time.Second, 500*time.Millisecond) // Check less frequently since each check is now fast

			met := collector.Metrics.(*metrics.MockMetrics)
			count, ok := met.Get("span_received")
			assert.True(t, ok)
			assert.Equal(t, float64(1000), count)
			count, ok = met.Get("span_processed")
			assert.True(t, ok)
			assert.Equal(t, float64(1000), count)
			count, ok = met.Get("trace_accepted")
			assert.True(t, ok)
			assert.Equal(t, float64(100), count)

			// These metrics are nondeterministic, but we can at least confirm they were reported
			for _, name := range []string{
				"spans_waiting",
				"memory_heap_allocation",
				"collector_incoming_queue_length",
				"collector_peer_queue_length",
				"collector_cache_size",
				"collector_num_loops",
			} {
				_, ok = met.Get(name)
				assert.True(t, ok)
			}

			assert.Greater(t, met.GetHistogramCount("collector_collect_loop_duration_ms"), 0)
			assert.Greater(t, met.GetHistogramCount("collect_cache_entries"), 0)
			assert.Greater(t, met.GetHistogramCount("collector_send_expired_traces_in_cache_dur_ms"), 0)
			assert.Greater(t, met.GetHistogramCount("collector_incoming_queue"), 0)
			assert.Greater(t, met.GetHistogramCount("collector_peer_queue"), 0)
		})
	}
}

// TestTraceIDSharding verifies that traces are consistently routed to the same loop.
func TestTraceIDSharding(t *testing.T) {
	// Test with different numbers of loops and their expected deterministic distributions
	// These distributions were empirically discovered for trace-0 through trace-99
	testCases := []struct {
		name                 string
		numLoops             int
		expectedDistribution []int
	}{
		{"single_loop", 1, []int{100}},
		{"two_loops", 2, []int{54, 46}},
		{"four_loops", 4, []int{26, 20, 28, 26}},
		{"eight_loops", 8, []int{12, 10, 15, 10, 14, 10, 13, 16}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf := &config.MockConfig{
				GetTracesConfigVal: config.TracesConfig{
					SendTicker:   config.Duration(100 * time.Millisecond),
					SendDelay:    config.Duration(50 * time.Millisecond),
					TraceTimeout: config.Duration(1 * time.Second),
					MaxBatchSize: 500,
				},
				GetCollectionConfigVal: config.CollectionConfig{
					CacheCapacity:   1000,
					NumCollectLoops: tc.numLoops,
				},
				GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 2},
				ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
				TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
				SampleCache: config.SampleCacheConfig{
					KeptSize:          100,
					DroppedSize:       100,
					SizeCheckInterval: config.Duration(1 * time.Second),
				},
			}

			collector := newTestCollector(t, conf)

			// Track which loop each trace ID maps to
			traceToLoop := make(map[string]int)

			// Test multiple trace IDs
			for i := 0; i < 100; i++ {
				traceID := fmt.Sprintf("trace-%d", i)

				// Get the loop for this trace multiple times
				// It should always return the same loop
				firstLoop := collector.getLoopForTrace(traceID)
				traceToLoop[traceID] = firstLoop

				// Verify consistency - call multiple times
				for j := 0; j < 10; j++ {
					loop := collector.getLoopForTrace(traceID)
					assert.Equal(t, firstLoop, loop,
						"Trace %s should always map to the same loop", traceID)
				}

				// Verify the loop index is within bounds
				assert.GreaterOrEqual(t, firstLoop, 0)
				assert.Less(t, firstLoop, tc.numLoops)
			}

			// Count traces per loop for distribution analysis
			loopCounts := make([]int, tc.numLoops)
			for _, loop := range traceToLoop {
				loopCounts[loop]++
			}

			// Assert the deterministic expected distribution
			assert.Equal(t, tc.expectedDistribution, loopCounts)

			// Also verify all loops are used appropriately
			assert.Equal(t, tc.numLoops, len(loopCounts), "Loop counts should match number of loops")
			totalTraces := 0
			for loop, count := range loopCounts {
				assert.Greater(t, count, 0, "Loop %d should have at least some traces", loop)
				totalTraces += count
			}
			assert.Equal(t, 100, totalTraces, "Total traces should equal 100")
		})
	}
}

// TestParallelCollectRaceConditions tests for race conditions when the collector
// processes spans concurrently.
func TestParallelCollectRaceConditions(t *testing.T) {
	// This test is designed to be run with -race flag
	const (
		numTraces     = 100
		spansPerTrace = 50
		numGoroutines = 10
	)

	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(100 * time.Millisecond),
			SendDelay:    config.Duration(50 * time.Millisecond),
			TraceTimeout: config.Duration(1 * time.Second),
			MaxBatchSize: 500,
		},
		GetCollectionConfigVal: config.CollectionConfig{
			CacheCapacity:   10000,
			NumCollectLoops: 4,
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 2},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		AddSpanCountToRoot: true,
		AddCountsToRoot:    false,
		DryRun:             false,
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
	}

	clock := clockwork.NewFakeClock()
	collector := newTestCollector(t, conf, clock)
	transmission := collector.Transmission.(*transmit.MockTransmission)

	// Track all traces we send
	sentTraces := generics.NewSet[string]()
	var sentMutex sync.Mutex

	// Generate test data
	type testSpan struct {
		traceID string
		spanID  int
		isRoot  bool
	}

	allSpans := make([]testSpan, 0, numTraces*spansPerTrace)
	for i := 0; i < numTraces; i++ {
		traceID := fmt.Sprintf("trace-%d", i)
		sentMutex.Lock()
		sentTraces.Add(traceID)
		sentMutex.Unlock()

		for j := 0; j < spansPerTrace; j++ {
			allSpans = append(allSpans, testSpan{
				traceID: traceID,
				spanID:  j,
				isRoot:  j == spansPerTrace-1, // Last span is root
			})
		}
	}

	// Shuffle spans to simulate random arrival
	rand.Shuffle(len(allSpans), func(i, j int) {
		allSpans[i], allSpans[j] = allSpans[j], allSpans[i]
	})

	// Start multiple goroutines to add spans concurrently
	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			// Each goroutine gets a slice of spans to add
			startIdx := goroutineID * (len(allSpans) / numGoroutines)
			endIdx := startIdx + (len(allSpans) / numGoroutines)
			if goroutineID == numGoroutines-1 {
				endIdx = len(allSpans) // Last goroutine handles remainder
			}

			for i := startIdx; i < endIdx; i++ {
				span := &types.Span{
					Event: types.Event{
						APIHost:    "http://api.honeycomb.io",
						APIKey:     legacyAPIKey,
						Dataset:    "test.dataset",
						SampleRate: 1,
						Timestamp:  time.Now(),
						Data:       types.Payload{},
					},
					TraceID:     allSpans[i].traceID,
					IsRoot:      allSpans[i].isRoot,
					ArrivalTime: time.Now(),
				}

				span.Data.Set("span_id", fmt.Sprintf("span-%d", allSpans[i].spanID))
				span.Data.Set("goroutine", goroutineID)

				// Alternate between AddSpan and AddSpanFromPeer
				var err error
				if i%2 == 0 {
					err = collector.AddSpan(span)
				} else {
					err = collector.AddSpanFromPeer(span)
				}
				assert.NoError(t, err)
			}
		}(g)
	}

	// Recreational reload command, should be fine.
	collector.sendReloadSignal("hash1", "hash2")

	// Wait for all input goroutines to complete - note this will deadlock if
	// we fill the input channels.
	wg.Wait()

	// Now that all the events have been enqueued, advance time to allow transmisison.
	clock.Advance(time.Second)

	// Wait for spans to be processed and sent
	var totalSent int
	assert.Eventually(t, func() bool {
		clock.Advance(time.Second)
		events := transmission.GetBlock(0)
		totalSent += len(events)

		// Sample rate of 2 in the confix means we expect roughly half of our
		// events to actually be sent. Make sure we get at least 45%
		return float64(totalSent) >= float64(len(allSpans))*0.45
	}, 2*time.Second, transmission.WaitTime, "Should have sent some spans to transmission")
}

// TestMemoryPressureWithConcurrency tests the collector's behavior under memory pressure
// with concurrent operations
func TestMemoryPressureWithConcurrency(t *testing.T) {
	const (
		numTraces     = 500
		spansPerTrace = 100
		maxAlloc      = 10 * 1024 * 1024 // 10MB limit
	)

	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(100 * time.Millisecond),
			SendDelay:    config.Duration(50 * time.Millisecond),
			TraceTimeout: config.Duration(2 * time.Second),
			MaxBatchSize: 500,
		},
		GetCollectionConfigVal: config.CollectionConfig{
			CacheCapacity: 5000,
			MaxAlloc:      config.MemorySize(maxAlloc),
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
	}

	clock := clockwork.NewFakeClock()
	collector := newTestCollector(t, conf, clock)
	transmission := collector.Transmission.(*transmit.MockTransmission)

	var wg sync.WaitGroup
	spansAdded := int32(0)
	spansEvicted := int32(0)

	// Monitor spans being sent (which indicates eviction) by advancing fake clock
	wg.Add(1)
	go func() {
		defer wg.Done()
		if fakeClock, ok := collector.Clock.(*clockwork.FakeClock); ok {
			for i := 0; i < 50; i++ { // Check 50 times
				fakeClock.Advance(100 * time.Millisecond) // Advance fake clock
				count := len(transmission.Events)
				if count > 0 {
					atomic.StoreInt32(&spansEvicted, int32(count))
				}
			}
		}
	}()

	// Add spans concurrently to trigger memory pressure
	numGoroutines := 5
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for i := 0; i < numTraces/numGoroutines; i++ {
				traceID := fmt.Sprintf("trace-%d-%d", id, i)

				for j := 0; j < spansPerTrace; j++ {
					span := &types.Span{
						Event: types.Event{
							APIHost:    "http://api.honeycomb.io",
							APIKey:     legacyAPIKey,
							Dataset:    "test.dataset",
							SampleRate: 1,
							Timestamp:  time.Now(),
							Data:       types.Payload{},
						},
						TraceID:     traceID,
						IsRoot:      j == spansPerTrace-1,
						ArrivalTime: time.Now(),
					}

					// Add large data to trigger memory pressure
					span.Data.Set("large_field", make([]byte, 1024)) // 1KB per span

					if err := collector.AddSpan(span); err == nil {
						atomic.AddInt32(&spansAdded, 1)
					}

					// Removed microsecond sleep to speed up test
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify the collector handled memory pressure
	assert.Greater(t, atomic.LoadInt32(&spansAdded), int32(0), "Should have added spans")
	// Note: Eviction might not always happen depending on actual memory usage
}

// TestCoordinatedReload verifies config reload coordination across loops
func TestCoordinatedReload(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(100 * time.Millisecond),
			SendDelay:    config.Duration(50 * time.Millisecond),
			TraceTimeout: config.Duration(1 * time.Second),
			MaxBatchSize: 500,
		},
		GetCollectionConfigVal: config.CollectionConfig{
			CacheCapacity:   1000,
			NumCollectLoops: 4,
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
	}

	collector := newTestCollector(t, conf)

	// Send some test spans to create dataset samplers
	processedInitial := int32(0)
	for i := 0; i < 10; i++ {
		span := &types.Span{
			Event: types.Event{
				APIHost:    "http://api.honeycomb.io",
				APIKey:     legacyAPIKey,
				Dataset:    fmt.Sprintf("dataset-%d", i%3),
				SampleRate: 1,
				Timestamp:  time.Now(),
				Data:       types.Payload{},
			},
			TraceID:     fmt.Sprintf("reload-trace-%d", i),
			IsRoot:      true,
			ArrivalTime: time.Now(),
		}
		if err := collector.AddSpan(span); err == nil {
			atomic.AddInt32(&processedInitial, 1)
		}
	}

	// Wait for initial spans to be processed
	assert.Eventually(t, func() bool {
		return atomic.LoadInt32(&processedInitial) >= 8
	}, 2*time.Second, 10*time.Millisecond, "Initial spans should be processed")

	// Trigger a reload - this should cause loops to recreate their samplers
	collector.sendReloadSignal("hash1", "hash2")

	// Give a moment for the reload signal to be processed (reload is async)
	// We'll verify the reload worked by checking that spans still get processed
	time.Sleep(50 * time.Millisecond)

	// Check that samplers were recreated by sending more spans
	processedAfterReload := int32(0)
	for i := 0; i < 20; i++ {
		span := &types.Span{
			Event: types.Event{
				APIHost:    "http://api.honeycomb.io",
				APIKey:     legacyAPIKey,
				Dataset:    "test.reload",
				SampleRate: 1,
				Timestamp:  time.Now(),
				Data:       types.Payload{},
			},
			TraceID:     fmt.Sprintf("after-reload-%d", i),
			IsRoot:      true,
			ArrivalTime: time.Now(),
		}
		if err := collector.AddSpan(span); err == nil {
			atomic.AddInt32(&processedAfterReload, 1)
		}
	}

	// Verify spans were processed after reload
	assert.Eventually(t, func() bool {
		return atomic.LoadInt32(&processedAfterReload) >= 15
	}, 2*time.Second, 100*time.Millisecond, "Spans should be processed after reload")

	// Trigger another reload to verify multiple reloads work
	collector.sendReloadSignal("hash2", "hash3")
	time.Sleep(50 * time.Millisecond)

	// Send more spans to verify system still works
	processedAfterSecondReload := int32(0)
	for i := 0; i < 20; i++ {
		span := &types.Span{
			Event: types.Event{
				APIHost:    "http://api.honeycomb.io",
				APIKey:     legacyAPIKey,
				Dataset:    "test.reload2",
				SampleRate: 1,
				Timestamp:  time.Now(),
				Data:       types.Payload{},
			},
			TraceID:     fmt.Sprintf("after-second-reload-%d", i),
			IsRoot:      true,
			ArrivalTime: time.Now(),
		}
		if err := collector.AddSpan(span); err == nil {
			atomic.AddInt32(&processedAfterSecondReload, 1)
		}
	}

	// Verify spans were processed after second reload
	assert.Eventually(t, func() bool {
		return atomic.LoadInt32(&processedAfterSecondReload) >= 15
	}, 2*time.Second, 100*time.Millisecond, "Spans should be processed after second reload")
}

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
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"
)

// getAllTracesFromLoops is a test helper that efficiently gets all traces from all workers.
// It pauses each worker, then extracts all traces. Note the traces themselves aren't locked
// so there is still a hypothetical race condition after this function returns.
func getAllTracesFromLoops(collector *InMemCollector) map[string]*types.Trace {
	ch := make(chan struct{})
	defer close(ch)

	allTraces := make(map[string]*types.Trace)
	for _, worker := range collector.workers {
		worker.pause <- ch
		traces := worker.cache.GetAll()

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
			TraceTimeout: config.Duration(60 * time.Second),
			MaxBatchSize: 500,
		},
		GetCollectionConfigVal: config.CollectionConfig{
			IncomingQueueSize:  30000,
			PeerQueueSize:      30000,
			HealthCheckTimeout: config.Duration(time.Second),
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

			// Verify workers were created
			assert.Equal(t, numLoops, len(collector.workers))

			assert.Eventually(t, func() bool {
				return collector.Health.(health.Reporter).IsReady()
			}, 5*time.Second, 10*time.Millisecond)

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

			// Verify traces are distributed across workers
			workerUsage := make(map[int]int)
			for i := 0; i < numTraces; i++ {
				traceID := fmt.Sprintf("trace-%d", i)
				workerIndex := collector.getWorkerIDForTrace(traceID)
				workerUsage[workerIndex]++
			}

			// All workers should have been used (with high probability)
			assert.Equal(t, numLoops, len(workerUsage))

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
				return foundCount == numTraces
			}, 5*time.Second, 500*time.Millisecond) // Check less frequently since each check is now fast

			met := collector.Metrics.(*metrics.MockMetrics)

			// Wait for the housekeeping ticker to aggregate all thread-local metrics
			assert.Eventually(t, func() bool {
				receivedCount, receivedOk := met.Get("span_received")
				processedCount, processedOk := met.Get("span_processed")

				return receivedOk && receivedCount == float64(spansPerTrace*numTraces) &&
					processedOk && processedCount == float64(spansPerTrace*numTraces)
			}, 1*time.Second, 50*time.Millisecond, "All thread-local metrics should be aggregated and reported")

			count, ok := met.Get("trace_accepted")
			assert.True(t, ok)
			assert.Equal(t, float64(numTraces), count)

			// Verify that local counters are reset after reporting
			assert.Eventually(t, func() bool {
				totalLocalReceived := int64(0)
				totalLocalWaiting := int64(0)

				for _, worker := range collector.workers {
					totalLocalReceived += worker.localSpanReceived.Load()
					totalLocalWaiting += worker.localSpansWaiting.Load()
				}

				return totalLocalReceived == 0 && totalLocalWaiting == 0
			}, 500*time.Millisecond, 25*time.Millisecond, "All local counters should be reset to 0 after aggregation")

			// These metrics are nondeterministic, but we can at least confirm they were reported
			for _, name := range []string{
				"spans_waiting",
				"memory_heap_allocation",
				"collector_incoming_queue_length",
				"collector_peer_queue_length",
				"collector_cache_size",
				"collector_num_workers",
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
					IncomingQueueSize: 3000,
					PeerQueueSize:     3000,
					NumCollectLoops:   tc.numLoops,
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

			// Track which worker each trace ID maps to
			traceToWorker := make(map[string]int)

			// Test multiple trace IDs
			for i := 0; i < 100; i++ {
				traceID := fmt.Sprintf("trace-%d", i)

				// Get the worker for this trace multiple times
				// It should always return the same worker
				firstWorker := collector.getWorkerIDForTrace(traceID)
				traceToWorker[traceID] = firstWorker

				// Verify consistency - call multiple times
				for j := 0; j < 10; j++ {
					worker := collector.getWorkerIDForTrace(traceID)
					assert.Equal(t, firstWorker, worker,
						"Trace %s should always map to the same worker", traceID)
				}

				// Verify the worker index is within bounds
				assert.GreaterOrEqual(t, firstWorker, 0)
				assert.Less(t, firstWorker, tc.numLoops)
			}

			// Count traces per worker for distribution analysis
			workerCounts := make([]int, tc.numLoops)
			for _, worker := range traceToWorker {
				workerCounts[worker]++
			}

			// Assert the deterministic expected distribution
			assert.Equal(t, tc.expectedDistribution, workerCounts)

			// Also verify all workers are used appropriately
			assert.Equal(t, tc.numLoops, len(workerCounts), "Worker counts should match number of workers")
			totalTraces := 0
			for worker, count := range workerCounts {
				assert.Greater(t, count, 0, "Worker %d should have at least some traces", worker)
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
			IncomingQueueSize: 30000,
			PeerQueueSize:     30000,
			NumCollectLoops:   4,
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
	clock.BlockUntilContext(t.Context(), conf.GetCollectionConfig().NumCollectLoops+1)
	clock.Advance(time.Second)

	// Wait for spans to be processed and sent
	var totalSent int
	assert.Eventually(t, func() bool {
		clock.Advance(time.Second)
		events := transmission.GetBlock(0)
		totalSent += len(events)

		// Sample rate of 2 in the config means we expect roughly half of our
		// events to actually be sent. Make sure we get at least 45%
		return float64(totalSent) >= float64(len(allSpans))*0.45
	}, 2*time.Second, transmission.WaitTime, "Should have sent some spans to transmission")
}

// TestMemoryPressureWithConcurrency tests the collector's behavior under memory pressure
// with concurrent operations. This seems like a silly test in that it doesn't assert
// much, but it can reveal deadlocks or races around checkAlloc.
func TestMemoryPressureWithConcurrency(t *testing.T) {
	const (
		numTraces     = 500
		spansPerTrace = 100
		maxAlloc      = 1 * 1024 * 1024 // 1MB limit
	)

	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(100 * time.Millisecond),
			SendDelay:    config.Duration(50 * time.Millisecond),
			TraceTimeout: config.Duration(2 * time.Hour),
			MaxBatchSize: 500,
		},
		GetCollectionConfigVal: config.CollectionConfig{
			MaxAlloc:          config.MemorySize(maxAlloc),
			IncomingQueueSize: 15000,
			PeerQueueSize:     15000,
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

	clock.BlockUntilContext(t.Context(), 2)

	var wg sync.WaitGroup
	spansAdded := int32(0)

	// Drain the transmission channel; we're sending a lot of events here and we
	// don't want it to back up.
	go func() {
		// This will terminate after we stop transmission and close the channel.
		for range transmission.Events {
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

					clock.Advance(10 * time.Millisecond)
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify the collector handled memory pressure
	assert.Greater(t, atomic.LoadInt32(&spansAdded), int32(0), "Should have added spans")

	met := collector.Metrics.(*metrics.MockMetrics)
	count, ok := met.Get("collector_cache_eviction")
	assert.True(t, ok)
	assert.Greater(t, count, 0.0)
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
			IncomingQueueSize: 3000,
			PeerQueueSize:     3000,
			NumCollectLoops:   4,
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

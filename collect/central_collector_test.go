package collect

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/centralstore"
	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/collect/stressRelief"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/generics"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/redis"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const legacyAPIKey = "c9945edf5d245834089a1bd6cc9ad01e"

// var storeTypes = []string{"local", "redis"}
var storeTypes = []string{"redis"}

func TestCentralCollector_AddSpan(t *testing.T) {
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			conf := &config.MockConfig{
				GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
				SendTickerVal:      2 * time.Millisecond,
				ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
				GetCollectionConfigVal: config.CollectionConfig{
					CacheCapacity: 3,
				},
			}
			coll := &CentralCollector{
				Clock: clockwork.NewFakeClock(),
			}
			stop := startCollector(t, conf, coll, storeType)
			defer stop()

			coll.processorCycle.Pause()

			var traceID1 = "mytrace"

			span := &types.Span{
				TraceID: traceID1,
				ID:      "456",
				Event: types.Event{
					Dataset: "aoeu",
					APIKey:  legacyAPIKey,
					Data: map[string]interface{}{
						"trace.parent_id": "123",
					},
				},
			}
			require.NoError(t, coll.AddSpan(span))

			// adding one child span should
			// * create the trace in the cache
			// * send the trace to the central store
			require.Eventually(t, func() bool {
				trace := coll.SpanCache.Get(traceID1)
				return trace != nil
			}, 5*time.Second, 500*time.Millisecond)

			ctx := context.Background()
			trace, err := coll.Store.GetTrace(ctx, traceID1)
			require.NoError(t, err)
			assert.Equal(t, traceID1, trace.TraceID)
			assert.Len(t, trace.Spans, 1)
			assert.Nil(t, trace.Root)

			root := &types.Span{
				TraceID: traceID1,
				ID:      "123",
				IsRoot:  true,
				Event: types.Event{
					Dataset: "aoeu",
					APIKey:  legacyAPIKey,
				},
			}
			require.NoError(t, coll.AddSpan(root))

			// adding root span should send the trace to the central store
			require.Eventually(t, func() bool {
				trace := coll.SpanCache.Get(traceID1)
				return trace.RootSpan != nil
			}, 5*time.Second, 500*time.Millisecond)
			trace, err = coll.Store.GetTrace(ctx, traceID1)
			require.NoError(t, err)
			assert.Equal(t, traceID1, trace.TraceID)
			assert.Len(t, trace.Spans, 2)
			assert.NotNil(t, trace.Root)
		})
	}
}

func TestCentralCollector_ProcessTraces(t *testing.T) {
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			conf := &config.MockConfig{
				GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
				SendTickerVal:      2 * time.Millisecond,
				ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
				GetCollectionConfigVal: config.CollectionConfig{
					CacheCapacity:              100,
					ProcessTracesPauseDuration: config.Duration(1 * time.Second),
					DeciderPauseDuration:       config.Duration(1 * time.Second),
				},
			}
			transmission := &transmit.MockTransmission{}

			collector := &CentralCollector{
				Transmission: transmission,
			}
			stop := startCollector(t, conf, collector, storeType)
			defer stop()

			collector.processorCycle.Pause()
			collector.deciderCycle.Pause()

			numberOfTraces := 10
			traceids := make([]string, 0, numberOfTraces)
			for tr := 0; tr < numberOfTraces; tr++ {
				tid := fmt.Sprintf("trace%02d", tr)
				traceids = append(traceids, tid)
				// write 9 child spans to the store
				for s := 1; s < 10; s++ {
					span := &types.Span{
						TraceID: tid,
						ID:      fmt.Sprintf("span%d", s),
						Event: types.Event{
							Dataset:     "aoeu",
							Environment: "test",
							Data: map[string]interface{}{
								"trace.parent_id": fmt.Sprintf("span%d", s-1),
							},
						},
					}
					require.NoError(t, collector.AddSpan(span))
				}
				// now write the root span
				span := &types.Span{
					TraceID: tid,
					ID:      "span0",
					IsRoot:  true,
				}
				require.NoError(t, collector.AddSpan(span))
			}

			// wait for all traces to be processed
			waitUntilReadyToDecide(t, collector, traceids)

			collector.deciderCycle.RunOnce()

			collector.processorCycle.RunOnce()

			count, ok := collector.Metrics.Get("trace_send_kept")
			require.True(t, ok)
			assert.Equal(t, float64(numberOfTraces), count)
		})
	}
}

func TestCentralCollector_Decider(t *testing.T) {
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			conf := &config.MockConfig{
				GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
				SendTickerVal:      2 * time.Millisecond,
				ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
				GetCollectionConfigVal: config.CollectionConfig{
					IncomingQueueSize:          100,
					ProcessTracesPauseDuration: config.Duration(1 * time.Second),
					DeciderPauseDuration:       config.Duration(1 * time.Second),
				},
			}
			transmission := &transmit.MockTransmission{}

			collector := &CentralCollector{
				Transmission: transmission,
			}
			stop := startCollector(t, conf, collector, storeType)
			defer stop()
			collector.deciderCycle.Pause()

			numberOfTraces := 10
			traceids := make([]string, 0, numberOfTraces)
			for tr := 0; tr < numberOfTraces; tr++ {
				tid := fmt.Sprintf("trace%02d", tr)
				traceids = append(traceids, tid)
				// write 9 child spans to the store
				for s := 1; s < 10; s++ {
					span := &types.Span{
						TraceID: tid,
						ID:      fmt.Sprintf("span%d", s),
						Event: types.Event{
							Dataset:     "aoeu",
							Environment: "test",
							Data: map[string]interface{}{
								"trace.parent_id": fmt.Sprintf("span%d", s-1),
							},
						},
					}
					err := collector.AddSpan(span)
					require.NoError(t, err)
				}
				// now write the root span
				span := &types.Span{
					TraceID: tid,
					ID:      "span0",
					IsRoot:  true,
				}
				err := collector.AddSpan(span)
				require.NoError(t, err)
			}

			waitUntilReadyToDecide(t, collector, traceids)

			ctx := context.Background()
			collector.deciderCycle.RunOnce()
			traces, err := collector.Store.GetStatusForTraces(ctx, traceids)
			require.NoError(t, err)
			require.Equal(t, numberOfTraces, len(traces))
			for _, trace := range traces {
				assert.Equal(t, centralstore.DecisionKeep, trace.State)
			}
		})
	}
}

func TestCentralCollector_OriginalSampleRateIsNotedInMetaField(t *testing.T) {
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			// The sample rate applied by Refinery in this test's config.
			const expectedDeterministicSampleRate = int(2)
			// The sample rate happening upstream of Refinery.
			const originalSampleRate = uint(50)

			conf := &config.MockConfig{
				GetSendDelayVal:    0,
				GetTraceTimeoutVal: 60 * time.Second,
				GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: expectedDeterministicSampleRate},
				SendTickerVal:      2 * time.Millisecond,
				ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
				GetCollectionConfigVal: config.CollectionConfig{
					IncomingQueueSize:          10000,
					DeciderPauseDuration:       config.Duration(1 * time.Second),
					ProcessTracesPauseDuration: config.Duration(1 * time.Second),
				},
				SampleCache: config.SampleCacheConfig{
					KeptSize:          100,
					DroppedSize:       100,
					SizeCheckInterval: config.Duration(1 * time.Second),
				},
			}
			transmission := &transmit.MockTransmission{}
			collector := &CentralCollector{
				Transmission: transmission,
			}
			stop := startCollector(t, conf, collector, storeType)
			defer stop()

			collector.deciderCycle.Pause()
			collector.processorCycle.Pause()

			// Generate events until one is sampled and appears on the transmission queue for sending.
			traceIDs := make([]string, 0, 10)
			for i := 0; i < 10; i++ {
				span := &types.Span{
					TraceID: fmt.Sprintf("trace-%v", i),
					Event: types.Event{
						Dataset:    "aoeu",
						APIKey:     legacyAPIKey,
						SampleRate: originalSampleRate,
						Data:       make(map[string]interface{}),
					},
				}
				traceIDs = append(traceIDs, span.TraceID)
				require.NoError(t, collector.AddSpan(span))
			}
			waitUntilReadyToDecide(t, collector, traceIDs)
			collector.deciderCycle.RunOnce()

			waitForTraceDecision(t, collector, traceIDs)

			collector.processorCycle.RunOnce()

			transmission.Mux.RLock()
			require.Greater(t, len(transmission.Events), 0,
				"At least one event should have been sampled and transmitted by now for us to make assertions upon.")
			upstreamSampledEvent := transmission.Events[0]
			transmission.Mux.RUnlock()

			assert.Equal(t, originalSampleRate, upstreamSampledEvent.Data["meta.refinery.original_sample_rate"],
				"metadata should be populated with original sample rate")
			assert.Equal(t, originalSampleRate*uint(expectedDeterministicSampleRate), upstreamSampledEvent.SampleRate,
				"sample rate for the event should be the original sample rate multiplied by the deterministic sample rate")

			// Generate one more event with no upstream sampling applied.
			traceID := fmt.Sprintf("trace-%v", 1000)
			err := collector.AddSpan(&types.Span{
				TraceID: traceID,
				Event: types.Event{
					Dataset:    "no-upstream-sampling",
					APIKey:     legacyAPIKey,
					SampleRate: 0, // no upstream sampling
					Data:       make(map[string]interface{}),
				},
			})
			require.NoError(t, err, "must be able to add the span")
			waitUntilReadyToDecide(t, collector, []string{traceID})
			collector.deciderCycle.RunOnce()
			waitForTraceDecision(t, collector, []string{traceID})

			collector.processorCycle.RunOnce()
			// Find the Refinery-sampled-and-sent event that had no upstream sampling which
			// should be the last event on the transmission queue.
			var noUpstreamSampleRateEvent *types.Event
			transmission.Mux.RLock()
			noUpstreamSampleRateEvent = transmission.Events[len(transmission.Events)-1]
			require.Equal(t, "no-upstream-sampling", noUpstreamSampleRateEvent.Dataset)
			transmission.Mux.RUnlock()

			assert.Nil(t, noUpstreamSampleRateEvent.Data["meta.refinery.original_sample_rate"],
				"original sample rate should not be set in metadata when original sample rate is zero")
		})
	}
}

// HoneyComb treats a missing or 0 SampleRate the same as 1, but
// behaves better/more consistently if the SampleRate is explicitly
// set instead of inferred
func TestCentralCollector_TransmittedSpansShouldHaveASampleRateOfAtLeastOne(t *testing.T) {
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			conf := &config.MockConfig{
				GetSendDelayVal:    0,
				GetTraceTimeoutVal: 60 * time.Second,
				GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
				GetCollectionConfigVal: config.CollectionConfig{
					IncomingQueueSize:          100,
					ProcessTracesPauseDuration: config.Duration(1 * time.Second),
					DeciderPauseDuration:       config.Duration(1 * time.Second),
				},
				SendTickerVal:      2 * time.Millisecond,
				ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
				SampleCache: config.SampleCacheConfig{
					KeptSize:          100,
					DroppedSize:       100,
					SizeCheckInterval: config.Duration(1 * time.Second),
				},
			}
			transmission := &transmit.MockTransmission{}
			coll := &CentralCollector{
				Transmission: transmission,
			}
			stop := startCollector(t, conf, coll, storeType)
			defer stop()

			coll.deciderCycle.Pause()
			coll.processorCycle.Pause()

			span := &types.Span{
				TraceID: fmt.Sprintf("trace-%v", 1),
				Event: types.Event{
					Dataset:    "aoeu",
					APIKey:     legacyAPIKey,
					SampleRate: 0, // This should get lifted to 1
					Data:       make(map[string]interface{}),
				},
			}

			err := coll.AddSpan(span)
			require.NoError(t, err)
			waitUntilReadyToDecide(t, coll, []string{span.TraceID})
			coll.deciderCycle.RunOnce()
			waitForTraceDecision(t, coll, []string{span.TraceID})
			coll.processorCycle.RunOnce()

			require.Len(t, transmission.Events, 1)
			assert.Equal(t, uint(1), transmission.Events[0].SampleRate,
				"SampleRate should be reset to one after starting at zero")
		})
	}
}

func TestCentralCollector_SampleConfigReload(t *testing.T) {
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			conf := &config.MockConfig{
				GetSendDelayVal:    0,
				GetTraceTimeoutVal: 60 * time.Second,
				GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
				SendTickerVal:      2 * time.Millisecond,
				ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
				GetCollectionConfigVal: config.CollectionConfig{
					CacheCapacity:              10,
					ProcessTracesPauseDuration: config.Duration(1 * time.Second),
					DeciderPauseDuration:       config.Duration(1 * time.Second),
				},
				SampleCache: config.SampleCacheConfig{
					KeptSize:          100,
					DroppedSize:       100,
					SizeCheckInterval: config.Duration(1 * time.Second),
				},
			}

			transmission := &transmit.MockTransmission{}
			coll := &CentralCollector{
				Transmission: transmission,
			}
			stop := startCollector(t, conf, coll, storeType)
			defer stop()

			coll.deciderCycle.Pause()
			coll.processorCycle.Pause()

			dataset := "aoeu"

			span := &types.Span{
				TraceID: "1",
				ID:      "span1",
				Event: types.Event{
					Dataset: dataset,
					APIKey:  legacyAPIKey,
				},
			}

			err := coll.AddSpan(span)
			require.NoError(t, err)

			waitUntilReadyToDecide(t, coll, []string{span.TraceID})

			_, ok := coll.samplersByDestination[dataset]
			require.True(t, ok)

			conf.ReloadConfig()

			assert.Eventually(t, func() bool {
				coll.mut.RLock()
				defer coll.mut.RUnlock()

				_, ok := coll.samplersByDestination[dataset]
				return !ok
			}, 2*time.Second, 20*time.Millisecond)

			span = &types.Span{
				TraceID: "2",
				Event: types.Event{
					Dataset: dataset,
					APIKey:  legacyAPIKey,
				},
			}

			err = coll.AddSpan(span)
			require.NoError(t, err)
			waitUntilReadyToDecide(t, coll, []string{span.TraceID})

			_, ok = coll.samplersByDestination[dataset]
			require.True(t, ok)
		})
	}
}

func TestCentralCollector_StableMaxAlloc(t *testing.T) {
	t.Skip("This test is flaky in CI and should be fixed before re-enabling")

	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			conf := &config.MockConfig{
				GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
				SendTickerVal:      2 * time.Millisecond,
				ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
				SampleCache: config.SampleCacheConfig{
					KeptSize:          100,
					DroppedSize:       100,
					SizeCheckInterval: config.Duration(1 * time.Second),
				},
				GetCollectionConfigVal: config.CollectionConfig{
					IncomingQueueSize:          600,
					ProcessTracesBatchSize:     500,
					DeciderBatchSize:           200,
					ProcessTracesPauseDuration: config.Duration(1 * time.Second),
					DeciderPauseDuration:       config.Duration(1 * time.Second),
				},
				StoreOptions: config.SmartWrapperOptions{
					SpanChannelSize: 500,
					DecisionTimeout: config.Duration(1 * time.Minute),
					StateTicker:     config.Duration(5 * time.Millisecond),
					SendDelay:       config.Duration(1 * time.Millisecond),
					TraceTimeout:    config.Duration(1 * time.Millisecond),
				},
			}

			transmission := &transmit.MockTransmission{}
			coll := &CentralCollector{
				Transmission: transmission,
			}
			stop := startCollector(t, conf, coll, storeType)
			defer stop()

			coll.deciderCycle.Pause()
			coll.processorCycle.Pause()

			totalTraceCount := 300
			spandata := make([]map[string]interface{}, totalTraceCount)
			for i := 0; i < totalTraceCount; i++ {
				spandata[i] = map[string]interface{}{
					"trace.parent_id": "unused",
					"id":              i,
					"str1":            strings.Repeat("abc", rand.Intn(100)+1),
					"str2":            strings.Repeat("def", rand.Intn(100)+1),
				}
			}

			toRemoveTraceCount := 200
			var memorySize uint64
			traceIDs := make([]string, 0, totalTraceCount)
			for i := 0; i < totalTraceCount; i++ {
				span := &types.Span{
					TraceID: strconv.Itoa(i),
					ID:      fmt.Sprintf("span%d", i),
					Event: types.Event{
						Dataset: "aoeu",
						Data:    spandata[i],
						APIKey:  legacyAPIKey,
					},
				}

				require.NoError(t, coll.AddSpan(span))
				if i < toRemoveTraceCount {
					memorySize += uint64(span.GetDataSize())
				}
				traceIDs = append(traceIDs, span.TraceID)
			}

			waitUntilReadyToDecide(t, coll, traceIDs)

			// Now there should be 300 traces in the cache.
			assert.Equal(t, totalTraceCount, coll.SpanCache.Len())

			// We want to induce an eviction event, so set MaxAlloc a bit below
			// our current post-GC alloc.
			runtime.GC()
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			// Set MaxAlloc, which should cause cache evictions.
			conf.Mux.Lock()
			conf.GetCollectionConfigVal.MaxAlloc = config.MemorySize(mem.Alloc - memorySize)
			conf.Mux.Unlock()

			// wait for the cache to take some action
			var numOfTracesInCache int
			for {
				time.Sleep(20 * time.Millisecond)
				coll.deciderCycle.RunOnce()
				coll.processorCycle.RunOnce()

				numOfTracesInCache = coll.SpanCache.Len()
				if numOfTracesInCache <= toRemoveTraceCount {
					break
				}
			}

			assert.Less(t, numOfTracesInCache, 280, "should have sent some traces")
			assert.Greater(t, numOfTracesInCache, 99, "should have NOT sent some traces")

			// We discarded the most costly spans, and sent them.
			transmission.Mux.Lock()
			assert.GreaterOrEqual(t, toRemoveTraceCount, len(transmission.Events), "should have sent traces that weren't kept")

			transmission.Mux.Unlock()
		})
	}
}

func TestCentralCollector_AddSpanNoBlock(t *testing.T) {
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			conf := &config.MockConfig{
				GetSendDelayVal:    0,
				GetTraceTimeoutVal: 10 * time.Minute,
				GetSamplerTypeVal:  &config.DeterministicSamplerConfig{},
				SendTickerVal:      2 * time.Millisecond,
				ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
				SampleCache: config.SampleCacheConfig{
					KeptSize:          100,
					DroppedSize:       100,
					SizeCheckInterval: config.Duration(1 * time.Second),
				},
				GetCollectionConfigVal: config.CollectionConfig{
					IncomingQueueSize:          3,
					DeciderPauseDuration:       config.Duration(1 * time.Second),
					ProcessTracesPauseDuration: config.Duration(1 * time.Second),
				},
			}

			transmission := &transmit.MockTransmission{}
			coll := &CentralCollector{
				Transmission:   transmission,
				blockOnCollect: true,
			}
			stop := startCollector(t, conf, coll, storeType)
			defer stop()

			span := &types.Span{
				TraceID: "1",
				Event: types.Event{
					Dataset: "aoeu",
					APIKey:  legacyAPIKey,
				},
			}

			for i := 0; i < 3; i++ {
				err := coll.AddSpan(span)
				assert.NoError(t, err)
			}

			err := coll.AddSpan(span)
			assert.Error(t, err)
		})
	}
}

// TestAddCountsToRoot tests that adding a root span winds up with a trace object in
// the cache and that that trace gets span count, span event count, span link count, and event count added to it
// This test also makes sure that AddCountsToRoot overrides the AddSpanCountToRoot config.
func TestCentralCollector_AddCountsToRoot(t *testing.T) {
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			conf := &config.MockConfig{
				GetSendDelayVal:    10 * time.Millisecond,
				GetTraceTimeoutVal: 60 * time.Second,
				GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
				SendTickerVal:      60 * time.Second,
				ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
				SampleCache: config.SampleCacheConfig{
					KeptSize:          100,
					DroppedSize:       100,
					SizeCheckInterval: config.Duration(1 * time.Second),
				},
				GetCollectionConfigVal: config.CollectionConfig{
					IncomingQueueSize:          100,
					ProcessTracesPauseDuration: config.Duration(1 * time.Second),
					DeciderPauseDuration:       config.Duration(1 * time.Second),
				},
			}

			transmission := &transmit.MockTransmission{}
			coll := &CentralCollector{
				Transmission: transmission,
			}
			stop := startCollector(t, conf, coll, storeType)
			defer stop()

			coll.deciderCycle.Pause()
			coll.processorCycle.Pause()

			var traceID = "mytrace"
			for i := 0; i < 4; i++ {
				span := &types.Span{
					TraceID: traceID,
					ID:      fmt.Sprintf("span%d", i),
					Event: types.Event{
						Dataset: "aoeu",
						Data: map[string]interface{}{
							"trace.parent_id": "unused",
						},
						APIKey: legacyAPIKey,
					},
				}
				switch i {
				case 0, 1:
					span.Data["meta.annotation_type"] = "span_event"
				case 2:
					span.Data["meta.annotation_type"] = "link"
				}
				require.NoError(t, coll.AddSpan(span))
			}
			coll.deciderCycle.RunOnce()
			coll.processorCycle.RunOnce()

			trace := coll.SpanCache.Get(traceID)
			require.NotNil(t, trace, "after adding the spans, we should have a trace in the cache")
			assert.Equal(t, traceID, trace.TraceID, "after adding the span, we should have a trace in the cache with the right trace ID")
			assert.Equal(t, 0, len(transmission.Events), "adding a non-root span should not yet send the span")

			// ok now let's add the root span and verify that both got sent
			rootSpan := &types.Span{
				TraceID: traceID,
				ID:      "root",
				Event: types.Event{
					Dataset: "aoeu",
					Data:    map[string]interface{}{},
					APIKey:  legacyAPIKey,
				},
				IsRoot: true,
			}
			require.NoError(t, coll.AddSpan(rootSpan))

			// make sure all spans are processed and the trace is ready
			// for decision
			waitUntilReadyToDecide(t, coll, []string{traceID})
			coll.deciderCycle.RunOnce()
			waitForTraceDecision(t, coll, []string{traceID})
			coll.processorCycle.RunOnce()

			trace = coll.SpanCache.Get(traceID)
			require.Nil(t, trace, "after adding a leaf and root span, it should be removed from the cache")

			transmission.Mux.RLock()
			require.Equal(t, 5, len(transmission.Events), "adding a root span should send all spans in the trace")
			assert.Equal(t, 2, transmission.Events[0].Data["meta.span_count"], "child span metadata should be populated with span count")
			assert.Equal(t, 2, transmission.Events[1].Data["meta.span_count"], "child span metadata should be populated with span count")
			assert.Equal(t, 2, transmission.Events[1].Data["meta.span_event_count"], "child span metadata should be populated with span event count")
			assert.Equal(t, 1, transmission.Events[1].Data["meta.span_link_count"], "child span metadata should be populated with span link count")
			assert.Equal(t, 5, transmission.Events[1].Data["meta.event_count"], "child span metadata should be populated with event count")
			assert.Equal(t, 2, transmission.Events[4].Data["meta.span_count"], "root span metadata should be populated with span count")
			assert.Equal(t, 2, transmission.Events[4].Data["meta.span_event_count"], "root span metadata should be populated with span event count")
			assert.Equal(t, 1, transmission.Events[4].Data["meta.span_link_count"], "root span metadata should be populated with span link count")
			assert.Equal(t, 5, transmission.Events[4].Data["meta.event_count"], "root span metadata should be populated with event count")
			transmission.Mux.RUnlock()
		})
	}
}

// TestLateRootGetsCounts tests that the root span gets decorated with the right counts
// even if the trace had already been sent
func TestCentralCollector_LateRootGetsCounts(t *testing.T) {
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			conf := &config.MockConfig{
				GetSendDelayVal:      0,
				GetTraceTimeoutVal:   5 * time.Millisecond,
				GetSamplerTypeVal:    &config.DeterministicSamplerConfig{SampleRate: 1},
				SendTickerVal:        2 * time.Millisecond,
				ParentIdFieldNames:   []string{"trace.parent_id", "parentId"},
				AddRuleReasonToTrace: true,
				SampleCache: config.SampleCacheConfig{
					KeptSize:          100,
					DroppedSize:       100,
					SizeCheckInterval: config.Duration(1 * time.Second),
				},
				GetCollectionConfigVal: config.CollectionConfig{
					IncomingQueueSize:          100,
					ProcessTracesPauseDuration: config.Duration(1 * time.Second),
					DeciderPauseDuration:       config.Duration(1 * time.Second),
				},
			}

			transmission := &transmit.MockTransmission{}
			coll := &CentralCollector{
				Transmission: transmission,
			}
			stop := startCollector(t, conf, coll, storeType)
			defer stop()

			coll.deciderCycle.Pause()
			coll.processorCycle.Pause()

			var traceID = "mytrace"

			for i := 0; i < 4; i++ {
				span := &types.Span{
					TraceID: traceID,
					ID:      fmt.Sprintf("span%d", i),
					Event: types.Event{
						Dataset: "aoeu",
						Data: map[string]interface{}{
							"trace.parent_id": "unused",
						},
						APIKey: legacyAPIKey,
					},
				}
				switch i {
				case 0, 1:
					span.Data["meta.annotation_type"] = "span_event"
				case 2:
					span.Data["meta.annotation_type"] = "link"
				}
				require.NoError(t, coll.AddSpan(span))
			}
			// make sure all spans are processed and the trace is ready
			// for decision
			waitUntilReadyToDecide(t, coll, []string{traceID})
			coll.deciderCycle.RunOnce()
			waitForTraceDecision(t, coll, []string{traceID})
			coll.processorCycle.RunOnce()
			trace := coll.SpanCache.Get(traceID)
			require.Nil(t, trace, "trace should have been sent")
			require.Equal(t, 4, len(transmission.Events), "adding a non-root span and waiting should send the span")

			// now we add the root span and verify that both got sent and that the root span had the span count
			rootSpan := &types.Span{
				TraceID: traceID,
				ID:      "root",
				Event: types.Event{
					Dataset: "aoeu",
					Data:    map[string]interface{}{},
					APIKey:  legacyAPIKey,
				},
				IsRoot: true,
			}
			require.NoError(t, coll.AddSpan(rootSpan))
			// The trace decision is already made for the late root span
			waitForTraceDecision(t, coll, []string{traceID})
			coll.processorCycle.RunOnce()

			trace = coll.SpanCache.Get(traceID)
			require.Nil(t, trace, "after adding a leaf and root span, it should be removed from the cache")
			transmission.Mux.RLock()

			assert.Equal(t, 5, len(transmission.Events), "adding a root span should send all spans in the trace")
			assert.Equal(t, 1, transmission.Events[0].Data["meta.span_count"], "child span metadata should NOT be populated with span count")
			assert.Equal(t, 1, transmission.Events[1].Data["meta.span_count"], "child span metadata should NOT be populated with span count")
			assert.Equal(t, 2, transmission.Events[1].Data["meta.span_event_count"], "child span metadata should NOT be populated with span event count")
			assert.Equal(t, 1, transmission.Events[1].Data["meta.span_link_count"], "child span metadata should NOT be populated with span link count")
			assert.Equal(t, 4, transmission.Events[1].Data["meta.event_count"], "child span metadata should NOT be populated with event count")
			assert.GreaterOrEqual(t, 1, transmission.Events[4].Data["meta.span_count"], "root span metadata should be populated with span count")
			assert.Equal(t, 2, transmission.Events[4].Data["meta.span_event_count"], "root span metadata should be populated with span event count")
			assert.Equal(t, 1, transmission.Events[4].Data["meta.span_link_count"], "root span metadata should be populated with span link count")

			// When a late span arrives, it's sent to redis by the collector to update all counters related to the trace.
			// The processor could retrieve the trace before redis has updated the counters, leading to inaccurate counts
			// where the late span is not counted.
			assert.GreaterOrEqual(t, 4, transmission.Events[4].Data["meta.event_count"], "root span metadata should be populated with event count")
			assert.Equal(t, "deterministic/always - late arriving span", transmission.Events[4].Data["meta.refinery.reason"], "late spans should have meta.refinery.reason set to rules + late arriving span.")
			transmission.Mux.RUnlock()
		})
	}
}

// TestLateRootNotDecorated tests that spans do not get decorated with 'meta.refinery.reason' meta field
// if the AddRuleReasonToTrace attribute not set in config
func TestCentralCollector_LateSpanNotDecorated(t *testing.T) {
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			conf := &config.MockConfig{
				GetSendDelayVal:    0,
				GetTraceTimeoutVal: 5 * time.Minute,
				GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
				SendTickerVal:      2 * time.Millisecond,
				ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
				SampleCache: config.SampleCacheConfig{
					KeptSize:          100,
					DroppedSize:       100,
					SizeCheckInterval: config.Duration(1 * time.Second),
				},
				GetCollectionConfigVal: config.CollectionConfig{
					IncomingQueueSize:          10,
					ProcessTracesPauseDuration: config.Duration(1 * time.Second),
					DeciderPauseDuration:       config.Duration(1 * time.Second),
				},
			}

			transmission := &transmit.MockTransmission{}
			coll := &CentralCollector{
				Transmission: transmission,
			}
			stop := startCollector(t, conf, coll, storeType)
			defer stop()

			coll.deciderCycle.Pause()
			coll.processorCycle.Pause()

			var traceID = "traceABC"

			span := &types.Span{
				TraceID: traceID,
				ID:      "span1",
				Event: types.Event{
					Dataset: "aoeu",
					Data: map[string]interface{}{
						"trace.parent_id": "unused",
					},
					APIKey: legacyAPIKey,
				},
			}
			require.NoError(t, coll.AddSpan(span))
			// make sure all spans are processed and the trace is ready
			// for decision
			waitUntilReadyToDecide(t, coll, []string{traceID})
			coll.deciderCycle.RunOnce()
			waitForTraceDecision(t, coll, []string{traceID})
			coll.processorCycle.RunOnce()
			trace := coll.SpanCache.Get(traceID)
			require.Nil(t, trace, "trace should have been sent")
			require.Equal(t, 1, len(transmission.Events), "adding a non-root span and waiting should send the span")

			rootSpan := &types.Span{
				TraceID: traceID,
				ID:      "root",
				Event: types.Event{
					Dataset: "aoeu",
					Data:    map[string]interface{}{},
					APIKey:  legacyAPIKey,
				},
				IsRoot: true,
			}
			require.NoError(t, coll.AddSpan(rootSpan))
			// The trace decision is already made for the late root span
			waitForTraceDecision(t, coll, []string{traceID})
			coll.processorCycle.RunOnce()

			trace = coll.SpanCache.Get(traceID)
			require.Nil(t, trace, "after adding a leaf and root span, it should be removed from the cache")

			transmission.Mux.RLock()
			assert.Equal(t, 2, len(transmission.Events), "adding a root span should send all spans in the trace")
			assert.Equal(t, nil, transmission.Events[1].Data["meta.refinery.reason"], "late span should not have meta.refinery.reason set to late")
			transmission.Mux.RUnlock()
		})
	}
}

func TestCentralCollector_AddAdditionalAttributes(t *testing.T) {
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			conf := &config.MockConfig{
				GetSendDelayVal:    0,
				GetTraceTimeoutVal: 60 * time.Second,
				GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
				SendTickerVal:      2 * time.Millisecond,
				AdditionalAttributes: map[string]string{
					"name":  "foo",
					"other": "bar",
				},
				SampleCache: config.SampleCacheConfig{
					KeptSize:          100,
					DroppedSize:       100,
					SizeCheckInterval: config.Duration(1 * time.Second),
				},
				GetCollectionConfigVal: config.CollectionConfig{
					IncomingQueueSize:          5,
					ProcessTracesPauseDuration: config.Duration(1 * time.Second),
					DeciderPauseDuration:       config.Duration(1 * time.Second),
				},
			}
			transmission := &transmit.MockTransmission{}
			coll := &CentralCollector{
				Transmission: transmission,
			}
			stop := startCollector(t, conf, coll, storeType)
			defer stop()

			coll.deciderCycle.Pause()
			coll.processorCycle.Pause()

			var traceID = "trace123"

			span := &types.Span{
				TraceID: traceID,
				ID:      "span1",
				Event: types.Event{
					Dataset: "aoeu",
					Data: map[string]interface{}{
						"trace.parent_id": "unused",
					},
					APIKey: legacyAPIKey,
				},
			}
			require.NoError(t, coll.AddSpan(span))

			rootSpan := &types.Span{
				TraceID: traceID,
				ID:      "root",
				Event: types.Event{
					Dataset: "aoeu",
					Data:    map[string]interface{}{},
					APIKey:  legacyAPIKey,
				},
				IsRoot: true,
			}
			require.NoError(t, coll.AddSpan(rootSpan))
			waitUntilReadyToDecide(t, coll, []string{traceID})
			coll.deciderCycle.RunOnce()
			waitForTraceDecision(t, coll, []string{traceID})
			coll.processorCycle.RunOnce()

			transmission.Mux.RLock()
			assert.Equal(t, 2, len(transmission.Events), "should be some events transmitted")
			assert.Equal(t, "foo", transmission.Events[0].Data["name"], "new attribute should appear in data")
			assert.Equal(t, "bar", transmission.Events[0].Data["other"], "new attribute should appear in data")
			transmission.Mux.RUnlock()
		})
	}
}

func TestCentralCollector_SpanWithRuleReasons(t *testing.T) {
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			conf := &config.MockConfig{
				GetSendDelayVal:    0,
				GetTraceTimeoutVal: 5 * time.Millisecond,
				GetSamplerTypeVal: &config.RulesBasedSamplerConfig{
					Rules: []*config.RulesBasedSamplerRule{
						{
							Name:       "rule 1",
							Scope:      "trace",
							SampleRate: 1,
							Conditions: []*config.RulesBasedSamplerCondition{
								{
									Field:    "test",
									Operator: config.EQ,
									Value:    int64(1),
								},
							},
							Sampler: &config.RulesBasedDownstreamSampler{
								DynamicSampler: &config.DynamicSamplerConfig{
									SampleRate: 1,
									FieldList:  []string{"http.status_code"},
								},
							},
						},
						{
							Name:  "rule 2",
							Scope: "span",
							Conditions: []*config.RulesBasedSamplerCondition{
								{
									Field:    "test",
									Operator: config.EQ,
									Value:    int64(2),
								},
							},
							Sampler: &config.RulesBasedDownstreamSampler{
								EMADynamicSampler: &config.EMADynamicSamplerConfig{
									GoalSampleRate: 1,
									FieldList:      []string{"http.status_code"},
								},
							},
						},
					}},
				SendTickerVal:        60 * time.Second,
				ParentIdFieldNames:   []string{"trace.parent_id", "parentId"},
				AddRuleReasonToTrace: true,
				SampleCache: config.SampleCacheConfig{
					KeptSize:          100,
					DroppedSize:       100,
					SizeCheckInterval: config.Duration(1 * time.Second),
				},
				GetCollectionConfigVal: config.CollectionConfig{
					IncomingQueueSize:          100,
					ProcessTracesPauseDuration: config.Duration(1 * time.Second),
					DeciderPauseDuration:       config.Duration(1 * time.Second),
				},
			}

			transmission := &transmit.MockTransmission{}
			coll := &CentralCollector{
				Transmission: transmission,
			}
			stop := startCollector(t, conf, coll, storeType)
			defer stop()

			coll.deciderCycle.Pause()
			coll.processorCycle.Pause()

			traceIDs := []string{"trace1", "trace2"}

			for i := 0; i < 4; i++ {
				span := &types.Span{
					ID: fmt.Sprintf("span%d", i),
					Event: types.Event{
						Dataset: "aoeu",
						Data: map[string]interface{}{
							"trace.parent_id":  "unused",
							"http.status_code": 200,
						},
						APIKey: legacyAPIKey,
					},
				}
				switch i {
				case 0, 1:
					span.TraceID = traceIDs[0]
					span.Data["test"] = int64(1)
				case 2, 3:
					span.TraceID = traceIDs[1]
					span.Data["test"] = int64(2)
				}
				require.NoError(t, coll.AddSpan(span))
			}
			waitUntilReadyToDecide(t, coll, traceIDs)
			coll.deciderCycle.RunOnce()
			waitForTraceDecision(t, coll, traceIDs)
			coll.processorCycle.RunOnce()
			require.Equal(t, 4, len(transmission.Events), "adding a non-root span and waiting should send the span")

			for i, traceID := range traceIDs {
				rootSpan := &types.Span{
					TraceID: traceID,
					ID:      "root",
					Event: types.Event{
						Dataset: "aoeu",
						Data: map[string]interface{}{
							"http.status_code": 200,
						},
						APIKey: legacyAPIKey,
					},
					IsRoot: true,
				}
				if i == 0 {
					rootSpan.Data["test"] = int64(1)
				} else {
					rootSpan.Data["test"] = int64(2)
				}

				require.NoError(t, coll.AddSpan(rootSpan))
			}

			waitForTraceDecision(t, coll, traceIDs)
			coll.processorCycle.RunOnce()

			transmission.Mux.RLock()
			assert.Equal(t, 6, len(transmission.Events), "adding a root span should send all spans in the trace")
			for _, event := range transmission.Events {
				reason := event.Data["meta.refinery.reason"]
				if event.Data["test"] == int64(1) {
					if _, ok := event.Data["trace.parent_id"]; ok {
						assert.Equal(t, "rules/trace/rule 1:dynamic", reason, event.Data)
					} else {
						assert.Equal(t, "rules/trace/rule 1:dynamic - late arriving span", reason, event.Data)
					}
				} else {
					if _, ok := event.Data["trace.parent_id"]; ok {
						assert.Equal(t, "rules/span/rule 2:emadynamic", reason, event.Data)
					} else {
						assert.Equal(t, "rules/span/rule 2:emadynamic - late arriving span", reason, event.Data)
					}
				}
			}
			transmission.Mux.RUnlock()
		})
	}
}

func TestCentralCollector_Shutdown(t *testing.T) {
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			conf := &config.MockConfig{
				GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 2},
				SendTickerVal:      2 * time.Millisecond,
				ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
				GetCollectionConfigVal: config.CollectionConfig{
					CacheCapacity:              100,
					ProcessTracesPauseDuration: config.Duration(1 * time.Second),
					DeciderPauseDuration:       config.Duration(1 * time.Second),
					ShutdownDelay:              config.Duration(500 * time.Millisecond),
				},
			}
			transmission := &transmit.MockTransmission{}

			collector := &CentralCollector{
				Transmission: transmission,
			}
			stop := startCollector(t, conf, collector, storeType)
			defer stop()

			collector.processorCycle.Pause()
			collector.deciderCycle.Pause()

			numberOfTraces := 10
			numberOfSpansPerTrace := 10
			traceids := make([]string, 0, numberOfTraces)
			for tr := 0; tr < numberOfTraces; tr++ {
				tid := fmt.Sprintf("trace%02d", tr)
				traceids = append(traceids, tid)
				// write 9 child spans to the store
				for s := 1; s < numberOfSpansPerTrace; s++ {
					span := &types.Span{
						TraceID: tid,
						ID:      fmt.Sprintf("span%d", s),
						Event: types.Event{
							Dataset:     "aoeu",
							Environment: "test",
							Data: map[string]interface{}{
								"trace.parent_id": fmt.Sprintf("span%d", s-1),
							},
						},
					}
					require.NoError(t, collector.AddSpan(span))
				}
				// now write the root span
				span := &types.Span{
					TraceID: tid,
					ID:      "span0",
					IsRoot:  true,
				}
				require.NoError(t, collector.AddSpan(span))
			}

			// wait for all traces to be processed
			waitUntilReadyToDecide(t, collector, traceids)

			// start the decider again to mock trace decision process
			collector.deciderCycle.Continue()
			time.Sleep(conf.GetCollectionConfigVal.GetDeciderPauseDuration() * 3)

			waitForTraceDecision(t, collector, traceids)

			ctx := context.Background()
			err := collector.shutdown(ctx)
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				transmission.Mux.Lock()
				defer transmission.Mux.Unlock()

				sentCount := len(transmission.Events)
				return sentCount > 0 && sentCount < numberOfSpansPerTrace*numberOfTraces
			}, 2*time.Second, 50*time.Millisecond, "should have sent some traces")
		})
	}
}

func TestCentralCollector_ProcessSpanImmediately(t *testing.T) {
	for _, storeType := range storeTypes {
		t.Run(storeType, func(t *testing.T) {
			conf := &config.MockConfig{
				ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
				GetCollectionConfigVal: config.CollectionConfig{
					IncomingQueueSize:          100,
					ProcessTracesPauseDuration: config.Duration(1 * time.Second),
					DeciderPauseDuration:       config.Duration(1 * time.Second),
				},
				StressRelief: config.StressReliefConfig{
					SamplingRate: 1,
				},
			}
			transmission := &transmit.MockTransmission{}
			collector := &CentralCollector{
				Transmission: transmission,
				StressRelief: &stressRelief.MockStressReliever{
					SampleDeterministically: false,
					SampleRate:              1,
				},
			}
			stop := startCollector(t, conf, collector, storeType)
			defer stop()

			span := &types.Span{
				TraceID: "trace1",
				ID:      "span1",
				Event: types.Event{
					Dataset: "aoeu",
					Data:    make(map[string]interface{}),
				},
			}

			// if the trace should not be deterministically sampled, it should go through
			// the normal processing path not the immediate path
			processed, err := collector.ProcessSpanImmediately(span)
			require.NoError(t, err)
			require.False(t, processed)
			// No new spans should be sent
			transmission.Mux.Lock()
			events := transmission.Events
			require.Len(t, events, 0)
			transmission.Mux.Unlock()

			// if the trace should be deterministically sampled, it should go through
			// the immediate processing path.
			// If the sample decision is drop, the span should not be sent
			collector.StressRelief.(*stressRelief.MockStressReliever).SampleDeterministically = true
			processed, err = collector.ProcessSpanImmediately(span)
			require.NoError(t, err)
			require.True(t, processed)
			// No new spans should be sent
			transmission.Mux.Lock()
			events = transmission.Events
			require.Len(t, events, 0)
			transmission.Mux.Unlock()

			// if the sample decision is kept, the span should be sent
			collector.StressRelief.(*stressRelief.MockStressReliever).ShouldKeep = true
			processed, err = collector.ProcessSpanImmediately(span)
			require.NoError(t, err)
			require.True(t, processed)

			transmission.Mux.Lock()
			events = transmission.Events
			require.Len(t, events, 1)
			require.NotEmpty(t, events[0].Data)
			require.True(t, events[0].Data["meta.stressed"].(bool))
			require.Equal(t, uint(1), events[0].SampleRate)
			transmission.Mux.Unlock()
		})
	}
}

func startCollector(t *testing.T, cfg *config.MockConfig, collector *CentralCollector,
	storeType string) func() {
	if cfg == nil {
		cfg = &config.MockConfig{}
	}

	if collector.Clock == nil {
		collector.Clock = clockwork.NewRealClock()
	}

	if collector.Transmission == nil {
		collector.Transmission = &transmit.MockTransmission{}
	}

	if collector.StressRelief == nil {
		collector.StressRelief = &stressRelief.MockStressReliever{}
	}

	if cfg.StoreOptions.SpanChannelSize == 0 {
		cfg.StoreOptions.SpanChannelSize = 100
	}

	if cfg.StoreOptions.StateTicker == 0 {
		cfg.StoreOptions.StateTicker = duration("50ms")
	}

	if cfg.GetSendTickerValue() == 0 {
		cfg.SendTickerVal = 50 * time.Millisecond
	}

	if cfg.StoreOptions.SendDelay == 0 {
		cfg.StoreOptions.SendDelay = duration("200ms")
	}

	if cfg.StoreOptions.TraceTimeout == 0 {
		cfg.StoreOptions.TraceTimeout = duration("500ms")
	}

	if cfg.StoreOptions.DecisionTimeout == 0 {
		cfg.StoreOptions.DecisionTimeout = duration("500ms")
	}

	if cfg.SampleCache.KeptSize == 0 {
		cfg.SampleCache.KeptSize = 1000
	}
	if cfg.SampleCache.DroppedSize == 0 {
		cfg.SampleCache.DroppedSize = 1000
	}

	if cfg.SampleCache.SizeCheckInterval == 0 {
		cfg.SampleCache.SizeCheckInterval = duration("1s")
	}

	if cfg.GetCollectionConfigVal.DeciderBatchSize == 0 {
		cfg.GetCollectionConfigVal.DeciderBatchSize = 10
	}

	if cfg.GetCollectionConfigVal.ProcessTracesBatchSize == 0 {
		cfg.GetCollectionConfigVal.ProcessTracesBatchSize = 10
	}

	if cfg.GetCollectionConfigVal.ShutdownDelay == 0 {
		cfg.GetCollectionConfigVal.ShutdownDelay = duration("10ms")
	}

	if cfg.GetTraceTimeoutVal == 0 {
		cfg.GetTraceTimeoutVal = time.Duration(500 * time.Microsecond)
	}

	collector.isTest = true
	var basicStore centralstore.BasicStorer
	switch storeType {
	case "redis":
		basicStore = &centralstore.RedisBasicStore{}
	case "local":
		basicStore = &centralstore.LocalStore{}
	}
	decisionCache := &cache.CuckooSentCache{}
	sw := &centralstore.SmartWrapper{}
	spanCache := &cache.SpanCache_basic{}
	redis := &redis.TestService{}
	samplerFactory := &sample.SamplerFactory{
		Config: cfg,
		Logger: &logger.NullLogger{},
	}

	objects := []*inject.Object{
		{Value: "version", Name: "version"},
		{Value: cfg},
		{Value: &logger.NullLogger{}},
		{Value: &metrics.MockMetrics{}, Name: "genericMetrics"},
		{Value: trace.Tracer(noop.Tracer{}), Name: "tracer"},
		{Value: decisionCache},
		{Value: spanCache},
		{Value: collector.Transmission, Name: "upstreamTransmission"},
		{Value: &peer.MockPeers{Peers: []string{"foo", "bar"}}},
		{Value: samplerFactory},
		{Value: redis, Name: "redis"},
		{Value: collector.Clock},
		{Value: collector.StressRelief, Name: "stressRelief"},
		{Value: basicStore},
		{Value: sw},
		{Value: collector},
		{Value: &health.Health{}},
	}
	g := inject.Graph{}
	require.NoError(t, g.Provide(objects...))
	require.NoError(t, g.Populate())

	require.NoError(t, startstop.Start(g.Objects(), nil))

	stopper := func() {
		require.NoError(t, startstop.Stop(g.Objects(), nil))
	}

	return stopper

}

func duration(s string) config.Duration {
	d, _ := time.ParseDuration(s)
	return config.Duration(d)
}

func waitUntilReadyToDecide(t *testing.T, coll *CentralCollector, traceIDs []string) {
	ctx := context.Background()
	idMap := generics.NewSetWithCapacity[string](len(traceIDs))

	require.Eventually(t, func() bool {
		ids, err := coll.Store.GetTracesForState(ctx, centralstore.ReadyToDecide)
		require.NoError(t, err)
		idMap.Add(ids...)
		return len(idMap.Members()) == len(traceIDs)
	}, 8*time.Second, 50*time.Millisecond)
}

func waitForTraceDecision(t *testing.T, coll *CentralCollector, traceIDs []string) {
	ctx := context.Background()
	require.Eventually(t, func() bool {
		statuses, err := coll.Store.GetStatusForTraces(ctx, traceIDs)
		require.NoError(t, err)
		var count int
		for _, status := range statuses {
			if !slices.Contains(traceIDs, status.TraceID) {
				continue
			}
			switch status.State {
			case centralstore.DecisionKeep:
				count++
			case centralstore.DecisionDrop:
				count++
			default:
				fmt.Println("status", status.State)
			}
		}
		return count == len(traceIDs)
	}, 5*time.Second, 20*time.Millisecond)
}

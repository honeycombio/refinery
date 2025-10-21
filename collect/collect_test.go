package collect

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/facebookgo/inject"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/sharder"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"
)

const legacyAPIKey = "c9945edf5d245834089a1bd6cc9ad01e"

var peerTraceIDs = []string{"peer-trace-1", "peer-trace-2", "peer-trace-3"}

// getFromCache is a test helper to retrieve a trace from the appropriate collect loop's cache.
// Note technically this is still racy because the trace itself could be modified
// concurrently, after this function returns. Since this is just for testing, if the race
// detector doesn't complain, then I won't worry about it.
func getFromCache(coll *InMemCollector, traceID string) *types.Trace {
	cl := coll.collectLoops[coll.getLoopForTrace(traceID)]

	ch := make(chan struct{})
	defer close(ch)

	cl.pause <- ch
	return cl.cache.Get(traceID)
}

func newCache() (cache.TraceSentCache, error) {
	cfg := config.SampleCacheConfig{
		KeptSize:          100,
		DroppedSize:       100,
		SizeCheckInterval: config.Duration(1 * time.Second),
	}

	return cache.NewCuckooSentCache(cfg, &metrics.NullMetrics{})
}

func newTestCollector(t testing.TB, conf config.Config, maybeClock ...clockwork.Clock) *InMemCollector {
	transmission := &transmit.MockTransmission{}
	err := transmission.Start()
	require.NoError(t, err)

	peerTransmission := &transmit.MockTransmission{}
	err = peerTransmission.Start()
	require.NoError(t, err)

	s := &metrics.MockMetrics{}
	s.Start()

	var clock clockwork.Clock
	if len(maybeClock) > 0 {
		clock = maybeClock[0]
	} else {
		clock = clockwork.NewRealClock()
	}

	healthReporter := &health.Health{
		Clock: clock,
	}
	healthReporter.Start()
	localPubSub := &pubsub.LocalPubSub{
		Config:  conf,
		Metrics: s,
	}
	localPubSub.Start()

	sf := &sample.SamplerFactory{
		Config:  conf,
		Metrics: s,
		Logger:  &logger.NullLogger{},
	}
	err = sf.Start()
	require.NoError(t, err)

	c := &InMemCollector{
		TestMode:         true,
		Config:           conf,
		Clock:            clock,
		Logger:           &logger.NullLogger{},
		Tracer:           noop.NewTracerProvider().Tracer("test"),
		Health:           healthReporter,
		Transmission:     transmission,
		PeerTransmission: peerTransmission,
		PubSub:           localPubSub,
		Metrics:          s,
		StressRelief:     &MockStressReliever{},
		SamplerFactory:   sf,
		done:             make(chan struct{}),
		Peers:            peer.NewMockPeers([]string{"api1", "api2"}, "api1"),
		Sharder: &sharder.MockSharder{
			Self: &sharder.TestShard{
				Addr: "api1",
			},
			Other: &sharder.TestShard{
				Addr:     "api2",
				TraceIDs: peerTraceIDs,
			},
		},
	}

	err = c.Start()
	require.NoError(t, err)

	t.Cleanup(func() {
		err := c.Stop()
		assert.NoError(t, err)
		err = transmission.Stop()
		assert.NoError(t, err)
		err = peerTransmission.Stop()
		assert.NoError(t, err)
		err = healthReporter.Stop()
		assert.NoError(t, err)

		sf.Stop()
	})

	return c
}

// TestAddRootSpan tests that adding a root span winds up with a trace object in
// the cache and that that trace gets sent
func TestAddRootSpan(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(60 * time.Second),
			MaxBatchSize: 500,
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			NumCollectLoops:   2,
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}
	coll := newTestCollector(t, conf)
	transmission := coll.Transmission.(*transmit.MockTransmission)

	var traceID1 = "mytrace"
	var traceID2 = "mytraess"

	span := &types.Span{
		TraceID: traceID1,
		Event: types.Event{
			Dataset: "aoeu",
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(span)

	// adding one span with no parent ID should:
	// * create the trace in the cache
	// * send the trace
	// * remove the trace from the cache

	events := transmission.GetBlock(1)
	require.Equal(t, 1, len(events), "adding a root span should send the span")
	assert.Equal(t, "aoeu", events[0].Dataset, "sending a root span should immediately send that span via transmission")

	assert.Nil(t, getFromCache(coll, traceID1), "after sending the span, it should be removed from the cache")

	span = &types.Span{
		TraceID: traceID2,
		Event: types.Event{
			Dataset: "aoeu",
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpanFromPeer(span)
	// adding one span with no parent ID should:
	// * create the trace in the cache
	// * send the trace
	// * remove the trace from the cache
	events = transmission.GetBlock(1)
	require.Equal(t, 1, len(events), "adding another root span should send the span")
	assert.Equal(t, "aoeu", events[0].Dataset, "sending a root span should immediately send that span via transmission")

	assert.Nil(t, getFromCache(coll, traceID1), "after sending the span, it should be removed from the cache")
}

// #490, SampleRate getting stomped could cause confusion if sampling was
// happening upstream of refinery. Writing down what got sent to refinery
// will help people figure out what is going on.
func TestOriginalSampleRateIsNotedInMetaField(t *testing.T) {
	// The sample rate applied by Refinery in this test's config.
	const expectedDeterministicSampleRate = int(2)
	// The sample rate happening upstream of Refinery.
	const originalSampleRate = uint(50)

	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(60 * time.Second),
			MaxBatchSize: 500,
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: expectedDeterministicSampleRate},
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			NumCollectLoops:   2,
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}
	coll := newTestCollector(t, conf)
	transmission := coll.Transmission.(*transmit.MockTransmission)

	// Generate events until one is sampled and appears on the transmission queue for sending.
	sendAttemptCount := 0
	require.Eventually(t, func() bool {
		sendAttemptCount++
		span := &types.Span{
			TraceID: fmt.Sprintf("trace-%v", sendAttemptCount),
			Event: types.Event{
				Dataset:    "aoeu",
				APIKey:     legacyAPIKey,
				SampleRate: originalSampleRate,
				Data:       types.NewPayload(coll.Config, make(map[string]interface{})),
			},
			IsRoot: true,
		}
		err := coll.AddSpan(span)
		require.NoError(t, err, "must be able to add the span")
		return len(transmission.Events) >= 1
	}, 5*time.Second, conf.GetTracesConfig().GetSendTickerValue(), "at least one event should be sampled and sent")

	events := transmission.GetBlock(1)
	require.Equal(t, 1, len(events), "adding another root span should send the span")
	upstreamSampledEvent := events[0]

	assert.NotNil(t, upstreamSampledEvent)
	assert.Equal(t, int64(originalSampleRate), upstreamSampledEvent.Data.Get(types.MetaRefineryOriginalSampleRate),
		"metadata should be populated with original sample rate")
	assert.Equal(t, originalSampleRate*uint(expectedDeterministicSampleRate), upstreamSampledEvent.SampleRate,
		"sample rate for the event should be the original sample rate multiplied by the deterministic sample rate")

	// Generate one more event with no upstream sampling applied.
	err := coll.AddSpan(&types.Span{
		TraceID: fmt.Sprintf("trace-%v", 1000),
		Event: types.Event{
			Dataset:    "no-upstream-sampling",
			APIKey:     legacyAPIKey,
			SampleRate: 0, // no upstream sampling
			Data:       types.NewPayload(coll.Config, make(map[string]interface{})),
		},
		IsRoot: true,
	})
	require.NoError(t, err, "must be able to add the span")

	// Now get the events and find the one we want
	events = transmission.GetBlock(2)
	var noUpstreamSampleRateEvent *types.Event
	for _, event := range events {
		if event.Dataset == "no-upstream-sampling" {
			noUpstreamSampleRateEvent = event
			break
		}
	}
	require.NotNil(t, noUpstreamSampleRateEvent)
	assert.Equal(t, "no-upstream-sampling", noUpstreamSampleRateEvent.Dataset)
	assert.Nil(t, noUpstreamSampleRateEvent.Data.Get(types.MetaRefineryOriginalSampleRate),
		"original sample rate should not be set in metadata when original sample rate is zero")
}

// HoneyComb treats a missing or 0 SampleRate the same as 1, but
// behaves better/more consistently if the SampleRate is explicitly
// set instead of inferred
func TestTransmittedSpansShouldHaveASampleRateOfAtLeastOne(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(60 * time.Second),
			MaxBatchSize: 500,
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			NumCollectLoops:   2,
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}
	coll := newTestCollector(t, conf)
	transmission := coll.Transmission.(*transmit.MockTransmission)

	span := &types.Span{
		TraceID: fmt.Sprintf("trace-%v", 1),
		Event: types.Event{
			Dataset:    "aoeu",
			APIKey:     legacyAPIKey,
			SampleRate: 0, // This should get lifted to 1
			Data:       types.NewPayload(coll.Config, nil),
		},
		IsRoot: true,
	}

	coll.AddSpan(span)

	events := transmission.GetBlock(1)
	assert.Equal(t, uint(1), events[0].SampleRate,
		"SampleRate should be reset to one after starting at zero")
}

// TestAddSpan tests that adding a span winds up with a trace object in the
// cache
func TestAddSpan(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(60 * time.Second),
			MaxBatchSize: 500,
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			NumCollectLoops:   2,
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}
	coll := newTestCollector(t, conf)
	transmission := coll.Transmission.(*transmit.MockTransmission)

	var traceID = "mytrace"
	loopIndex := coll.getLoopForTrace(traceID)

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: types.NewPayload(coll.Config, map[string]interface{}{
				"trace.trace_id":  traceID,
				"trace.parent_id": "unused",
			}),
			APIKey: legacyAPIKey,
		},
	}
	span.Event.Data.ExtractMetadata()
	coll.AddSpanFromPeer(span)

	assert.EventuallyWithT(t,
		func(cT *assert.CollectT) {
			trace := getFromCache(coll, traceID)
			require.NotNil(cT, trace)
			assert.Equal(cT, traceID, trace.TraceID)
			v, ok := coll.Metrics.Get("span_processed")
			assert.True(cT, ok)
			assert.Equal(cT, float64(1), v)
		},
		conf.GetTracesConfig().GetSendTickerValue()*60, // waitFor
		conf.GetTracesConfig().GetSendTickerValue()*2,  // tick
		"within a reasonable time, cache should contain a traceID matching the one from the span we added",
	)
	assert.Equal(t, 0, len(transmission.GetBlock(0)), "adding a non-root span should not yet send the span")

	// ok now let's add the root span and verify that both got sent
	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    types.NewPayload(coll.Config, nil),
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	rootSpan.Event.Data.ExtractMetadata()
	coll.AddSpan(rootSpan)

	assert.Equal(t, 2, len(transmission.GetBlock(2)), "adding a root span should send all spans in the trace")
	assert.Nil(t, getFromCache(coll, traceID), "after adding a leaf and root span, it should be removed from the cache")

	ch := make(chan struct{})
	defer close(ch)
	coll.collectLoops[loopIndex].pause <- ch
	assert.Equal(t, int64(0), coll.collectLoops[loopIndex].localSpanProcessed)
}

// TestDryRunMode tests that all traces are sent, regardless of sampling decision, and that the
// sampling decision is marked on each span in the trace
func TestDryRunMode(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(20 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(60 * time.Second),
			MaxBatchSize: 500,
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal: &config.DeterministicSamplerConfig{
			SampleRate: 10,
		},
		DryRun:             true,
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			NumCollectLoops:   2,
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}
	coll := newTestCollector(t, conf)
	transmission := coll.Transmission.(*transmit.MockTransmission)

	samplerFactory := &sample.SamplerFactory{
		Config: conf,
		Logger: &logger.NullLogger{},
	}
	sampler := samplerFactory.GetSamplerImplementationForKey("test")
	coll.SamplerFactory = samplerFactory

	var traceID1 = "abc123"
	var traceID2 = "def456"
	var traceID3 = "ghi789"
	// sampling decisions based on trace ID
	sampleRate1, keepTraceID1, _, _ := sampler.GetSampleRate(&types.Trace{TraceID: traceID1})
	// would be dropped if dry run mode was not enabled
	assert.False(t, keepTraceID1)
	assert.Equal(t, uint(10), sampleRate1)
	sampleRate2, keepTraceID2, _, _ := sampler.GetSampleRate(&types.Trace{TraceID: traceID2})
	assert.True(t, keepTraceID2)
	assert.Equal(t, uint(10), sampleRate2)
	sampleRate3, keepTraceID3, _, _ := sampler.GetSampleRate(&types.Trace{TraceID: traceID3})
	// would be dropped if dry run mode was not enabled
	assert.False(t, keepTraceID3)
	assert.Equal(t, uint(10), sampleRate3)

	span := &types.Span{
		TraceID: traceID1,
		Event: types.Event{
			Data:   types.NewPayload(coll.Config, nil),
			APIKey: legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(span)

	// adding one span with no parent ID should:
	// * create the trace in the cache
	// * send the trace
	// * remove the trace from the cache
	events := transmission.GetBlock(1)
	require.Equal(t, 1, len(events), "adding a root span should send the span")
	assert.Equal(t, keepTraceID1, events[0].Data.Get(config.DryRunFieldName), "config.DryRunFieldName should match sampling decision for its trace ID")
	assert.Nil(t, getFromCache(coll, traceID1), "after sending the span, it should be removed from the cache")

	// add a non-root span, create the trace in the cache
	span = &types.Span{
		TraceID: traceID2,
		Event: types.Event{
			Dataset: "aoeu",
			Data: types.NewPayload(coll.Config, map[string]interface{}{
				"trace.parent_id": "unused",
			}),
			APIKey: legacyAPIKey,
		},
	}
	coll.AddSpanFromPeer(span)

	assert.Eventually(t, func() bool {
		trace := getFromCache(coll, traceID2)
		if trace == nil {
			return false
		}
		return traceID2 == trace.TraceID
	}, conf.GetTracesConfig().GetSendTickerValue()*6, conf.GetTracesConfig().GetSendTickerValue()*2, "after adding the span, we should have a trace in the cache with the right trace ID")

	span = &types.Span{
		TraceID: traceID2,
		Event: types.Event{
			Data:   types.NewPayload(coll.Config, nil),
			APIKey: legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpanFromPeer(span)

	// adding root span to send the trace
	events = transmission.GetBlock(2)
	require.Equal(t, 2, len(events), "adding another root span should send the span")
	// both spanscollectshould be marked with the sampling decision
	assert.Equal(t, keepTraceID2, events[0].Data.Get(config.DryRunFieldName), "config.DryRunFieldName should match sampling decision for its trace ID")
	assert.Equal(t, keepTraceID2, events[1].Data.Get(config.DryRunFieldName), "config.DryRunFieldName should match sampling decision for its trace ID")
	// check that meta value associated with dry run mode is properly applied
	assert.Equal(t, uint(10), events[0].Data.Get("meta.dryrun.sample_rate"))
	// check expected sampleRate against span data
	assert.Equal(t, sampleRate1, events[0].Data.Get("meta.dryrun.sample_rate"))
	assert.Equal(t, sampleRate2, events[1].Data.Get("meta.dryrun.sample_rate"))

	span = &types.Span{
		TraceID: traceID3,
		Event: types.Event{
			Data:   types.NewPayload(coll.Config, nil),
			APIKey: legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(span)

	// adding one span with no parent ID should:
	// * create the trace in the cache
	// * send the trace
	// * remove the trace from the cache
	events = transmission.GetBlock(1)
	require.Equal(t, 1, len(events), "adding a root span should send the span")
	assert.Equal(t, keepTraceID3, events[0].Data.Get(config.DryRunFieldName), "field should match sampling decision for its trace ID")
	assert.Nil(t, getFromCache(coll, traceID3), "after sending the span, it should be removed from the cache")

}

// TestSampleConfigReload tests dynamic configuration reloading for sampling rules
func TestSampleConfigReload(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(60 * time.Second),
			MaxBatchSize: 500,
		},
		// Start with DynamicSampler with low sample rate to test config changes
		GetSamplerTypeVal:  &config.DynamicSamplerConfig{SampleRate: 2, FieldList: []string{"service.name"}},
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			NumCollectLoops:   1,
			IncomingQueueSize: 10,
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
	}

	coll := newTestCollector(t, conf)

	// Start the SamplerFactory to initialize its maps
	err := coll.SamplerFactory.Start()
	require.NoError(t, err)

	// Just one loop keep things intelligible.
	loop := coll.collectLoops[0]

	dataset := "aoeu"

	// Create a test trace for consistent results
	createTestTrace := func(traceID string) *types.Trace {
		mockCfg := &config.MockConfig{}
		trace := &types.Trace{TraceID: traceID}
		trace.AddSpan(&types.Span{
			Event: types.Event{
				Data: types.NewPayload(mockCfg, map[string]any{
					"service.name": "test-service",
				}),
			},
		})
		return trace
	}

	// Sending a span should cause the sampler to be loaded.
	span := &types.Span{
		TraceID: "1",
		Event: types.Event{
			Dataset: dataset,
			APIKey:  legacyAPIKey,
			Data: types.NewPayload(
				&config.MockConfig{},
				map[string]any{"service.name": "test-service"},
			),
		},
		IsRoot: true,
	}
	coll.AddSpan(span)

	// Wait for the sampler to be loaded and test initial sample rate
	var initialRate uint
	assert.Eventually(t, func() bool {
		ch := make(chan struct{})
		defer close(ch)

		loop.pause <- ch
		if sampler := loop.datasetSamplers[dataset]; sampler != nil {
			testTrace := createTestTrace("test-trace-1")
			rate, _, reason, _ := sampler.GetSampleRate(testTrace)
			if reason == "dynamic" && rate > 0 {
				initialRate = rate
				return true
			}
		}
		return false
	}, conf.GetTracesConfig().GetTraceTimeout()*2, conf.GetTracesConfig().GetSendTickerValue())
	assert.Equal(t, uint(2), initialRate)

	// Change the configuration to a higher sample rate
	conf.Mux.Lock()
	conf.GetSamplerTypeVal = &config.DynamicSamplerConfig{SampleRate: 10, FieldList: []string{"service.name"}}
	conf.Mux.Unlock()

	// Reloading the config should clear the sampler.
	conf.Reload()
	assert.Eventually(t, func() bool {
		ch := make(chan struct{})
		defer close(ch)

		loop.pause <- ch
		return loop.datasetSamplers[dataset] == nil
	}, conf.GetTracesConfig().GetTraceTimeout()*2, conf.GetTracesConfig().GetSendTickerValue())

	// Another span, it gets loaded again with new configuration.
	span = &types.Span{
		TraceID: "2",
		Event:   types.Event{Dataset: dataset, APIKey: legacyAPIKey, Data: types.NewPayload(&config.MockConfig{}, map[string]any{"service.name": "test-service"})},
		IsRoot:  true,
	}
	coll.AddSpan(span)

	// Wait for the new sampler to be loaded and test the updated sample rate
	var newRate uint
	assert.Eventually(t, func() bool {
		ch := make(chan struct{})
		defer close(ch)

		loop.pause <- ch
		if sampler := loop.datasetSamplers[dataset]; sampler != nil {
			testTrace := createTestTrace("test-trace-2")
			rate, _, reason, _ := sampler.GetSampleRate(testTrace)
			if reason == "dynamic" && rate > 0 {
				newRate = rate
				return true
			}
		}
		return false
	}, conf.GetTracesConfig().GetTraceTimeout()*2, conf.GetTracesConfig().GetSendTickerValue())
	assert.Equal(t, uint(10), newRate)
}

// TestStableMaxAlloc tests memory pressure handling and cache eviction when memory allocation limits are exceeded
func TestStableMaxAlloc(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(10 * time.Minute),
			MaxBatchSize: 500,
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          1000,
			DroppedSize:       1000,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			NumCollectLoops:   2,
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 1000,
			PeerQueueSize:     5,
		},
	}

	coll := newTestCollector(t, conf)
	transmission := coll.Transmission.(*transmit.MockTransmission)

	spandata := make([]map[string]interface{}, 500)
	for i := 0; i < 500; i++ {
		spandata[i] = map[string]interface{}{
			"trace.parent_id": "unused",
			"id":              i,
			"str1":            strings.Repeat("abc", rand.Intn(100)+1),
			"str2":            strings.Repeat("def", rand.Intn(100)+1),
		}
	}

	for i := 0; i < 500; i++ {
		span := &types.Span{
			TraceID: strconv.Itoa(i),
			Event: types.Event{
				Dataset: "aoeu",
				Data:    types.NewPayload(coll.Config, spandata[i]),
				APIKey:  legacyAPIKey,
			},
		}

		// TODO: enable this once we want to turn on DisableTraceLocality
		//	if i < 3 {
		//		// add some spans that belongs to peer
		//		span.TraceID = peerTraceIDs[i]
		//		// add extrac data so that the peer traces have bigger
		//		// cache impact, which will get evicted first
		//		span.Data["extra_data"] = strings.Repeat("abc", 100)
		//	}

		coll.AddSpan(span)
	}

	// Wait for spans to be processed into cache
	assert.Eventually(t, func() bool {
		totalCacheSize := 0
		for _, loop := range coll.collectLoops {
			totalCacheSize += loop.GetCacheSize()
		}
		return totalCacheSize == 500
	}, 5*time.Second, 50*time.Millisecond, "Should have 500 traces in cache")

	// We want to induce an eviction event, so set MaxAlloc a bit below
	// our current post-GC alloc. In the parallel collector, each loop manages
	// its own memory limits, so we need to account for that.
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	// With parallel loops, memory management is more aggressive as each loop
	// gets 1/numLoops of the memory limit. Set a higher threshold to get
	// partial eviction rather than complete eviction.
	conf.SetMaxAlloc(config.MemorySize(mem.Alloc * 98 / 100)) // More conservative than 99.5%
	coll.reloadConfigs()

	// Wait for cache eviction to happen due to memory pressure
	var totalTraces int
	assert.Eventually(t, func() bool {
		totalTraces = 0
		for _, loop := range coll.collectLoops {
			totalTraces += loop.GetCacheSize()
		}
		return totalTraces < 500 // Wait for any eviction to start
	}, 3*time.Second, 100*time.Millisecond, "Cache should evict some traces due to memory pressure")

	// TODO: enable this once we want to turn on DisableTraceLocality
	//	peerTracesLeft := 0
	//	for _, trace := range traces {
	//		if slices.Contains(peerTraceIDs, trace.TraceID) {
	//			peerTracesLeft++
	//		}
	//	}
	//	assert.Equal(t, 2, peerTracesLeft, "should have kept the peer traces")

	// We discarded the most costly spans, and sent them.
	assert.Equal(t, 500-totalTraces, len(transmission.GetBlock(500-totalTraces)), "should have sent traces that weren't kept")
}

// TestAddSpanNoBlock tests non-blocking span addition behavior when channels are full
func TestAddSpanNoBlock(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(10 * time.Minute),
			MaxBatchSize: 500,
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{},
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			NumCollectLoops:   2,
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 10,
			PeerQueueSize:     10,
		},
	}

	coll := newTestCollector(t, conf)

	stc, err := newCache()
	require.NoError(t, err)

	coll.sampleTraceCache = stc

	// Block the collect loop so nothing gets processed
	ch := make(chan struct{})
	defer close(ch)
	coll.collectLoops[0].pause <- ch

	span := &types.Span{
		TraceID: "1",
		Event: types.Event{
			Dataset: "aoeu",
			APIKey:  legacyAPIKey,
		},
	}

	// Fill the channel
	for range cap(coll.collectLoops[0].incoming) {
		err := coll.AddSpan(span)
		assert.NoError(t, err)
		err = coll.AddSpanFromPeer(span)
		assert.NoError(t, err)
	}

	// Errors instead of blocking
	err = coll.AddSpan(span)
	assert.Error(t, err)
	err = coll.AddSpanFromPeer(span)
	assert.Error(t, err)
}

// TestDependencyInjection tests that the collector can be properly instantiated through dependency injection
func TestDependencyInjection(t *testing.T) {
	var g inject.Graph
	err := g.Provide(
		&inject.Object{Value: &InMemCollector{}},
		&inject.Object{Value: &config.MockConfig{}},
		&inject.Object{Value: &logger.NullLogger{}},
		&inject.Object{Value: noop.NewTracerProvider().Tracer("test"), Name: "tracer"},
		&inject.Object{Value: clockwork.NewFakeClock()},
		&inject.Object{Value: &health.Health{}},
		&inject.Object{Value: &sharder.SingleServerSharder{}},
		&inject.Object{Value: &transmit.MockTransmission{}, Name: "upstreamTransmission"},
		&inject.Object{Value: &transmit.MockTransmission{}, Name: "peerTransmission"},
		&inject.Object{Value: &pubsub.LocalPubSub{}},
		&inject.Object{Value: &metrics.NullMetrics{}, Name: "metrics"},
		&inject.Object{Value: &sample.SamplerFactory{}},
		&inject.Object{Value: &MockStressReliever{}, Name: "stressRelief"},
		&inject.Object{Value: &peer.MockPeers{}},
	)
	if err != nil {
		t.Error(err)
	}
	if err := g.Populate(); err != nil {
		t.Error(err)
	}
}

// TestAddCountsToRoot tests that adding a root span winds up with a trace object in
// the cache and that trace gets span count, span event count, span link count, and event count added to it
// This test also makes sure that AddCountsToRoot overrides the AddSpanCountToRoot config.
func TestAddCountsToRoot(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(60 * time.Second),
			MaxBatchSize: 500,
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		AddSpanCountToRoot: true,
		AddCountsToRoot:    true,
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			NumCollectLoops:   2,
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 9,
			PeerQueueSize:     9,
		},
	}

	coll := newTestCollector(t, conf)
	transmission := coll.Transmission.(*transmit.MockTransmission)

	var traceID = "mytrace"
	for i := 0; i < 4; i++ {
		span := &types.Span{
			TraceID: traceID,
			Event: types.Event{
				Dataset: "aoeu",
				Data: types.NewPayload(coll.Config, map[string]interface{}{
					"trace.parent_id": "unused",
				}),
				APIKey: legacyAPIKey,
			},
		}
		switch i {
		case 0, 1:
			span.Data.MetaAnnotationType = "span_event"
		case 2:
			span.Data.MetaAnnotationType = "link"
		}

		coll.AddSpanFromPeer(span)
	}

	// Add the root span to trigger trace processing
	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    types.NewPayload(coll.Config, nil),
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(rootSpan)

	// Try to get all available events
	events := transmission.GetBlock(5) // 0 means get all available

	// Find the root span - it's the one without trace.parent_id
	var rootEvent *types.Event
	var childEvents []*types.Event
	for _, ev := range events {
		if ev.Data.Get("trace.parent_id") == nil {
			rootEvent = ev
		} else {
			childEvents = append(childEvents, ev)
		}
	}

	assert.NotNil(t, rootEvent, "should have found a root span")
	assert.Equal(t, 4, len(childEvents), "should have found 4 child spans")

	// Check child spans don't have counts
	for _, child := range childEvents {
		assert.Equal(t, nil, child.Data.Get("meta.span_count"), "child span metadata should NOT be populated with span count")
		assert.Equal(t, nil, child.Data.Get("meta.span_event_count"), "child span metadata should NOT be populated with span event count")
		assert.Equal(t, nil, child.Data.Get("meta.span_link_count"), "child span metadata should NOT be populated with span link count")
		assert.Equal(t, nil, child.Data.Get("meta.event_count"), "child span metadata should NOT be populated with event count")
	}

	// Check root span has counts
	assert.Equal(t, int64(2), rootEvent.Data.Get("meta.span_count"), "root span metadata should be populated with span count")
	assert.Equal(t, int64(2), rootEvent.Data.Get("meta.span_event_count"), "root span metadata should be populated with span event count")
	assert.Equal(t, int64(1), rootEvent.Data.Get("meta.span_link_count"), "root span metadata should be populated with span link count")
	assert.Equal(t, int64(5), rootEvent.Data.Get("meta.event_count"), "root span metadata should be populated with event count")
	assert.Nil(t, getFromCache(coll, traceID), "after adding a leaf and root span, it should be removed from the cache")
}

// TestLateRootGetsCounts tests that the root span gets decorated with the right counts
// even if the trace had already been sent
func TestLateRootGetsCounts(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(5 * time.Millisecond),
			MaxBatchSize: 500,
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:    &config.DeterministicSamplerConfig{SampleRate: 1},
		AddSpanCountToRoot:   true,
		AddCountsToRoot:      true,
		TraceIdFieldNames:    []string{"trace.trace_id", "traceId"},
		ParentIdFieldNames:   []string{"trace.parent_id", "parentId"},
		AddRuleReasonToTrace: true,
		GetCollectionConfigVal: config.CollectionConfig{
			NumCollectLoops:   2,
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 10,
			PeerQueueSize:     10,
		},
	}

	coll := newTestCollector(t, conf)
	transmission := coll.Transmission.(*transmit.MockTransmission)

	var traceID = "mytrace"

	for i := 0; i < 4; i++ {
		span := &types.Span{
			TraceID: traceID,
			Event: types.Event{
				Dataset: "aoeu",
				Data: types.NewPayload(coll.Config, map[string]interface{}{
					"trace.parent_id": "unused",
				}),
				APIKey: legacyAPIKey,
			},
		}
		switch i {
		case 0, 1:
			span.Data.MetaAnnotationType = "span_event"
		case 2:
			span.Data.MetaAnnotationType = "link"
		}

		coll.AddSpanFromPeer(span)
	}

	childSpans := transmission.GetBlock(4)
	assert.Equal(t, 4, len(childSpans), "adding a non-root span and waiting should send the span")
	assert.Equal(t, nil, childSpans[0].Data.Get("meta.span_count"), "child span metadata should NOT be populated with span count")
	assert.Equal(t, nil, childSpans[1].Data.Get("meta.span_count"), "child span metadata should NOT be populated with span count")
	assert.Equal(t, nil, childSpans[1].Data.Get("meta.span_event_count"), "child span metadata should NOT be populated with span event count")
	assert.Equal(t, nil, childSpans[1].Data.Get("meta.span_link_count"), "child span metadata should NOT be populated with span link count")
	assert.Equal(t, nil, childSpans[1].Data.Get("meta.event_count"), "child span metadata should NOT be populated with event count")
	trace := getFromCache(coll, traceID)
	assert.Nil(t, trace, "trace should have been sent although the root span hasn't arrived")

	// now we add the root span and verify that both got sent and that the root span had the span count
	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    types.NewPayload(coll.Config, nil),
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(rootSpan)

	event := transmission.GetBlock(1)
	assert.Equal(t, 1, len(event), "adding a root span should send all spans in the trace")
	assert.Equal(t, int64(2), event[0].Data.Get("meta.span_count"), "root span metadata should be populated with span count")
	assert.Equal(t, int64(2), event[0].Data.Get("meta.span_event_count"), "root span metadata should be populated with span event count")
	assert.Equal(t, int64(1), event[0].Data.Get("meta.span_link_count"), "root span metadata should be populated with span link count")
	assert.Equal(t, int64(5), event[0].Data.Get("meta.event_count"), "root span metadata should be populated with event count")
	assert.Equal(t, "deterministic/always - late arriving span", event[0].Data.Get("meta.refinery.reason"), "late spans should have meta.refinery.reason set to rules + late arriving span.")
	assert.Nil(t, getFromCache(coll, traceID), "after adding a leaf and root span, it should be removed from the cache")
}

// TestAddSpanCount tests that adding a root span winds up with a trace object in
// the cache and that that trace gets span count added to it
func TestAddSpanCount(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(60 * time.Second),
			MaxBatchSize: 500,
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		AddSpanCountToRoot: true,
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			NumCollectLoops:   2,
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}
	coll := newTestCollector(t, conf)
	transmission := coll.Transmission.(*transmit.MockTransmission)

	var traceID = "mytrace"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: types.NewPayload(coll.Config, map[string]interface{}{
				"trace.parent_id": "unused",
			}),
			APIKey: legacyAPIKey,
		},
	}
	span.Data.ExtractMetadata()

	coll.AddSpanFromPeer(span)

	assert.EventuallyWithT(t,
		func(cT *assert.CollectT) {
			trace := getFromCache(coll, traceID)
			require.NotNil(cT, trace)
			assert.Equal(cT, traceID, trace.TraceID)
		},
		conf.GetTracesConfig().GetSendTickerValue()*60, // waitFor
		conf.GetTracesConfig().GetSendTickerValue()*2,  // tick
		"within a reasonable time, cache should contain a traceID matching the one from the span we added",
	)
	assert.Equal(t, 0, len(transmission.GetBlock(0)), "adding a non-root span should not yet send the span")

	// ok now let's add the root span and verify that both got sent
	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    types.NewPayload(coll.Config, nil),
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	rootSpan.Data.ExtractMetadata()
	coll.AddSpan(rootSpan)

	events := transmission.GetBlock(2)
	assert.Equal(t, 2, len(events), "adding a root span should send all spans in the trace")
	assert.Equal(t, nil, events[0].Data.Get("meta.span_count"), "child span metadata should NOT be populated with span count")
	assert.Equal(t, int64(2), events[1].Data.Get("meta.span_count"), "root span metadata should be populated with span count")
	assert.Nil(t, getFromCache(coll, traceID), "after adding a leaf and root span, it should be removed from the cache")
}

// TestLateRootGetsSpanCount tests that the root span gets decorated with the right span count
// even if the trace had already been sent
func TestLateRootGetsSpanCount(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(5 * time.Millisecond),
			MaxBatchSize: 500,
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:    &config.DeterministicSamplerConfig{SampleRate: 1},
		AddSpanCountToRoot:   true,
		ParentIdFieldNames:   []string{"trace.parent_id", "parentId"},
		TraceIdFieldNames:    []string{"trace.trace_id", "traceId"},
		AddRuleReasonToTrace: true,
		GetCollectionConfigVal: config.CollectionConfig{
			NumCollectLoops:   2,
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}
	coll := newTestCollector(t, conf)
	transmission := coll.Transmission.(*transmit.MockTransmission)

	var traceID = "mytrace"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: types.NewPayload(coll.Config, map[string]interface{}{
				"trace.parent_id": "unused",
			}),
			APIKey: legacyAPIKey,
		},
	}
	coll.AddSpanFromPeer(span)

	childSpans := transmission.GetBlock(1)
	assert.Equal(t, 1, len(childSpans), "adding a non-root span and waiting should send the span")
	assert.Equal(t, nil, childSpans[0].Data.Get("meta.span_count"), "child span metadata should NOT be populated with span count")

	// now we add the root span and verify that both got sent and that the root span had the span count
	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    types.NewPayload(coll.Config, nil),
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(rootSpan)

	events := transmission.GetBlock(1)
	assert.Equal(t, 1, len(events), "adding a root span should send all spans in the trace")
	assert.Equal(t, int64(2), events[0].Data.Get("meta.span_count"), "root span metadata should be populated with span count")
	assert.Equal(t, "deterministic/always - late arriving span", events[0].Data.Get("meta.refinery.reason"), "late spans should have meta.refinery.reason set to late.")

	assert.Nil(t, getFromCache(coll, traceID), "after adding a leaf and root span, it should be removed from the cache")
}

// TestLateRootNotDecorated tests that spans do not get decorated with 'meta.refinery.reason' meta field
// if the AddRuleReasonToTrace attribute not set in config
func TestLateSpanNotDecorated(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(5 * time.Minute),
			MaxBatchSize: 500,
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		GetCollectionConfigVal: config.CollectionConfig{
			NumCollectLoops:   2,
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}

	coll := newTestCollector(t, conf)
	transmission := coll.Transmission.(*transmit.MockTransmission)

	var traceID = "traceABC"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: types.NewPayload(coll.Config, map[string]interface{}{
				"trace.parent_id": "unused",
			}),
			APIKey: legacyAPIKey,
		},
	}
	coll.AddSpanFromPeer(span)

	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    types.NewPayload(coll.Config, nil),
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(rootSpan)

	events := transmission.GetBlock(2)
	assert.Equal(t, 2, len(events), "adding a root span should send all spans in the trace")
	if len(events) == 2 {
		assert.Equal(t, nil, events[1].Data.Get("meta.refinery.reason"), "late span should not have meta.refinery.reason set to late")
	}
}

// TestAddAdditionalAttributes tests adding additional attributes to spans
func TestAddAdditionalAttributes(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(60 * time.Second),
			MaxBatchSize: 500,
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		AdditionalAttributes: map[string]string{
			"name":  "foo",
			"other": "bar",
		},
		GetCollectionConfigVal: config.CollectionConfig{
			NumCollectLoops:   2,
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}

	coll := newTestCollector(t, conf)
	transmission := coll.Transmission.(*transmit.MockTransmission)

	var traceID = "trace123"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: types.NewPayload(coll.Config, map[string]interface{}{
				"trace.parent_id": "unused",
			}),
			APIKey: legacyAPIKey,
		},
	}
	coll.AddSpanFromPeer(span)

	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    types.NewPayload(coll.Config, nil),
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(rootSpan)

	events := transmission.GetBlock(2)
	assert.Equal(t, 2, len(events), "should be some events transmitted")
	assert.Equal(t, "foo", events[0].Data.Get("name"), "new attribute should appear in data")
	assert.Equal(t, "bar", events[0].Data.Get("other"), "new attribute should appear in data")
}

// TestStressReliefSampleRate tests stress relief mode sample rate adjustment
func TestStressReliefSampleRate(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(5 * time.Minute),
			MaxBatchSize: 500,
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			NumCollectLoops: 2,
			ShutdownDelay:   config.Duration(1 * time.Millisecond),
		},
	}

	coll := newTestCollector(t, conf)
	transmission := coll.Transmission.(*transmit.MockTransmission)

	stc, err := newCache()
	require.NoError(t, err)
	coll.sampleTraceCache = stc

	var traceID = "traceABC"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: types.NewPayload(coll.Config, map[string]interface{}{
				"trace.parent_id": "unused",
			}),
			APIKey: legacyAPIKey,
		},
	}
	coll.StressRelief = &MockStressReliever{
		IsStressed:              true,
		SampleDeterministically: true,
		ShouldKeep:              true,
		SampleRate:              100,
	}
	processed, kept := coll.ProcessSpanImmediately(span)
	require.True(t, processed)
	require.True(t, kept)

	tr, _, found := coll.sampleTraceCache.CheckTrace(traceID)
	require.True(t, found)
	require.NotNil(t, tr)
	assert.Equal(t, uint(100), tr.Rate())

	events := transmission.GetBlock(1)
	assert.Equal(t, 1, len(events), "span should immediately be sent during stress relief")
	assert.Equal(t, uint(100), events[0].SampleRate)

	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset:    "aoeu",
			Data:       types.NewPayload(coll.Config, nil),
			APIKey:     legacyAPIKey,
			SampleRate: 10,
		},
		IsRoot: true,
	}

	processed2, kept2 := coll.ProcessSpanImmediately(rootSpan)
	require.True(t, processed2)
	require.True(t, kept2)

	tr2, _, found2 := coll.sampleTraceCache.CheckTrace(traceID)
	require.True(t, found2)
	require.NotNil(t, tr2)
	assert.Equal(t, uint(100), tr2.Rate())
	eventsWithRoot := transmission.GetBlock(1)
	assert.Equal(t, 1, len(eventsWithRoot), "span should immediately be sent during stress relief")
	assert.Equal(t, uint(1000), eventsWithRoot[0].SampleRate)
}

// TestStressReliefDecorateHostname tests that the span gets decorated with hostname if
// StressReliefMode is active
func TestStressReliefDecorateHostname(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(5 * time.Minute),
			MaxBatchSize: 500,
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		StressRelief: config.StressReliefConfig{
			Mode:              "monitor",
			ActivationLevel:   75,
			DeactivationLevel: 25,
			SamplingRate:      100,
		},
		GetCollectionConfigVal: config.CollectionConfig{
			NumCollectLoops:   2,
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}

	coll := newTestCollector(t, conf)
	transmission := coll.Transmission.(*transmit.MockTransmission)

	coll.hostname = "host123"

	var traceID = "traceABC"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: types.NewPayload(coll.Config, map[string]interface{}{
				"trace.parent_id": "unused",
			}),
			APIKey: legacyAPIKey,
		},
	}
	coll.AddSpanFromPeer(span)

	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    types.NewPayload(coll.Config, nil),
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(rootSpan)

	events := transmission.GetBlock(2)
	assert.Equal(t, 2, len(events), "adding a root span should send all spans in the trace")
	assert.Equal(t, "host123", events[1].Data.Get("meta.refinery.local_hostname"))

}

// TestSpanWithRuleReasons tests span decoration with sampling rule reasons
func TestSpanWithRuleReasons(t *testing.T) {
	conf := &config.MockConfig{
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(5 * time.Millisecond),
			MaxBatchSize: 500,
		},
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
		TraceIdFieldNames:    []string{"trace.trace_id", "traceId"},
		ParentIdFieldNames:   []string{"trace.parent_id", "parentId"},
		AddRuleReasonToTrace: true,
		GetCollectionConfigVal: config.CollectionConfig{
			NumCollectLoops:   2,
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 10,
			PeerQueueSize:     10,
		},
	}

	coll := newTestCollector(t, conf)
	transmission := coll.Transmission.(*transmit.MockTransmission)

	traceIDs := []string{"trace1", "trace2"}

	for i := 0; i < 4; i++ {
		span := &types.Span{
			Event: types.Event{
				Dataset: "aoeu",
				Data: types.NewPayload(coll.Config, map[string]interface{}{
					"trace.parent_id":  "unused",
					"http.status_code": 200,
				}),
				APIKey: legacyAPIKey,
			},
		}
		switch i {
		case 0, 1:
			span.TraceID = traceIDs[0]
			span.Data.Set("test", int64(1))
		case 2, 3:
			span.TraceID = traceIDs[1]
			span.Data.Set("test", int64(2))
		}
		coll.AddSpanFromPeer(span)
	}

	eventsWithoutRoot := transmission.GetBlock(4)
	assert.Equal(t, 4, len(eventsWithoutRoot), "traces should have been sent due to trace timeout")
	for _, event := range eventsWithoutRoot {
		reason := event.Data.Get("meta.refinery.reason")
		if event.Data.Get("test") == int64(1) {
			assert.Equal(t, "rules/trace/rule 1:dynamic", reason, event.Data)
		} else {
			assert.Equal(t, "rules/span/rule 2:emadynamic", reason, event.Data)
		}
	}

	for i, traceID := range traceIDs {
		assert.Nil(t, getFromCache(coll, traceID), "trace should have been sent although the root span hasn't arrived")
		rootSpan := &types.Span{
			TraceID: traceID,
			Event: types.Event{
				Dataset: "aoeu",
				Data: types.NewPayload(coll.Config, map[string]interface{}{
					"http.status_code": 200,
				}),
				APIKey: legacyAPIKey,
			},
			IsRoot: true,
		}
		if i == 0 {
			rootSpan.Data.Set("test", int64(1))
		} else {
			rootSpan.Data.Set("test", int64(2))
		}

		coll.AddSpan(rootSpan)
	}
	// now we add the root span and verify that both got sent and that the root span had the span count

	roots := transmission.GetBlock(2)
	assert.Equal(t, 2, len(roots), "root span should be sent immediately")
	for _, event := range roots {
		reason := event.Data.Get("meta.refinery.reason")
		if event.Data.Get("test") == int64(1) {
			assert.Equal(t, "rules/trace/rule 1:dynamic - late arriving span", reason, event.Data)
		} else {
			assert.Equal(t, "rules/span/rule 2:emadynamic - late arriving span", reason, event.Data)
		}
	}

}

// TestBigTracesGoEarly tests that traces exceeding span limits are sent early
func TestBigTracesGoEarly(t *testing.T) {
	spanlimit := 200
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(10 * time.Millisecond),
			TraceTimeout: config.Duration(500 * time.Millisecond),
			SpanLimit:    uint(spanlimit - 1),
			MaxBatchSize: 1500,
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:    &config.DeterministicSamplerConfig{SampleRate: 2},
		AddSpanCountToRoot:   true,
		AddCountsToRoot:      true,
		ParentIdFieldNames:   []string{"trace.parent_id", "parentId"},
		TraceIdFieldNames:    []string{"trace.trace_id", "traceId"},
		AddRuleReasonToTrace: true,
		GetCollectionConfigVal: config.CollectionConfig{
			NumCollectLoops:   2,
			IncomingQueueSize: 500,
			PeerQueueSize:     500,
		},
	}

	coll := newTestCollector(t, conf)
	transmission := coll.Transmission.(*transmit.MockTransmission)

	// this name was chosen to be Kept with the deterministic/2 sampler
	var traceID = "myTrace"

	for i := 0; i < spanlimit; i++ {
		span := &types.Span{
			TraceID: traceID,
			Event: types.Event{
				Dataset: "aoeu",
				Data: types.NewPayload(coll.Config, map[string]interface{}{
					"trace.parent_id": "unused",
					"index":           i,
				}),
				APIKey: legacyAPIKey,
			},
		}
		coll.AddSpanFromPeer(span)
	}

	// wait for all the events to be transmitted
	childEvents := transmission.GetBlock(spanlimit)
	assert.Equal(t, spanlimit, len(childEvents), "hitting the spanlimit should send the trace")

	// now we add the root span and verify that it got sent and that the root span had the span count
	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    types.NewPayload(coll.Config, nil),
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(rootSpan)

	rootEvents := transmission.GetBlock(1)
	assert.Equal(t, 1, len(rootEvents), "hitting the spanlimit should send the trace")
	assert.Equal(t, nil, childEvents[0].Data.Get("meta.span_count"), "child span metadata should NOT be populated with span count")
	assert.Equal(t, "trace_send_span_limit", childEvents[0].Data.Get("meta.refinery.send_reason"), "child span metadata should set to trace_send_span_limit")
	assert.EqualValues(t, spanlimit+1, rootEvents[0].Data.Get("meta.span_count"), "root span metadata should be populated with span count")
	assert.EqualValues(t, spanlimit+1, rootEvents[0].Data.Get("meta.event_count"), "root span metadata should be populated with event count")
	assert.Equal(t, "deterministic/chance - late arriving span", rootEvents[0].Data.Get("meta.refinery.reason"), "the late root span should have meta.refinery.reason set to rules + late arriving span.")
	assert.EqualValues(t, 2, rootEvents[0].SampleRate, "the late root span should sample rate set")
	assert.Equal(t, "trace_send_late_span", rootEvents[0].Data.Get("meta.refinery.send_reason"), "send reason should indicate span count exceeded")

}

// TestSpanLimitSendByPreservation tests that once a trace has hit its span limit and been sent,
// late arriving spans with earlier SendBy values won't update the trace's SendBy time
func TestSpanLimitSendByPreservation(t *testing.T) {
	// Set up a configuration with a small span limit
	spanLimit := 2
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(10 * time.Second),
			SendDelay:    config.Duration(10 * time.Second),
			TraceTimeout: config.Duration(60 * time.Second),
			SpanLimit:    uint(spanLimit),
			MaxBatchSize: 500,
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			NumCollectLoops: 2,
			ShutdownDelay:   config.Duration(1 * time.Millisecond),
		},
	}

	clock := clockwork.NewFakeClock()
	coll := newTestCollector(t, conf, clock)

	// Pause the collect loop so we can manipulate the cache
	ch := make(chan struct{})
	defer close(ch)
	coll.collectLoops[0].pause <- ch

	sampleTraceCache, err := newCache()
	require.NoError(t, err)

	coll.sampleTraceCache = sampleTraceCache

	traceID := "span-limit-trace"

	// Pre-create the trace with a known SendBy time
	now := clock.Now()
	initialSendBy := now.Add(10 * time.Second)
	trace := &types.Trace{
		TraceID:     traceID,
		Dataset:     "test-dataset",
		APIKey:      "test-api-key",
		SendBy:      initialSendBy,
		ArrivalTime: now,
	}
	for i := 0; i <= spanLimit; i++ {
		trace.AddSpan(&types.Span{
			TraceID: traceID,
			Event: types.Event{
				Dataset: "test-dataset",
				Data: types.NewPayload(coll.Config, map[string]interface{}{
					"trace.parent_id": "unused",
				}),
				APIKey: legacyAPIKey,
			},
		})
	}
	// Add trace to the loop's cache
	coll.collectLoops[0].cache.Set(trace)

	clock.Advance(5 * time.Second)
	// process another span for the same trace that exceeds the span limit should not change the SendBy time
	lateSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "test-dataset",
			APIKey:  legacyAPIKey,
		},
	}
	// Route span to appropriate loop for processing
	coll.collectLoops[0].processSpan(context.Background(), lateSpan)

	updatedTrace := coll.collectLoops[0].cache.Get(traceID)
	require.Equal(t, trace.SendBy.Unix(), updatedTrace.SendBy.Unix())
}

// BenchmarkCollectorWithSamplers runs benchmarks for different sampler configurations.
// This is a tricky benchmark to interpret because just setting up the input data
// can easily be more expensive than the collector's routing code. The goal is to
// demonstrate the collector's (in-)ability to scale across CPUs. It does NOT work
// well with just one CPU.
func BenchmarkCollectorWithSamplers(b *testing.B) {
	// Common test scenarios to run for each sampler
	scenarios := []struct {
		name          string
		spansPerTrace int
	}{
		{"small_traces", 100},
		{"large_traces", 10_000},
	}

	// Different sampler configurations to test
	samplerConfigs := []struct {
		name   string
		config any
	}{
		{
			"deterministic",
			&config.DeterministicSamplerConfig{SampleRate: 1},
		},
		{
			"dynamic",
			&config.DynamicSamplerConfig{
				SampleRate: 1,
				FieldList:  []string{"sampler-field-1", "sampler-field-2"},
			},
		},
		{
			"rulesbased_expensive",
			&config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "expensive_regex_rule",
						Scope:      "trace",
						SampleRate: 1,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "large_field_1",
								Operator: config.MatchesRegexp,
								Value:    "x{50,200}.*x{50,200}",
							},
							{
								Field:    "service.name",
								Operator: config.In,
								Value:    []interface{}{"service-0", "service-1", "service-2"},
								Datatype: "string",
							},
							{
								Fields:   []string{"string.field_0", "string.field_1", "string.field_2", "string.field_3", "string.field_4"},
								Operator: config.Contains,
								Value:    "string_value",
							},
						},
					},
					{
						Name:       "expensive_multi_field_rule",
						Scope:      "trace",
						SampleRate: 1,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "large_field_2",
								Operator: config.MatchesRegexp,
								Value:    "y+.*y+.*y+.*y+",
							},
							{
								Fields:   []string{"int.field_0", "int.field_1", "int.field_2", "int.field_3", "int.field_4", "int.field_5", "int.field_6", "int.field_7", "int.field_8", "int.field_9"},
								Operator: config.GTE,
								Value:    5,
								Datatype: "int",
							},
							{
								Fields:   []string{"float.field_10", "float.field_11", "float.field_12", "float.field_13", "float.field_14"},
								Operator: config.LT,
								Value:    15.0,
								Datatype: "float",
							},
							{
								Field:    "large_field_3",
								Operator: config.DoesNotContain,
								Value:    "nonexistent",
							},
						},
					},
					{
						Name:       "default",
						Scope:      "trace",
						SampleRate: 1,
					},
				},
			},
		},
	}

	// Run benchmarks for each sampler with each scenario
	for _, sampler := range samplerConfigs {
		for _, scenario := range scenarios {
			benchName := fmt.Sprintf("%s/%s", sampler.name, scenario.name)
			b.Run(benchName, func(b *testing.B) {
				b.StopTimer()

				collector := setupBenchmarkCollector(b, sampler.config)
				sender := &mockSender{
					spanQueue: make(chan *types.Span, 1000),
				}
				collector.Transmission = sender

				b.N = scenario.spansPerTrace * int(math.Ceil(float64(b.N)/float64(scenario.spansPerTrace)))

				// Setup done channel that waits for all spans to be processed
				wg := &sync.WaitGroup{}
				wg.Add(1)
				go func() {
					sender.waitForCount(b.N)
					wg.Done()
				}()

				b.StartTimer()
				var traceID string
				for n := range b.N {
					if n%scenario.spansPerTrace == 0 {
						// Start a new trace
						traceID = fmt.Sprintf("trace-%d", n)
					}
					// Create spans for this trace
					isRoot := n%scenario.spansPerTrace == scenario.spansPerTrace-1 // Last span is root

					span := createBenchmarkSpans(traceID, n%scenario.spansPerTrace, isRoot, collector.Config)
					collector.AddSpan(span)
				}

				// Wait for all spans to be processed
				wg.Wait()
				b.StopTimer()
			})
		}
	}
}

// Why not a sync.Pool? We're handling a lot of points in-flight and empirically
// a sync.Pool gets emptied by the garbage collector too aggressively. A channel
// allows us to guarantee that nothing in the pool is ever GC'd.
var benchmarkSpanPool = make(chan *types.Span, 25_000)

func createBenchmarkSpans(traceID string, spanIdx int, isRoot bool, cfg config.Config) *types.Span {
	// Recycle spans through a pool, changing only the fields we really need to
	// per use. This means that the various other fields in there will carry over
	// between uses and won't be "correct" relative to the spanIdx, but the benchmark
	// should be structured such that we don't really care. It also means that
	// memoized values for trace_id etc within the Payload are incorrect, so we
	// can't use those for our sampler rules.
	var span *types.Span
	select {
	case span = <-benchmarkSpanPool:
	default:
		// Large text values for testing
		largeText := strings.Repeat("x", 1000)
		mediumText := strings.Repeat("y", 500)
		smallText := strings.Repeat("z", 100)

		data := make(map[string]any)

		// fields used for sampling
		data["sampler-field-1"] = rand.Intn(20)
		data["sampler-field-2"] = "static-value"

		data["service.name"] = fmt.Sprintf("service-%d", spanIdx%3)
		data["duration_ms"] = 100.0

		data["meta.signal_type"] = "trace"
		data["meta.annotation_type"] = "span"
		data["meta.refinery.incoming_user_agent"] = "refinery/v2.4.1"

		// Large text fields (3 large fields)
		data["large_field_1"] = largeText
		data["large_field_2"] = mediumText
		data["large_field_3"] = smallText

		// many string, float, int, and bool fields
		for j := 0; j < 25; j++ {
			data[fmt.Sprintf("string.field_%d", j)] = fmt.Sprintf("string_value_%d", j)
			data[fmt.Sprintf("int.field_%d", j)] = int64(j)
			data[fmt.Sprintf("float.field_%d", j)] = float64(j)
			data[fmt.Sprintf("bool.field_%d", j)] = (spanIdx+j)%2 == 0
		}

		// Array field
		data["tags"] = []string{
			fmt.Sprintf("tag_%d", spanIdx),
			fmt.Sprintf("tag_%d", spanIdx+1),
			fmt.Sprintf("tag_%d", spanIdx+2),
		}

		// Nested map field
		data["custom.metadata"] = map[string]interface{}{
			"nested_field_1": map[string]interface{}{
				"key1": fmt.Sprintf("value_%d", spanIdx),
				"key2": spanIdx * 10,
				"key3": spanIdx%2 == 0,
			},
			"nested_field_2": map[string]interface{}{
				"key1": true,
				"key2": false,
				"key3": spanIdx + 100,
			},
		}
		payload := types.NewPayload(cfg, data)
		payload.ExtractMetadata()

		span = &types.Span{
			Event: types.Event{
				Dataset: "benchmark-dataset",
				APIKey:  "test-api-key",
				Data:    payload,
			},
		}
	}

	span.TraceID = traceID
	span.Event.Data.MetaTraceID = traceID
	span.IsRoot = isRoot
	span.Event.Data.MetaRefineryRoot.Set(isRoot)

	// Core tracing fields (always present, differs per trace)
	parentID := ""
	if spanIdx%3 != 0 { // 2/3 of spans have parents
		parentID = fmt.Sprintf("span-%018d", spanIdx-1)
	}
	span.Event.Data.Set("trace.trace_id", traceID)
	span.Event.Data.Set("trace.span_id", fmt.Sprintf("span-%018d", spanIdx))
	if isRoot {
		span.Event.Data.UnsetForTest("trace.parent_id")
	} else {
		span.Event.Data.Set("trace.parent_id", parentID)
	}

	return span
}

func setupBenchmarkCollector(b *testing.B, samplerConfig any) *InMemCollector {
	conf := &config.MockConfig{
		DryRun: true,
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(5 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(60 * time.Second),
			MaxBatchSize: 500,
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100000,
			DroppedSize:       1000,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:  samplerConfig,
		TraceIdFieldNames:  []string{"trace.trace_id", "traceId"},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay:      config.Duration(1 * time.Millisecond),
			HealthCheckTimeout: config.Duration(100 * time.Millisecond),
			IncomingQueueSize:  100000,
			PeerQueueSize:      100000,
			NumCollectLoops:    16,
		},
		AddCountsToRoot:        true,
		AddSpanCountToRoot:     true,
		AddRuleReasonToTrace:   true,
		AddHostMetadataToTrace: true,
	}

	coll := newTestCollector(b, conf)
	coll.BlockOnAddSpan = true

	return coll
}

// mockSender is a mock implementation of the Transmission
type mockSender struct {
	spanQueue chan *types.Span
}

func (c *mockSender) EnqueueEvent(event *types.Event) {
	panic("unimplemented")
}

func (c *mockSender) EnqueueSpan(span *types.Span) {
	select {
	case c.spanQueue <- span:
	}
}

func (c *mockSender) Flush() {
	// No-op for mock sender
}

func (c *mockSender) RegisterMetrics() {
	// No-op for mock sender
}

func (c *mockSender) waitForCount(target int) {
	var count int
	for {
		select {
		case span := <-c.spanQueue:
			select {
			case benchmarkSpanPool <- span:
			default:
			}
			count += 1
			if count >= target {
				return
			}
		}
	}
}

package collect

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
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
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/sharder"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"
)

const legacyAPIKey = "c9945edf5d245834089a1bd6cc9ad01e"

func newCache() (cache.TraceSentCache, error) {
	cfg := config.SampleCacheConfig{
		KeptSize:          100,
		DroppedSize:       100,
		SizeCheckInterval: config.Duration(1 * time.Second),
	}

	return cache.NewCuckooSentCache(cfg, &metrics.NullMetrics{})
}

func newTestCollector(conf config.Config, transmission transmit.Transmission, peerTransmission transmit.Transmission) *InMemCollector {
	s := &metrics.MockMetrics{}
	s.Start()
	clock := clockwork.NewRealClock()
	healthReporter := &health.Health{
		Clock: clock,
	}
	healthReporter.Start()

	return &InMemCollector{
		Config:           conf,
		Clock:            clock,
		Logger:           &logger.NullLogger{},
		Tracer:           noop.NewTracerProvider().Tracer("test"),
		Health:           healthReporter,
		Transmission:     transmission,
		PeerTransmission: peerTransmission,
		Metrics:          &metrics.NullMetrics{},
		StressRelief:     &MockStressReliever{},
		SamplerFactory: &sample.SamplerFactory{
			Config:  conf,
			Metrics: s,
			Logger:  &logger.NullLogger{},
		},
		done: make(chan struct{}),
		Peers: &peer.MockPeers{
			Peers: []string{"api1", "api2"},
		},
		Sharder: &sharder.MockSharder{
			Self: &sharder.TestShard{
				Addr: "api1",
			},
		},
		redistributeTimer: newRedistributeNotifier(&logger.NullLogger{}, &metrics.NullMetrics{}, clock),
	}
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
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay: config.Duration(1 * time.Millisecond),
		},
	}
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	go coll.sendTraces()

	defer coll.Stop()

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
	// * remove the trace from the cache
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()

		require.Equal(collect, 1, len(transmission.Events), "adding a root span should send the span")
		assert.Equal(collect, "aoeu", transmission.Events[0].Dataset, "sending a root span should immediately send that span via transmission")

	}, conf.GetTracesConfig().GetSendTickerValue()*8, conf.GetTracesConfig().GetSendTickerValue()*2)
	assert.Nil(t, coll.getFromCache(traceID1), "after sending the span, it should be removed from the cache")

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
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()
		require.Equal(collect, 2, len(transmission.Events), "adding another root span should send the span")
		assert.Equal(collect, "aoeu", transmission.Events[1].Dataset, "sending a root span should immediately send that span via transmission")
	}, conf.GetTracesConfig().GetSendTickerValue()*10, conf.GetTracesConfig().GetSendTickerValue()*2)

	assert.Nil(t, coll.getFromCache(traceID1), "after sending the span, it should be removed from the cache")

	decisionSpanTraceID := "decision_root_span"
	span = &types.Span{
		TraceID: decisionSpanTraceID,
		Event: types.Event{
			Dataset: "aoeu",
			APIKey:  legacyAPIKey,
			Data: map[string]interface{}{
				"meta.refinery.min_span": true,
			},
		},
		IsRoot: true,
	}

	coll.AddSpanFromPeer(span)
	// adding one root decision span with no parent ID should:
	// * create the trace in the cache
	// * send the trace
	// * remove the trace from the cache
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()
		assert.Equal(collect, 2, len(transmission.Events), "adding a root decision span should send the trace but not the decision span itself")
	}, conf.GetTracesConfig().GetSendTickerValue()*5, conf.GetTracesConfig().GetSendTickerValue())

	assert.Nil(t, coll.getFromCache(decisionSpanTraceID), "after sending the span, it should be removed from the cache")
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
			TraceTimeout: config.Duration(1 * time.Second),
			MaxBatchSize: 500,
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: expectedDeterministicSampleRate},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay: config.Duration(1 * time.Millisecond),
		},
	}
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	go coll.sendTraces()

	defer coll.Stop()

	// Generate events until one is sampled and appears on the transmission queue for sending.
	sendAttemptCount := 0
	for getEventsLength(transmission) < 1 {
		sendAttemptCount++
		span := &types.Span{
			TraceID: fmt.Sprintf("trace-%v", sendAttemptCount),
			Event: types.Event{
				Dataset:    "aoeu",
				APIKey:     legacyAPIKey,
				SampleRate: originalSampleRate,
				Data:       make(map[string]interface{}),
			},
		}
		err := coll.AddSpan(span)
		require.NoError(t, err, "must be able to add the span")
		time.Sleep(conf.GetTracesConfig().GetSendTickerValue() * 5)
	}

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
	err = coll.AddSpan(&types.Span{
		TraceID: fmt.Sprintf("trace-%v", 1000),
		Event: types.Event{
			Dataset:    "no-upstream-sampling",
			APIKey:     legacyAPIKey,
			SampleRate: 0, // no upstream sampling
			Data:       make(map[string]interface{}),
		},
		IsRoot: true,
	})
	require.NoError(t, err, "must be able to add the span")

	// Find the Refinery-sampled-and-sent event that had no upstream sampling which
	// should be the last event on the transmission queue.
	var noUpstreamSampleRateEvent *types.Event
	require.Eventually(t, func() bool {
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()
		noUpstreamSampleRateEvent = transmission.Events[len(transmission.Events)-1]
		return noUpstreamSampleRateEvent.Dataset == "no-upstream-sampling"
	}, 5*time.Second, conf.GetTracesConfig().GetSendTickerValue()*2, "the event with no upstream sampling should have appeared in the transmission queue by now")

	assert.Nil(t, noUpstreamSampleRateEvent.Data["meta.refinery.original_sample_rate"],
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
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay: config.Duration(1 * time.Millisecond),
		},
	}
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	go coll.sendTraces()

	defer coll.Stop()

	span := &types.Span{
		TraceID: fmt.Sprintf("trace-%v", 1),
		Event: types.Event{
			Dataset:    "aoeu",
			APIKey:     legacyAPIKey,
			SampleRate: 0, // This should get lifted to 1
			Data:       make(map[string]interface{}),
		},
		IsRoot: true,
	}

	coll.AddSpan(span)

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()
		require.Greater(collect, len(transmission.Events), 0)
		assert.Equal(collect, uint(1), transmission.Events[0].SampleRate,
			"SampleRate should be reset to one after starting at zero")
	}, conf.GetTracesConfig().GetSendTickerValue()*8, conf.GetTracesConfig().GetSendTickerValue()*2)
}

func getEventsLength(transmission *transmit.MockTransmission) int {
	transmission.Mux.RLock()
	defer transmission.Mux.RUnlock()

	return len(transmission.Events)
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
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay: config.Duration(1 * time.Millisecond),
		},
	}
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	go coll.sendTraces()

	defer coll.Stop()

	var traceID = "mytrace"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: map[string]interface{}{
				"trace.parent_id": "unused",
			},
			APIKey: legacyAPIKey,
		},
	}
	coll.AddSpanFromPeer(span)

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		trace := coll.getFromCache(traceID)
		require.NotNil(collect, trace)
		assert.Equal(collect, traceID, trace.TraceID, "after adding the span, we should have a trace in the cache with the right trace ID")

		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()

		assert.Equal(collect, 0, len(transmission.Events), "adding a non-root span should not yet send the span")
	}, conf.GetTracesConfig().GetSendTickerValue()*5, conf.GetTracesConfig().GetSendTickerValue())

	// ok now let's add the root span and verify that both got sent
	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    map[string]interface{}{},
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(rootSpan)

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Nil(collect, coll.getFromCache(traceID), "after adding a leaf and root span, it should be removed from the cache")
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()
		assert.Equal(collect, 2, len(transmission.Events), "adding a root span should send all spans in the trace")
	}, conf.GetTracesConfig().GetSendTickerValue()*5, conf.GetTracesConfig().GetSendTickerValue())

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
		GetSamplerTypeVal: &config.DeterministicSamplerConfig{
			SampleRate: 10,
		},
		DryRun:             true,
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay: config.Duration(1 * time.Millisecond),
		},
	}
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	samplerFactory := &sample.SamplerFactory{
		Config: conf,
		Logger: &logger.NullLogger{},
	}
	sampler := samplerFactory.GetSamplerImplementationForKey("test", true)
	coll.SamplerFactory = samplerFactory
	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	go coll.sendTraces()

	defer coll.Stop()

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
			Data:   map[string]interface{}{},
			APIKey: legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(span)

	// adding one span with no parent ID should:
	// * create the trace in the cache
	// * send the trace
	// * remove the trace from the cache
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Nil(collect, coll.getFromCache(traceID1), "after sending the span, it should be removed from the cache")
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()

		require.Equal(collect, 1, len(transmission.Events), "adding a root span should send the span")
		assert.Equal(collect, keepTraceID1, transmission.Events[0].Data[config.DryRunFieldName], "config.DryRunFieldName should match sampling decision for its trace ID")
	}, conf.GetTracesConfig().GetSendTickerValue()*5, conf.GetTracesConfig().GetSendTickerValue()*2)

	// add a non-root span, create the trace in the cache
	span = &types.Span{
		TraceID: traceID2,
		Event: types.Event{
			Dataset: "aoeu",
			Data: map[string]interface{}{
				"trace.parent_id": "unused",
			},
			APIKey: legacyAPIKey,
		},
	}
	coll.AddSpanFromPeer(span)

	assert.Eventually(t, func() bool {
		return traceID2 == coll.getFromCache(traceID2).TraceID
	}, conf.GetTracesConfig().GetSendTickerValue()*6, conf.GetTracesConfig().GetSendTickerValue()*2, "after adding the span, we should have a trace in the cache with the right trace ID")

	span = &types.Span{
		TraceID: traceID2,
		Event: types.Event{
			Data:   map[string]interface{}{},
			APIKey: legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpanFromPeer(span)

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		// adding root span to send the trace
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()
		require.Equal(collect, 3, len(transmission.Events), "adding another root span should send the span")
		// both spanscollectshould be marked with the sampling decision
		assert.Equal(collect, keepTraceID2, transmission.Events[1].Data[config.DryRunFieldName], "config.DryRunFieldName should match sampling decision for its trace ID")
		assert.Equal(collect, keepTraceID2, transmission.Events[2].Data[config.DryRunFieldName], "config.DryRunFieldName should match sampling decision for its trace ID")
		// check that meta value associated with dry run mode is properly applied
		assert.Equal(collect, uint(10), transmission.Events[1].Data["meta.dryrun.sample_rate"])
		// check expecollectted sampleRate against span data
		assert.Equal(collect, sampleRate1, transmission.Events[0].Data["meta.dryrun.sample_rate"])
		assert.Equal(collect, sampleRate2, transmission.Events[1].Data["meta.dryrun.sample_rate"])
	}, conf.GetTracesConfig().GetSendTickerValue()*8, conf.GetTracesConfig().GetSendTickerValue()*2)

	span = &types.Span{
		TraceID: traceID3,
		Event: types.Event{
			Data:   map[string]interface{}{},
			APIKey: legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(span)

	// adding one span with no parent ID should:
	// * create the trace in the cache
	// * send the trace
	// * remove the trace from the cache
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Nil(collect, coll.getFromCache(traceID3), "after sending the span, it should be removed from the cache")
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()
		require.Equal(collect, 4, len(transmission.Events), "adding a root span should send the span")
		assert.Equal(collect, keepTraceID3, transmission.Events[3].Data[config.DryRunFieldName], "field should match sampling decision for its trace ID")
	}, conf.GetTracesConfig().GetSendTickerValue()*8, conf.GetTracesConfig().GetSendTickerValue()*2)

}

func TestCacheSizeReload(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(10 * time.Minute),
			MaxBatchSize: 500,
		},
		GetSamplerTypeVal: &config.DeterministicSamplerConfig{SampleRate: 1},
		GetCollectionConfigVal: config.CollectionConfig{
			CacheCapacity: 1,
			ShutdownDelay: config.Duration(1 * time.Millisecond),
		},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)
	coll.Peers = &peer.MockPeers{}

	err := coll.Start()
	assert.NoError(t, err)
	defer coll.Stop()

	event := types.Event{
		Dataset: "dataset",
		Data: map[string]interface{}{
			"trace.parent_id": "1",
		},
		APIKey: legacyAPIKey,
	}

	err = coll.AddSpan(&types.Span{TraceID: "1", Event: event})
	assert.NoError(t, err)
	err = coll.AddSpan(&types.Span{TraceID: "2", Event: event})
	assert.NoError(t, err)

	expectedEvents := 1
	wait := 1 * time.Second
	check := func() bool {
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()

		return len(transmission.Events) == expectedEvents
	}
	assert.Eventually(t, check, 60*wait, wait, "expected one trace evicted and sent")

	conf.Mux.Lock()
	conf.GetCollectionConfigVal.CacheCapacity = 2
	conf.Mux.Unlock()
	conf.Reload()

	assert.Eventually(t, func() bool {
		coll.mutex.RLock()
		defer coll.mutex.RUnlock()
		return coll.cache.GetCacheCapacity() == 2
	}, 60*wait, wait, "cache size to change")

	err = coll.AddSpan(&types.Span{TraceID: "3", Event: event})
	assert.NoError(t, err)
	time.Sleep(5 * conf.GetTracesConfig().GetSendTickerValue())
	assert.Eventually(t, func() bool {
		return check()
	}, 8*conf.GetTracesConfig().GetSendTickerValue(), 4*conf.GetTracesConfig().GetSendTickerValue(), "expected no more traces evicted and sent")

	conf.Mux.Lock()
	conf.GetCollectionConfigVal.CacheCapacity = 1
	conf.Mux.Unlock()
	conf.Reload()

	expectedEvents = 2
	assert.Eventually(t, check, 60*wait, wait, "expected another trace evicted and sent")
}

func TestSampleConfigReload(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(60 * time.Second),
			MaxBatchSize: 500,
		},
		GetSamplerTypeVal:      &config.DeterministicSamplerConfig{SampleRate: 1},
		ParentIdFieldNames:     []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{CacheCapacity: 10, ShutdownDelay: config.Duration(1 * time.Millisecond)},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	err := coll.Start()
	assert.NoError(t, err)
	defer coll.Stop()

	dataset := "aoeu"

	span := &types.Span{
		TraceID: "1",
		Event: types.Event{
			Dataset: dataset,
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}

	coll.AddSpan(span)

	assert.Eventually(t, func() bool {
		coll.mutex.Lock()
		_, ok := coll.datasetSamplers[dataset]
		coll.mutex.Unlock()

		return ok
	}, conf.GetTracesConfig().GetTraceTimeout()*2, conf.GetTracesConfig().GetSendTickerValue())

	conf.Reload()

	assert.Eventually(t, func() bool {
		coll.mutex.Lock()
		_, ok := coll.datasetSamplers[dataset]
		coll.mutex.Unlock()
		return !ok
	}, conf.GetTracesConfig().GetTraceTimeout()*2, conf.GetTracesConfig().GetSendTickerValue())

	span = &types.Span{
		TraceID: "2",
		Event: types.Event{
			Dataset: dataset,
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}

	coll.AddSpan(span)

	assert.Eventually(t, func() bool {
		coll.mutex.Lock()
		_, ok := coll.datasetSamplers[dataset]
		coll.mutex.Unlock()
		return ok
	}, conf.GetTracesConfig().GetTraceTimeout()*2, conf.GetTracesConfig().GetSendTickerValue())
}

func TestStableMaxAlloc(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(10 * time.Minute),
			MaxBatchSize: 500,
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay: config.Duration(1 * time.Millisecond),
			CacheCapacity: 1000,
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	spandata := make([]map[string]interface{}, 500)
	for i := 0; i < 500; i++ {
		spandata[i] = map[string]interface{}{
			"trace.parent_id": "unused",
			"id":              i,
			"str1":            strings.Repeat("abc", rand.Intn(100)+1),
			"str2":            strings.Repeat("def", rand.Intn(100)+1),
		}
	}

	c := cache.NewInMemCache(1000, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 1000)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	go coll.sendTraces()

	defer coll.Stop()

	for i := 0; i < 500; i++ {
		span := &types.Span{
			TraceID: strconv.Itoa(i),
			Event: types.Event{
				Dataset: "aoeu",
				Data:    spandata[i],
				APIKey:  legacyAPIKey,
			},
		}
		coll.AddSpan(span)
	}

	for len(coll.incoming) > 0 {
		time.Sleep(conf.GetTracesConfig().GetSendTickerValue())
	}

	// Now there should be 500 traces in the cache.
	coll.mutex.Lock()
	assert.Equal(t, 500, len(coll.cache.GetAll()))

	// We want to induce an eviction event, so set MaxAlloc a bit below
	// our current post-GC alloc.
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	// Set MaxAlloc, which should cause cache evictions.
	conf.GetCollectionConfigVal.MaxAlloc = config.MemorySize(mem.Alloc * 99 / 100)
	coll.mutex.Unlock()
	// wait for the cache to take some action
	var traces []*types.Trace
	for {
		coll.mutex.Lock()
		traces = coll.cache.GetAll()
		if len(traces) < 500 {
			break
		}
		coll.mutex.Unlock()

		time.Sleep(conf.GetTracesConfig().GetSendTickerValue())
	}

	assert.Equal(t, 1000, coll.cache.GetCacheCapacity(), "cache size shouldn't change")

	tracesLeft := len(traces)
	assert.Less(t, tracesLeft, 480, "should have sent some traces")
	assert.Greater(t, tracesLeft, 100, "should have NOT sent some traces")
	coll.mutex.Unlock()

	// We discarded the most costly spans, and sent them.
	totalEvents := getEventsLength(transmission)
	assert.Equal(t, 500-len(traces), totalEvents, "should have sent traces that weren't kept")
}

func TestAddSpanNoBlock(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(10 * time.Minute),
			MaxBatchSize: 500,
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay: config.Duration(1 * time.Millisecond),
			CacheCapacity: 10,
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	c := cache.NewInMemCache(10, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 3)
	coll.fromPeer = make(chan *types.Span, 3)
	coll.datasetSamplers = make(map[string]sample.Sampler)

	// Don't start collect(), so the queues are never drained
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
		err = coll.AddSpanFromPeer(span)
		assert.NoError(t, err)
	}

	err = coll.AddSpan(span)
	assert.Error(t, err)
	err = coll.AddSpanFromPeer(span)
	assert.Error(t, err)
}

func TestDependencyInjection(t *testing.T) {
	var g inject.Graph
	err := g.Provide(
		&inject.Object{Value: &InMemCollector{}},
		&inject.Object{Value: &config.MockConfig{}},
		&inject.Object{Value: &logger.NullLogger{}},
		&inject.Object{Value: noop.NewTracerProvider().Tracer("test"), Name: "tracer"},
		&inject.Object{Value: clockwork.NewRealClock()},
		&inject.Object{Value: &health.Health{}},
		&inject.Object{Value: &sharder.SingleServerSharder{}},
		&inject.Object{Value: &transmit.MockTransmission{}, Name: "upstreamTransmission"},
		&inject.Object{Value: &transmit.MockTransmission{}, Name: "peerTransmission"},
		&inject.Object{Value: &metrics.NullMetrics{}, Name: "genericMetrics"},
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
// the cache and that that trace gets span count, span event count, span link count, and event count added to it
// This test also makes sure that AddCountsToRoot overrides the AddSpanCountToRoot config.
func TestAddCountsToRoot(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(60 * time.Second),
			MaxBatchSize: 500,
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		AddSpanCountToRoot: true,
		AddCountsToRoot:    true,
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay: config.Duration(1 * time.Millisecond),
			CacheCapacity: 3,
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	go coll.sendTraces()

	defer coll.Stop()

	var traceID = "mytrace"
	for i := 0; i < 4; i++ {
		span := &types.Span{
			TraceID: traceID,
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
		coll.AddSpanFromPeer(span)
	}

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Equal(collect, traceID, coll.getFromCache(traceID).TraceID, "after adding the span, we should have a trace in the cache with the right trace ID")
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()

		assert.Equal(collect, 0, len(transmission.Events), "adding a non-root span should not yet send the span")
	}, conf.GetTracesConfig().GetSendTickerValue()*6, conf.GetTracesConfig().GetSendTickerValue()*2)

	// ok now let's add the root span and verify that both got sent
	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    map[string]interface{}{},
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(rootSpan)
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Nil(collect, coll.getFromCache(traceID), "after adding a leaf and root span, it should be removed from the cache")
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()
		assert.Equal(collect, 5, len(transmission.Events), "adding a root span should send all spans in the trace")
		assert.Equal(collect, nil, transmission.Events[0].Data["meta.span_count"], "child span metadata should NOT be populated with span count")
		assert.Equal(collect, nil, transmission.Events[1].Data["meta.span_count"], "child span metadata should NOT be populated with span count")
		assert.Equal(collect, nil, transmission.Events[1].Data["meta.span_event_count"], "child span metadata should NOT be populated with span event count")
		assert.Equal(collect, nil, transmission.Events[1].Data["meta.span_link_count"], "child span metadata should NOT be populated with span link count")
		assert.Equal(collect, nil, transmission.Events[1].Data["meta.event_count"], "child span metadata should NOT be populated with event count")
		assert.Equal(collect, int64(2), transmission.Events[4].Data["meta.span_count"], "root span metadata should be populated with span count")
		assert.Equal(collect, int64(2), transmission.Events[4].Data["meta.span_event_count"], "root span metadata should be populated with span event count")
		assert.Equal(collect, int64(1), transmission.Events[4].Data["meta.span_link_count"], "root span metadata should be populated with span link count")
		assert.Equal(collect, int64(5), transmission.Events[4].Data["meta.event_count"], "root span metadata should be populated with event count")
	}, conf.GetTracesConfig().GetSendTickerValue()*6, conf.GetTracesConfig().GetSendTickerValue()*2)

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
		GetSamplerTypeVal:    &config.DeterministicSamplerConfig{SampleRate: 1},
		AddSpanCountToRoot:   true,
		AddCountsToRoot:      true,
		ParentIdFieldNames:   []string{"trace.parent_id", "parentId"},
		AddRuleReasonToTrace: true,
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay: config.Duration(1 * time.Millisecond),
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	go coll.sendTraces()
	defer coll.Stop()

	var traceID = "mytrace"

	for i := 0; i < 4; i++ {
		span := &types.Span{
			TraceID: traceID,
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
		coll.AddSpanFromPeer(span)
	}

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		trace := coll.getFromCache(traceID)
		assert.Nil(collect, trace, "trace should have been sent although the root span hasn't arrived")
		transmission.Mux.RLock()
		assert.Equal(collect, 4, len(transmission.Events), "adding a non-root span and waiting should send the span")
		transmission.Mux.RUnlock()
	}, conf.GetTracesConfig().GetSendTickerValue()*12, conf.GetTracesConfig().GetSendTickerValue()*3)

	// now we add the root span and verify that both got sent and that the root span had the span count
	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    map[string]interface{}{},
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(rootSpan)

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Nil(collect, coll.getFromCache(traceID), "after adding a leaf and root span, it should be removed from the cache")
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()
		assert.Equal(collect, 5, len(transmission.Events), "adding a root span should send all spans in the trace")
		assert.Equal(collect, nil, transmission.Events[0].Data["meta.span_count"], "child span metadata should NOT be populated with span count")
		assert.Equal(collect, nil, transmission.Events[1].Data["meta.span_count"], "child span metadata should NOT be populated with span count")
		assert.Equal(collect, nil, transmission.Events[1].Data["meta.span_event_count"], "child span metadata should NOT be populated with span event count")
		assert.Equal(collect, nil, transmission.Events[1].Data["meta.span_link_count"], "child span metadata should NOT be populated with span link count")
		assert.Equal(collect, nil, transmission.Events[1].Data["meta.event_count"], "child span metadata should NOT be populated with event count")
		assert.Equal(collect, int64(2), transmission.Events[4].Data["meta.span_count"], "root span metadata should be populated with span count")
		assert.Equal(collect, int64(2), transmission.Events[4].Data["meta.span_event_count"], "root span metadata should be populated with span event count")
		assert.Equal(collect, int64(1), transmission.Events[4].Data["meta.span_link_count"], "root span metadata should be populated with span link count")
		assert.Equal(collect, int64(5), transmission.Events[4].Data["meta.event_count"], "root span metadata should be populated with event count")
		assert.Equal(collect, "deterministic/always - late arriving span", transmission.Events[4].Data["meta.refinery.reason"], "late spans should have meta.refinery.reason set to rules + late arriving span.")
	}, conf.GetTracesConfig().GetSendTickerValue()*6, conf.GetTracesConfig().GetSendTickerValue()*2)

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
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		AddSpanCountToRoot: true,
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay: config.Duration(1 * time.Millisecond),
		},
	}
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	go coll.sendTraces()

	defer coll.Stop()

	var traceID = "mytrace"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: map[string]interface{}{
				"trace.parent_id": "unused",
			},
			APIKey: legacyAPIKey,
		},
	}
	decisionSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: map[string]interface{}{
				"trace.parent_id":        "unused",
				"meta.refinery.min_span": true,
			},
			APIKey: legacyAPIKey,
		},
	}
	coll.AddSpanFromPeer(span)
	coll.AddSpanFromPeer(decisionSpan)

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Equal(collect, traceID, coll.getFromCache(traceID).TraceID, "after adding the span, we should have a trace in the cache with the right trace ID")
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()
		assert.Equal(collect, 0, len(transmission.Events), "adding a non-root span should not yet send the span")
	}, conf.GetTracesConfig().GetSendTickerValue()*6, conf.GetTracesConfig().GetSendTickerValue()*2)

	// ok now let's add the root span and verify that both got sent
	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    map[string]interface{}{},
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(rootSpan)

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Nil(collect, coll.getFromCache(traceID), "after adding a leaf and root span, it should be removed from the cache")
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()
		assert.Equal(collect, 2, len(transmission.Events), "adding a root span should send all spans in the trace")
		assert.Equal(collect, nil, transmission.Events[0].Data["meta.span_count"], "child span metadata should NOT be populated with span count")
		assert.Equal(collect, int64(3), transmission.Events[1].Data["meta.span_count"], "root span metadata should be populated with span count")
	}, conf.GetTracesConfig().GetSendTickerValue()*6, conf.GetTracesConfig().GetSendTickerValue()*2)

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
		GetSamplerTypeVal:    &config.DeterministicSamplerConfig{SampleRate: 1},
		AddSpanCountToRoot:   true,
		ParentIdFieldNames:   []string{"trace.parent_id", "parentId"},
		AddRuleReasonToTrace: true,
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay: config.Duration(1 * time.Millisecond),
		},
	}
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	go coll.sendTraces()

	defer coll.Stop()

	var traceID = "mytrace"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: map[string]interface{}{
				"trace.parent_id": "unused",
			},
			APIKey: legacyAPIKey,
		},
	}
	coll.AddSpanFromPeer(span)

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()
		assert.Equal(collect, 1, len(transmission.Events), "adding a non-root span and waiting should send the span")
	}, conf.GetTracesConfig().GetSendTickerValue()*12, conf.GetTracesConfig().GetSendTickerValue()*2)

	// now we add the root span and verify that both got sent and that the root span had the span count
	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    map[string]interface{}{},
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(rootSpan)

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()
		assert.Equal(collect, 2, len(transmission.Events), "adding a root span should send all spans in the trace")
		assert.Equal(collect, nil, transmission.Events[0].Data["meta.span_count"], "child span metadata should NOT be populated with span count")
		assert.Equal(collect, int64(2), transmission.Events[1].Data["meta.span_count"], "root span metadata should be populated with span count")
		assert.Equal(collect, "deterministic/always - late arriving span", transmission.Events[1].Data["meta.refinery.reason"], "late spans should have meta.refinery.reason set to late.")
	}, 5*conf.GetTracesConfig().GetSendTickerValue(), conf.GetTracesConfig().GetSendTickerValue())

	assert.Nil(t, coll.getFromCache(traceID), "after adding a leaf and root span, it should be removed from the cache")
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
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay: config.Duration(1 * time.Millisecond),
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	go coll.sendTraces()

	defer coll.Stop()

	var traceID = "traceABC"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: map[string]interface{}{
				"trace.parent_id": "unused",
			},
			APIKey: legacyAPIKey,
		},
	}
	coll.AddSpanFromPeer(span)

	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    map[string]interface{}{},
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(rootSpan)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		transmission.Mux.RLock()
		assert.Equal(c, 2, len(transmission.Events), "adding a root span should send all spans in the trace")
		if len(transmission.Events) == 2 {
			assert.Equal(c, nil, transmission.Events[1].Data["meta.refinery.reason"], "late span should not have meta.refinery.reason set to late")
		}
		transmission.Mux.RUnlock()
	}, 5*time.Second, conf.GetTracesConfig().GetSendTickerValue())
}

func TestAddAdditionalAttributes(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(60 * time.Second),
			MaxBatchSize: 500,
		},
		GetSamplerTypeVal: &config.DeterministicSamplerConfig{SampleRate: 1},
		AdditionalAttributes: map[string]string{
			"name":  "foo",
			"other": "bar",
		},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay: config.Duration(1 * time.Millisecond),
		},
	}
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	go coll.sendTraces()

	defer coll.Stop()

	var traceID = "trace123"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: map[string]interface{}{
				"trace.parent_id": "unused",
			},
			APIKey: legacyAPIKey,
		},
	}
	coll.AddSpanFromPeer(span)
	time.Sleep(conf.GetTracesConfig().GetSendTickerValue() * 3)

	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    map[string]interface{}{},
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(rootSpan)
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()
		assert.Equal(collect, 2, len(transmission.Events), "should be some events transmitted")
		assert.Equal(collect, "foo", transmission.Events[0].Data["name"], "new attribute should appear in data")
		assert.Equal(collect, "bar", transmission.Events[0].Data["other"], "new attribute should appear in data")
	}, conf.GetTracesConfig().GetSendTickerValue()*10, conf.GetTracesConfig().GetSendTickerValue()*2)

}

func TestStressReliefSampleRate(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(5 * time.Minute),
			MaxBatchSize: 500,
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay: config.Duration(1 * time.Millisecond),
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	var traceID = "traceABC"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: map[string]interface{}{
				"trace.parent_id": "unused",
			},
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

	transmission.Mux.RLock()
	assert.Equal(t, 1, len(transmission.Events), "span should immediately be sent during stress relief")
	assert.Equal(t, uint(100), transmission.Events[0].SampleRate)
	transmission.Mux.RUnlock()

	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset:    "aoeu",
			Data:       map[string]interface{}{},
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
	transmission.Mux.RLock()
	assert.Equal(t, 2, len(transmission.Events), "span should immediately be sent during stress relief")
	assert.Equal(t, uint(1000), transmission.Events[1].SampleRate)
	transmission.Mux.RUnlock()
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
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		StressRelief: config.StressReliefConfig{
			Mode:              "monitor",
			ActivationLevel:   75,
			DeactivationLevel: 25,
			SamplingRate:      100,
		},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay: config.Duration(1 * time.Millisecond),
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	coll.hostname = "host123"
	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	go coll.sendTraces()

	defer coll.Stop()

	var traceID = "traceABC"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: map[string]interface{}{
				"trace.parent_id": "unused",
			},
			APIKey: legacyAPIKey,
		},
	}
	coll.AddSpanFromPeer(span)
	time.Sleep(conf.GetTracesConfig().GetSendTickerValue() * 3)

	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    map[string]interface{}{},
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(rootSpan)

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()
		assert.Equal(collect, 2, len(transmission.Events), "adding a root span should send all spans in the trace")
		assert.Equal(collect, "host123", transmission.Events[1].Data["meta.refinery.local_hostname"])
	}, conf.GetTracesConfig().GetSendTickerValue()*6, conf.GetTracesConfig().GetSendTickerValue()*2)

}

func TestSpanWithRuleReasons(t *testing.T) {
	conf := &config.MockConfig{
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
		ParentIdFieldNames:   []string{"trace.parent_id", "parentId"},
		AddRuleReasonToTrace: true,
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay: config.Duration(1 * time.Millisecond),
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	go coll.sendTraces()

	defer coll.Stop()

	traceIDs := []string{"trace1", "trace2"}

	for i := 0; i < 4; i++ {
		span := &types.Span{
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
		coll.AddSpanFromPeer(span)
	}
	time.Sleep(conf.GetTracesConfig().GetSendTickerValue() * 10)

	for i, traceID := range traceIDs {
		assert.Nil(t, coll.getFromCache(traceID), "trace should have been sent although the root span hasn't arrived")
		rootSpan := &types.Span{
			TraceID: traceID,
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

		coll.AddSpan(rootSpan)
	}
	// now we add the root span and verify that both got sent and that the root span had the span count

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()
		assert.Equal(collect, 6, len(transmission.Events), "adding a root span should send all spans in the trace")
		for _, event := range transmission.Events {
			reason := event.Data["meta.refinery.reason"]
			if event.Data["test"] == int64(1) {
				if _, ok := event.Data["trace.parent_id"]; ok {
					assert.Equal(collect, "rules/trace/rule 1:dynamic", reason, event.Data)
				} else {
					assert.Equal(collect, "rules/trace/rule 1:dynamic - late arriving span", reason, event.Data)
				}
			} else {
				if _, ok := event.Data["trace.parent_id"]; ok {
					assert.Equal(collect, "rules/span/rule 2:emadynamic", reason, event.Data)
				} else {
					assert.Equal(collect, "rules/span/rule 2:emadynamic - late arriving span", reason, event.Data)
				}
			}
		}
	}, conf.GetTracesConfig().GetSendTickerValue()*6, conf.GetTracesConfig().GetSendTickerValue()*2)

}

func TestRedistributeTraces(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(1 * time.Second),
			SendTicker:   config.Duration(2 * time.Millisecond),
		},
		GetSamplerTypeVal:      &config.DeterministicSamplerConfig{SampleRate: 1},
		ParentIdFieldNames:     []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{CacheCapacity: 10},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	s := &sharder.MockSharder{
		Self: &sharder.TestShard{Addr: "api1"},
	}

	coll.Sharder = s

	err := coll.Start()
	assert.NoError(t, err)
	defer coll.Stop()

	dataset := "aoeu"

	span := &types.Span{
		TraceID: "1",
		Event: types.Event{
			Dataset: dataset,
			APIKey:  legacyAPIKey,
			APIHost: "api1",
			Data:    make(map[string]interface{}),
		},
	}

	coll.AddSpan(span)

	assert.Eventually(t, func() bool {
		transmission.Mux.Lock()
		defer transmission.Mux.Unlock()

		return len(transmission.Events) == 1 && transmission.Events[0].APIHost == "api1"
	}, conf.GetTracesConfig().GetTraceTimeout()*2, conf.GetTracesConfig().GetSendTickerValue())
	peerTransmission.Flush()

	s.Other = &sharder.TestShard{Addr: "api2"}
	span = &types.Span{
		TraceID: "11",
		Event: types.Event{
			Dataset: dataset,
			APIKey:  legacyAPIKey,
			Data:    make(map[string]interface{}),
		},
	}
	trace := &types.Trace{
		TraceID: span.TraceID,
		Dataset: dataset,
		SendBy:  coll.Clock.Now().Add(5 * time.Second),
	}
	trace.AddSpan(span)

	coll.mutex.Lock()
	coll.cache.Set(trace)
	coll.mutex.Unlock()
	coll.Peers.RegisterUpdatedPeersCallback(coll.redistributeTimer.Reset)

	assert.Eventually(t, func() bool {
		peerTransmission.Mux.Lock()
		defer peerTransmission.Mux.Unlock()
		if len(peerTransmission.Events) == 0 {
			return false
		}

		return len(peerTransmission.Events) == 1 && peerTransmission.Events[0].APIHost == "api2"
	}, conf.GetTracesConfig().GetTraceTimeout()*2, conf.GetTracesConfig().GetSendTickerValue())
}

func TestDrainTracesOnShutdown(t *testing.T) {
	// set up the trace cache
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(60 * time.Second),
			MaxBatchSize: 500,
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay: config.Duration(100 * time.Millisecond),
			CacheCapacity: 3,
		},
	}
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	coll.hostname = "host123"
	coll.Sharder = &sharder.MockSharder{
		Self:  &sharder.TestShard{Addr: "api1"},
		Other: &sharder.TestShard{Addr: "api2"},
	}

	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)

	coll.outgoingTraces = make(chan sendableTrace, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)

	sentTraceChan := make(chan sentRecord, 1)
	forwardTraceChan := make(chan *types.Span, 1)

	// test 1
	// the trace in cache already has decision made
	trace1 := &types.Trace{
		TraceID: "traceID1",
	}
	span1 := &types.Span{
		TraceID: "traceID1",
		Event: types.Event{
			Dataset: "aoeu",
			Data:    make(map[string]interface{}),
		},
	}

	stc.Record(trace1, true, "test")

	coll.distributeSpansOnShutdown(sentTraceChan, forwardTraceChan, span1)
	require.Len(t, sentTraceChan, 1)
	require.Len(t, forwardTraceChan, 0)

	ctx1, cancel1 := context.WithCancel(context.Background())
	go coll.sendSpansOnShutdown(ctx1, sentTraceChan, forwardTraceChan)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		transmission.Mux.Lock()
		defer transmission.Mux.Unlock()
		require.Len(collect, transmission.Events, 1)
		require.Equal(collect, span1.Dataset, transmission.Events[0].Dataset)
	}, 2*time.Second, 100*time.Millisecond)

	cancel1()
	transmission.Flush()

	// test 2
	// we can't make a decision for the trace yet, let's
	// forward it to its new home
	span2 := &types.Span{
		TraceID: "traceID2",
		Event: types.Event{
			Dataset: "test2",
			Data:    make(map[string]interface{}),
		},
	}

	coll.distributeSpansOnShutdown(sentTraceChan, forwardTraceChan, span2)
	require.Len(t, sentTraceChan, 0)
	require.Len(t, forwardTraceChan, 1)

	ctx2, cancel2 := context.WithCancel(context.Background())
	go coll.sendSpansOnShutdown(ctx2, sentTraceChan, forwardTraceChan)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		peerTransmission.Mux.Lock()
		defer peerTransmission.Mux.Unlock()
		require.Len(collect, peerTransmission.Events, 1)
		require.Equal(collect, span2.Dataset, peerTransmission.Events[0].Dataset)
		require.Equal(collect, "api2", peerTransmission.Events[0].APIHost)
	}, 2*time.Second, 100*time.Millisecond)
	cancel2()
}

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
		GetSamplerTypeVal:    &config.DeterministicSamplerConfig{SampleRate: 2},
		AddSpanCountToRoot:   true,
		AddCountsToRoot:      true,
		ParentIdFieldNames:   []string{"trace.parent_id", "parentId"},
		AddRuleReasonToTrace: true,
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 500)
	coll.fromPeer = make(chan *types.Span, 500)
	coll.outgoingTraces = make(chan sendableTrace, 500)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	go coll.sendTraces()

	defer coll.Stop()

	// this name was chosen to be Kept with the deterministic/2 sampler
	var traceID = "myTrace"

	for i := 0; i < spanlimit; i++ {
		span := &types.Span{
			TraceID: traceID,
			Event: types.Event{
				Dataset: "aoeu",
				Data: map[string]interface{}{
					"trace.parent_id": "unused",
					"index":           i,
				},
				APIKey: legacyAPIKey,
			},
		}
		coll.AddSpanFromPeer(span)
	}

	// wait for all the events to be transmitted
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		transmission.Mux.RLock()
		assert.Equal(collect, spanlimit, len(transmission.Events), "hitting the spanlimit should send the trace")
		transmission.Mux.RUnlock()
	}, 5*time.Second, 100*time.Millisecond)

	// now we add the root span and verify that it got sent and that the root span had the span count
	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    map[string]interface{}{},
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(rootSpan)

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()
		assert.Equal(collect, spanlimit+1, len(transmission.Events), "hitting the spanlimit should send the trace")
		require.Equal(t, spanlimit+1, len(transmission.Events), "adding a root span should send all spans in the trace")
		assert.Equal(t, nil, transmission.Events[0].Data["meta.span_count"], "child span metadata should NOT be populated with span count")
		assert.Equal(t, "trace_send_span_limit", transmission.Events[0].Data["meta.refinery.send_reason"], "child span metadata should set to trace_send_span_limit")
		assert.EqualValues(t, spanlimit+1, transmission.Events[spanlimit].Data["meta.span_count"], "root span metadata should be populated with span count")
		assert.EqualValues(t, spanlimit+1, transmission.Events[spanlimit].Data["meta.event_count"], "root span metadata should be populated with event count")
		assert.Equal(t, "deterministic/chance - late arriving span", transmission.Events[spanlimit].Data["meta.refinery.reason"], "the late root span should have meta.refinery.reason set to rules + late arriving span.")
		assert.EqualValues(t, 2, transmission.Events[spanlimit].SampleRate, "the late root span should sample rate set")
		assert.Equal(t, "trace_send_late_span", transmission.Events[spanlimit].Data["meta.refinery.send_reason"], "send reason should indicate span count exceeded")
	}, 5*time.Second, 100*time.Millisecond)

}

func TestCreateDecisionSpan(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(5 * time.Millisecond),
			MaxBatchSize: 500,
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	coll := newTestCollector(conf, transmission, peerTransmission)

	mockSampler := &sample.DynamicSampler{
		Config: &config.DynamicSamplerConfig{
			SampleRate: 1,
			FieldList:  []string{"http.status_code", "test"},
		}, Logger: coll.Logger, Metrics: coll.Metrics,
	}
	mockSampler.Start()

	coll.datasetSamplers = map[string]sample.Sampler{
		"aoeu": mockSampler,
	}

	traceID1 := "trace1"
	peerShard := &sharder.TestShard{Addr: "peer-address"}

	nonrootSpan := &types.Span{
		TraceID: traceID1,
		Event: types.Event{
			Dataset: "aoeu",
			Data: map[string]interface{}{
				"trace.parent_id":        "unused",
				"http.status_code":       200,
				"test":                   1,
				"should-not-be-included": 123,
			},
			APIKey: legacyAPIKey,
		},
	}

	trace := &types.Trace{
		TraceID: traceID1,
		Dataset: "aoeu",
		APIKey:  legacyAPIKey,
	}
	ds := coll.createDecisionSpan(nonrootSpan, trace, peerShard)

	expected := &types.Event{
		Dataset: "aoeu",
		APIHost: peerShard.Addr,
		APIKey:  legacyAPIKey,
		Data: map[string]interface{}{
			"meta.annotation_type":         types.SpanAnnotationTypeUnknown,
			"meta.refinery.min_span":       true,
			"meta.refinery.root":           false,
			"meta.refinery.span_data_size": 30,
			"trace_id":                     traceID1,

			"http.status_code": 200,
			"test":             1,
		},
	}

	assert.EqualValues(t, expected, ds)

	rootSpan := nonrootSpan
	rootSpan.IsRoot = true

	ds = coll.createDecisionSpan(rootSpan, trace, peerShard)
	expected.Data["meta.refinery.root"] = true
	assert.EqualValues(t, expected, ds)
}

package collect

import (
	"context"
	"fmt"
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
	localPubSub := &pubsub.LocalPubSub{
		Config:  conf,
		Metrics: s,
	}
	localPubSub.Start()
	redistributionDelay := time.Duration(conf.GetCollectionConfig().RedistributionDelay)
	if redistributionDelay == 0 {
		redistributionDelay = 2 * time.Millisecond
	}
	redistributeNotifier := newRedistributeNotifier(&logger.NullLogger{}, &metrics.NullMetrics{}, clock, redistributionDelay)

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
		Metrics:          &metrics.NullMetrics{},
		StressRelief:     &MockStressReliever{},
		SamplerFactory: &sample.SamplerFactory{
			Config:  conf,
			Metrics: s,
			Logger:  &logger.NullLogger{},
		},
		done:                 make(chan struct{}),
		keptDecisionMessages: make(chan string, 50),
		dropDecisionMessages: make(chan string, 50),
		dropDecisionBuffer:   make(chan TraceDecision, 5),
		keptDecisionBuffer:   make(chan TraceDecision, 5),
		Peers: &peer.MockPeers{
			Peers: []string{"api1", "api2"},
			ID:    "api1",
		},
		Sharder: &sharder.MockSharder{
			Self: &sharder.TestShard{
				Addr: "api1",
			},
			Other: &sharder.TestShard{
				Addr:     "api2",
				TraceIDs: peerTraceIDs,
			},
		},
		redistributeTimer: redistributeNotifier,
	}

	if !conf.GetCollectionConfig().TraceLocalityEnabled() {
		localPubSub.Subscribe(context.Background(), keptTraceDecisionTopic, c.signalKeptTraceDecisions)
	}

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
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
		GetAccessKeyConfigVal: config.AccessKeyConfig{
			SendKey:     "another-key",
			SendKeyMode: "all",
		},
	}
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)

	coll.Start()

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

	events := transmission.GetBlock(1)
	require.Equal(t, 1, len(events), "adding a root span should send the span")
	assert.Equal(t, "aoeu", events[0].Dataset, "sending a root span should immediately send that span via transmission")
	assert.Equal(t, "another-key", events[0].APIKey, "api key should be replaced with the send key")

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
	events = transmission.GetBlock(1)
	require.Equal(t, 1, len(events), "adding another root span should send the span")
	assert.Equal(t, "aoeu", events[0].Dataset, "sending a root span should immediately send that span via transmission")
	assert.Equal(t, "another-key", events[0].APIKey, "api key should be replaced with the send key")

	assert.Nil(t, coll.getFromCache(traceID1), "after sending the span, it should be removed from the cache")

	conf.Mux.Lock()
	collectionCfg := conf.GetCollectionConfigVal
	collectionCfg.TraceLocalityMode = "distributed"
	conf.GetCollectionConfigVal = collectionCfg
	conf.Mux.Unlock()

	decisionSpanTraceID := "decision_root_span"
	span = &types.Span{
		TraceID: decisionSpanTraceID,
		Event: types.Event{
			Dataset: "aoeu",
			APIKey:  legacyAPIKey,
			Data: types.NewPayload(map[string]interface{}{
				"meta.refinery.min_span": true,
			}),
		},
		IsRoot: true,
	}
	span.Event.Data.ExtractMetadata(nil, nil)

	coll.AddSpanFromPeer(span)
	// adding one root decision span with no parent ID should:
	// * create the trace in the cache
	// * send the trace
	// * remove the trace from the cache
	assert.Eventually(t, func() bool {
		return coll.getFromCache(decisionSpanTraceID) == nil
	}, time.Second*1, time.Millisecond*100, "after sending the span, it should be removed from the cache")

	events = transmission.GetBlock(0)
	assert.Equal(t, 0, len(events), "adding a root decision span should send the trace but not the decision span itself")

	peerSpan := &types.Span{
		TraceID: peerTraceIDs[0],
		Event: types.Event{
			Dataset: "aoeu",
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(peerSpan)

	// adding one span that belongs to peer with no parent ID should:
	// * create the trace in the cache
	// * a decision span is forwarded to peer
	peerEvents := peerTransmission.GetBlock(1)
	assert.Equal(t, 1, len(peerEvents), "adding a root span should send the span")
	assert.Equal(t, "aoeu", peerEvents[0].Dataset, "sending a root span should immediately send that span via transmission")
	assert.NotNil(t, coll.getFromCache(peerTraceIDs[0]), "trace belongs to peers should stay in current node cache")
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
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)

	coll.Start()
	defer coll.Stop()

	// Generate events until one is sampled and appears on the transmission queue for sending.
	sendAttemptCount := 0
	for len(transmission.Events) < 1 {
		sendAttemptCount++
		span := &types.Span{
			TraceID: fmt.Sprintf("trace-%v", sendAttemptCount),
			Event: types.Event{
				Dataset:    "aoeu",
				APIKey:     legacyAPIKey,
				SampleRate: originalSampleRate,
				Data:       types.NewPayload(make(map[string]interface{})),
			},
			IsRoot: true,
		}
		err := coll.AddSpan(span)
		require.NoError(t, err, "must be able to add the span")
		time.Sleep(conf.GetTracesConfig().GetSendTickerValue() * 5)
	}

	events := transmission.GetBlock(1)
	require.Equal(t, 1, len(events), "adding another root span should send the span")
	upstreamSampledEvent := events[0]

	assert.NotNil(t, upstreamSampledEvent)
	assert.Equal(t, originalSampleRate, upstreamSampledEvent.Data.Get("meta.refinery.original_sample_rate"),
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
			Data:       types.NewPayload(make(map[string]interface{})),
		},
		IsRoot: true,
	})
	require.NoError(t, err, "must be able to add the span")

	// Find the Refinery-sampled-and-sent event that had no upstream sampling which
	// should be the last event on the transmission queue.
	events = transmission.GetBlock(1)
	require.Equal(t, 1, len(events), "adding another root span should send the span")
	var noUpstreamSampleRateEvent *types.Event
	for _, event := range events {
		if event.Dataset == "no-upstream-sampling" {
			noUpstreamSampleRateEvent = event
			break
		}
	}

	require.NotNil(t, noUpstreamSampleRateEvent)
	assert.Equal(t, "no-upstream-sampling", noUpstreamSampleRateEvent.Dataset)
	assert.Nil(t, noUpstreamSampleRateEvent.Data.Get("meta.refinery.original_sample_rate"),
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
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)

	coll.Start()
	defer coll.Stop()

	span := &types.Span{
		TraceID: fmt.Sprintf("trace-%v", 1),
		Event: types.Event{
			Dataset:    "aoeu",
			APIKey:     legacyAPIKey,
			SampleRate: 0, // This should get lifted to 1
			Data:       types.NewPayload(make(map[string]interface{})),
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
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)

	coll.Start()
	defer coll.Stop()

	var traceID = "mytrace"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: types.NewPayload(map[string]interface{}{
				"trace.parent_id": "unused",
			}),
			APIKey: legacyAPIKey,
		},
	}
	span.Event.Data.ExtractMetadata(nil, nil)
	coll.AddSpanFromPeer(span)

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		trace := coll.getFromCache(traceID)
		require.NotNil(t, trace)
		assert.Equal(t, traceID, trace.TraceID, "after adding the span, we should have a trace in the cache with the right trace ID")
	}, time.Second*1, time.Millisecond)
	assert.Equal(t, 0, len(transmission.GetBlock(0)), "adding a non-root span should not yet send the span")

	sendBy := coll.Clock.Now().Add(10 * time.Second).Unix()
	spanFromPeer := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: types.NewPayload(map[string]interface{}{
				"trace.parent_id":        "unused",
				"meta.refinery.min_span": true,
				"meta.refinery.send_by":  sendBy,
			}),
			APIKey: legacyAPIKey,
		},
	}
	spanFromPeer.Event.Data.ExtractMetadata(nil, nil)

	coll.AddSpanFromPeer(spanFromPeer)
	time.Sleep(conf.GetTracesConfig().GetSendTickerValue() * 2)

	trace := coll.getFromCache(traceID)
	require.NotNil(t, trace)
	assert.Equal(t, int(trace.DescendantCount()), 2, "after adding a decision span, we should have 2 descendant in the trace")
	assert.Equal(t, trace.SendBy.Unix(), sendBy, "send_by should be the same as the one set from its peer")
	assert.Equal(t, traceID, trace.TraceID, "after adding the span, we should have a trace in the cache with the right trace ID")
	assert.Equal(t, 0, len(transmission.Events), "adding a non-root span should not yet send the span")

	// ok now let's add the root span and verify that both got sent
	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    types.NewPayload(map[string]interface{}{}),
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	rootSpan.Event.Data.ExtractMetadata(nil, nil)
	coll.AddSpan(rootSpan)

	assert.Equal(t, 2, len(transmission.GetBlock(2)), "adding a root span should send all spans in the trace")
	assert.Nil(t, coll.getFromCache(traceID), "after adding a leaf and root span, it should be removed from the cache")
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
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			TraceLocalityMode: "distributed",
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)

	samplerFactory := &sample.SamplerFactory{
		Config: conf,
		Logger: &logger.NullLogger{},
	}
	sampler := samplerFactory.GetSamplerImplementationForKey("test", true)
	coll.SamplerFactory = samplerFactory

	coll.Start()
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
			Data:   types.NewPayload(map[string]interface{}{}),
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
	assert.Nil(t, coll.getFromCache(traceID1), "after sending the span, it should be removed from the cache")

	// add a non-root span, create the trace in the cache
	span = &types.Span{
		TraceID: traceID2,
		Event: types.Event{
			Dataset: "aoeu",
			Data: types.NewPayload(map[string]interface{}{
				"trace.parent_id": "unused",
			}),
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
			Data:   types.NewPayload(map[string]interface{}{}),
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
			Data:   types.NewPayload(map[string]interface{}{}),
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
	assert.Nil(t, coll.getFromCache(traceID3), "after sending the span, it should be removed from the cache")

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
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
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
		SampleCache: config.SampleCacheConfig{
			KeptSize:          1000,
			DroppedSize:       1000,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 1000,
			PeerQueueSize:     5,
		},
	}

	transmission := &transmit.MockTransmission{
		Capacity: 1000,
	}
	transmission.Start()
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{
		Capacity: 1000,
	}
	peerTransmission.Start()
	defer peerTransmission.Stop()
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

	coll.Start()
	defer coll.Stop()

	for i := 0; i < 500; i++ {
		span := &types.Span{
			TraceID: strconv.Itoa(i),
			Event: types.Event{
				Dataset: "aoeu",
				Data:    types.NewPayload(spandata[i]),
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
	conf.SetMaxAlloc(config.MemorySize(mem.Alloc * 995 / 1000))
	coll.reloadConfigs()

	// TODO: enable this once we want to turn on DisableTraceLocality
	//	orphanPeerTrace := coll.cache.Get(peerTraceIDs[0])
	//
	//	orphanPeerTrace.SendBy = coll.Clock.Now().Add(-conf.GetTracesConfig().GetTraceTimeout() * 5)
	//	peerSpan := orphanPeerTrace.GetSpans()[0]
	//	// cache impact is also calculated based on the arrival time
	//	peerSpan.ArrivalTime = coll.Clock.Now().Add(-conf.GetTracesConfig().GetTraceTimeout())
	//	assert.True(t, orphanPeerTrace.IsOrphan(conf.GetTracesConfig().GetTraceTimeout(), coll.Clock.Now()))

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

	tracesLeft := len(traces)
	assert.Less(t, tracesLeft, 480, "should have sent some traces")
	assert.Greater(t, tracesLeft, 100, "should have NOT sent some traces")
	// TODO: enable this once we want to turn on DisableTraceLocality
	//	peerTracesLeft := 0
	//	for _, trace := range traces {
	//		if slices.Contains(peerTraceIDs, trace.TraceID) {
	//			peerTracesLeft++
	//		}
	//	}
	//	assert.Equal(t, 2, peerTracesLeft, "should have kept the peer traces")
	coll.mutex.Unlock()

	// We discarded the most costly spans, and sent them.
	assert.Equal(t, 500-len(traces), len(transmission.GetBlock(500-len(traces))), "should have sent traces that weren't kept")
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
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
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
		&inject.Object{Value: &pubsub.LocalPubSub{}},
		&inject.Object{Value: &metrics.NullMetrics{}, Name: "genericMetrics"},
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
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay: config.Duration(1 * time.Millisecond),
			CacheCapacity: 3,
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)

	coll.Start()
	defer coll.Stop()

	var traceID = "mytrace"
	for i := 0; i < 4; i++ {
		span := &types.Span{
			TraceID: traceID,
			Event: types.Event{
				Dataset: "aoeu",
				Data: types.NewPayload(map[string]interface{}{
					"trace.parent_id": "unused",
				}),
				APIKey: legacyAPIKey,
			},
		}
		switch i {
		case 0, 1:
			span.Data.MetaAnnotationType = "span_event"
			span.Data.MetaRefineryMinSpan.Set(true)
		case 2:
			span.Data.MetaAnnotationType = "link"
		}

		coll.AddSpanFromPeer(span)
	}

	time.Sleep(conf.GetTracesConfig().GetSendTickerValue() * 2)

	// ok now let's add the root span and verify that both got sent
	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    types.NewPayload(map[string]interface{}{}),
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(rootSpan)

	// Wait for the trace to be processed
	time.Sleep(conf.GetTracesConfig().GetSendTickerValue() * 2)

	// Try to get all available events
	events := transmission.GetBlock(0) // 0 means get all available
	t.Logf("Got %d events", len(events))

	// Debug: check what we got
	for i, ev := range events {
		parentID := ev.Data.Get("trace.parent_id")
		spanCount := ev.Data.Get("meta.span_count")
		t.Logf("Event %d: parent_id=%v, span_count=%v", i, parentID, spanCount)
	}

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
	assert.Equal(t, 2, len(childEvents), "should have found 2 child spans")

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
	assert.Nil(t, coll.getFromCache(traceID), "after adding a leaf and root span, it should be removed from the cache")
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
		ParentIdFieldNames:   []string{"trace.parent_id", "parentId"},
		AddRuleReasonToTrace: true,
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)

	coll.Start()
	defer coll.Stop()

	var traceID = "mytrace"

	for i := 0; i < 4; i++ {
		span := &types.Span{
			TraceID: traceID,
			Event: types.Event{
				Dataset: "aoeu",
				Data: types.NewPayload(map[string]interface{}{
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
			span.Data.MetaRefineryMinSpan.Set(true)
		}

		coll.AddSpanFromPeer(span)
	}

	childSpans := transmission.GetBlock(3)
	assert.Equal(t, 3, len(childSpans), "adding a non-root span and waiting should send the span")
	assert.Equal(t, nil, childSpans[0].Data.Get("meta.span_count"), "child span metadata should NOT be populated with span count")
	assert.Equal(t, nil, childSpans[1].Data.Get("meta.span_count"), "child span metadata should NOT be populated with span count")
	assert.Equal(t, nil, childSpans[1].Data.Get("meta.span_event_count"), "child span metadata should NOT be populated with span event count")
	assert.Equal(t, nil, childSpans[1].Data.Get("meta.span_link_count"), "child span metadata should NOT be populated with span link count")
	assert.Equal(t, nil, childSpans[1].Data.Get("meta.event_count"), "child span metadata should NOT be populated with event count")
	trace := coll.getFromCache(traceID)
	assert.Nil(t, trace, "trace should have been sent although the root span hasn't arrived")

	// now we add the root span and verify that both got sent and that the root span had the span count
	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    types.NewPayload(map[string]interface{}{}),
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
	assert.Nil(t, coll.getFromCache(traceID), "after adding a leaf and root span, it should be removed from the cache")
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
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)

	coll.Start()
	defer coll.Stop()

	var traceID = "mytrace"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: types.NewPayload(map[string]interface{}{
				"trace.parent_id": "unused",
			}),
			APIKey: legacyAPIKey,
		},
	}
	span.Data.ExtractMetadata(conf.GetTraceIdFieldNames(), conf.GetParentIdFieldNames())
	decisionSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: types.NewPayload(map[string]interface{}{
				"trace.parent_id":        "unused",
				"meta.refinery.min_span": true,
			}),
			APIKey: legacyAPIKey,
		},
	}
	decisionSpan.Data.ExtractMetadata(conf.GetTraceIdFieldNames(), conf.GetParentIdFieldNames())

	coll.AddSpanFromPeer(span)
	coll.AddSpanFromPeer(decisionSpan)

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Equal(collect, traceID, coll.getFromCache(traceID).TraceID, "after adding the span, we should have a trace in the cache with the right trace ID")
	}, conf.GetTracesConfig().GetSendTickerValue()*6, conf.GetTracesConfig().GetSendTickerValue()*2)
	assert.Equal(t, 0, len(transmission.GetBlock(0)), "adding a non-root span should not yet send the span")

	// ok now let's add the root span and verify that both got sent
	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    types.NewPayload(map[string]interface{}{}),
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	rootSpan.Data.ExtractMetadata(nil, nil)
	coll.AddSpan(rootSpan)

	events := transmission.GetBlock(2)
	assert.Equal(t, 2, len(events), "adding a root span should send all spans in the trace")
	assert.Equal(t, nil, events[0].Data.Get("meta.span_count"), "child span metadata should NOT be populated with span count")
	assert.Equal(t, int64(3), events[1].Data.Get("meta.span_count"), "root span metadata should be populated with span count")
	assert.Nil(t, coll.getFromCache(traceID), "after adding a leaf and root span, it should be removed from the cache")
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
		AddRuleReasonToTrace: true,
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)

	coll.Start()
	defer coll.Stop()

	var traceID = "mytrace"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: types.NewPayload(map[string]interface{}{
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
			Data:    types.NewPayload(map[string]interface{}{}),
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(rootSpan)

	events := transmission.GetBlock(1)
	assert.Equal(t, 1, len(events), "adding a root span should send all spans in the trace")
	assert.Equal(t, int64(2), events[0].Data.Get("meta.span_count"), "root span metadata should be populated with span count")
	assert.Equal(t, "deterministic/always - late arriving span", events[0].Data.Get("meta.refinery.reason"), "late spans should have meta.refinery.reason set to late.")

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
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)

	coll.Start()
	defer coll.Stop()

	var traceID = "traceABC"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: types.NewPayload(map[string]interface{}{
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
			Data:    types.NewPayload(map[string]interface{}{}),
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
		GetSamplerTypeVal: &config.DeterministicSamplerConfig{SampleRate: 1},
		AdditionalAttributes: map[string]string{
			"name":  "foo",
			"other": "bar",
		},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer transmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)

	err := coll.Start()
	assert.NoError(t, err, "collector should start")
	defer coll.Stop()

	var traceID = "trace123"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: types.NewPayload(map[string]interface{}{
				"trace.parent_id": "unused",
			}),
			APIKey: legacyAPIKey,
		},
	}
	coll.AddSpanFromPeer(span)
	time.Sleep(conf.GetTracesConfig().GetSendTickerValue() * 3)

	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    types.NewPayload(map[string]interface{}{}),
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
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)

	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	var traceID = "traceABC"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: types.NewPayload(map[string]interface{}{
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
			Data:       types.NewPayload(map[string]interface{}{}),
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
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		StressRelief: config.StressReliefConfig{
			Mode:              "monitor",
			ActivationLevel:   75,
			DeactivationLevel: 25,
			SamplingRate:      100,
		},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)

	coll.hostname = "host123"

	err := coll.Start()
	require.NoError(t, err, "collector should start")
	defer coll.Stop()

	var traceID = "traceABC"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: types.NewPayload(map[string]interface{}{
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
			Data:    types.NewPayload(map[string]interface{}{}),
			APIKey:  legacyAPIKey,
		},
		IsRoot: true,
	}
	coll.AddSpan(rootSpan)

	events := transmission.GetBlock(2)
	assert.Equal(t, 2, len(events), "adding a root span should send all spans in the trace")
	assert.Equal(t, "host123", events[1].Data.Get("meta.refinery.local_hostname"))

}

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
		ParentIdFieldNames:   []string{"trace.parent_id", "parentId"},
		AddRuleReasonToTrace: true,
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			IncomingQueueSize: 5,
			PeerQueueSize:     5,
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)

	err := coll.Start()
	require.NoError(t, err, "collector should start")
	defer coll.Stop()

	traceIDs := []string{"trace1", "trace2"}

	for i := 0; i < 4; i++ {
		span := &types.Span{
			Event: types.Event{
				Dataset: "aoeu",
				Data: types.NewPayload(map[string]interface{}{
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
		assert.Nil(t, coll.getFromCache(traceID), "trace should have been sent although the root span hasn't arrived")
		rootSpan := &types.Span{
			TraceID: traceID,
			Event: types.Event{
				Dataset: "aoeu",
				Data: types.NewPayload(map[string]interface{}{
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

func TestRedistributeTraces(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(1 * time.Minute),
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
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)

	s := &sharder.MockSharder{
		Self: &sharder.TestShard{Addr: "api1"},
	}

	coll.Sharder = s

	err := coll.Start()
	require.NoError(t, err, "collector should start")

	defer coll.Stop()

	dataset := "aoeu"

	peerEvents := peerTransmission.GetBlock(0)
	assert.Len(t, peerEvents, 0)

	// Traces don't belong to us and its ownership has not changed
	// Redistribution should do nothing
	peerTraceID := "11"
	s.Other = &sharder.TestShard{Addr: "api2", TraceIDs: []string{peerTraceID}}
	acutalSpan := &types.Span{
		TraceID: peerTraceID,
		Event: types.Event{
			Dataset: dataset,
			APIKey:  legacyAPIKey,
			Data:    types.NewPayload(make(map[string]interface{})),
		},
	}

	trace := &types.Trace{
		TraceID:          peerTraceID,
		Dataset:          dataset,
		SendBy:           coll.Clock.Now().Add(5 * time.Second),
		DeciderShardAddr: s.Other.GetAddress(),
	}
	trace.AddSpan(acutalSpan)

	coll.mutex.Lock()
	coll.cache.Set(trace)
	coll.mutex.Unlock()
	coll.redistributeTimer.Reset()

	peerEvents = peerTransmission.GetBlock(0)
	assert.Len(t, peerEvents, 0)

	// if the ownership has changed and the trace doesn't belong to us
	// redistribution should forward a decision span to its new owner
	s.Other = &sharder.TestShard{Addr: "api3", TraceIDs: []string{peerTraceID}}

	coll.redistributeTimer.Reset()

	peerEvents = peerTransmission.GetBlock(1)
	assert.Len(t, peerEvents, 1)
	assert.Equal(t, s.Other.GetAddress(), peerEvents[0].APIHost)

	// Trace belongs to us
	// Redistribution should do nothing
	myTraceID := "1"
	span := &types.Span{
		TraceID: myTraceID,
		Event: types.Event{
			Dataset: dataset,
			APIKey:  legacyAPIKey,
			APIHost: "api1",
			Data:    types.NewPayload(make(map[string]interface{})),
		},
		IsRoot: true,
	}
	span.Data.ExtractMetadata(conf.GetTraceIdFieldNames(), conf.GetParentIdFieldNames())
	decisionSpan := &types.Span{
		TraceID: myTraceID,
		Event: types.Event{
			Dataset: dataset,
			APIKey:  legacyAPIKey,
			Data: types.NewPayload(map[string]interface{}{
				"meta.refinery.min_span": true,
			}),
		},
	}
	decisionSpan.Data.ExtractMetadata(conf.GetTraceIdFieldNames(), conf.GetParentIdFieldNames())

	myTrace := &types.Trace{
		TraceID:          myTraceID,
		Dataset:          dataset,
		SendBy:           coll.Clock.Now().Add(5 * time.Second),
		DeciderShardAddr: s.Other.GetAddress(),
	}

	myTrace.AddSpan(span)
	myTrace.AddSpan(decisionSpan)

	coll.mutex.Lock()
	coll.cache.Set(myTrace)
	coll.mutex.Unlock()
	coll.redistributeTimer.Reset()

	peerEvents = peerTransmission.GetBlock(0)
	assert.Len(t, peerEvents, 0)
	coll.mutex.Lock()
	coll.cache.Get(myTraceID)
	coll.mutex.Unlock()
	assert.Len(t, myTrace.GetSpans(), 2)

	// if the trace previously belongs to us and now belongs to other peers
	// redistribution should forward a decision span to its new owner
	// and remove all decision spans from the cache
	s.Other = &sharder.TestShard{Addr: "api4", TraceIDs: []string{myTraceID}}

	coll.redistributeTimer.Reset()

	peerEvents = peerTransmission.GetBlock(1)
	assert.Len(t, peerEvents, 1)
	assert.Equal(t, s.Other.GetAddress(), peerEvents[0].APIHost)

	coll.mutex.Lock()
	result := coll.cache.Get(myTraceID)
	coll.mutex.Unlock()
	assert.Nil(t, result, "trace should be removed from cache after redistribution")

	conf.Mux.Lock()
	conf.GetCollectionConfigVal.TraceLocalityMode = "distributed"
	conf.Mux.Unlock()
	myTrace2 := &types.Trace{
		TraceID:          myTraceID,
		Dataset:          dataset,
		SendBy:           coll.Clock.Now().Add(5 * time.Second),
		DeciderShardAddr: "api1",
	}

	myTrace2.AddSpan(span)
	myTrace2.AddSpan(decisionSpan)

	coll.mutex.Lock()
	coll.cache.Set(myTrace2)
	coll.mutex.Unlock()
	coll.redistributeTimer.Reset()

	peerEvents = peerTransmission.GetBlock(1)
	assert.Len(t, peerEvents, 1)
	assert.Equal(t, s.Other.GetAddress(), peerEvents[0].APIHost)

	result = coll.cache.Get(myTraceID)
	assert.NotNil(t, result, "only decision spans should be removed from cache after redistribution")
	spans := result.GetSpans()
	assert.Len(t, spans, 1, "trace should still be in cache after redistribution")
	assert.False(t, spans[0].IsDecisionSpan())

	// create a trace that only contains decision spans
	//	if the trace previously belongs to us and now belongs to other peers
	// redistribution should delete the trace from cache
	s.Other = &sharder.TestShard{Addr: "api4", TraceIDs: []string{peerTraceID}}
	emptyTrace := &types.Trace{
		TraceID:          peerTraceID,
		Dataset:          dataset,
		SendBy:           coll.Clock.Now().Add(5 * time.Second),
		DeciderShardAddr: "api1",
	}

	emptyTrace.AddSpan(decisionSpan)
	coll.mutex.Lock()
	coll.cache.Set(emptyTrace)
	coll.mutex.Unlock()
	coll.redistributeTimer.Reset()

	peerEvents = peerTransmission.GetBlock(0)
	assert.Len(t, peerEvents, 0)

	coll.mutex.Lock()
	assert.Nil(t, coll.cache.Get(emptyTrace.TraceID), "empty trace should be removed from cache after redistribution")
	coll.mutex.Unlock()
}

func TestDrainTracesOnShutdown(t *testing.T) {
	// Set up common configuration and test collector
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
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)

	coll.hostname = "host123"
	coll.Sharder = &sharder.MockSharder{
		Self:  &sharder.TestShard{Addr: "api1"},
		Other: &sharder.TestShard{Addr: "api2", TraceIDs: []string{"traceID1", "traceID2"}},
	}

	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)

	coll.outgoingTraces = make(chan sendableTrace, 5)
	coll.keptDecisionBuffer = make(chan TraceDecision, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)

	sentTraceChan := make(chan sentRecord, 1)
	forwardTraceChan := make(chan *types.Span, 1)
	now := time.Now()

	// Define test cases
	tests := []struct {
		name                 string
		traceID              string
		span                 *types.Span
		preRecordTrace       bool
		expectedSent         int
		expectedForwarded    int
		expectedTransmission *transmit.MockTransmission
		expectedAPIHost      string
		expectedTraceSendBy  int64
	}{
		{
			name:                 "Trace already has decision, should be sent",
			traceID:              "traceID1",
			span:                 &types.Span{TraceID: "traceID1", Event: types.Event{Dataset: "aoeu", Data: types.NewPayload(make(map[string]interface{}))}},
			preRecordTrace:       true,
			expectedSent:         1,
			expectedForwarded:    0,
			expectedTransmission: transmission,
		},
		{
			name:                 "Trace cannot be decided yet, should be forwarded",
			traceID:              "traceID2",
			span:                 &types.Span{TraceID: "traceID2", Event: types.Event{Dataset: "test2", Data: types.NewPayload(make(map[string]interface{}))}},
			preRecordTrace:       false,
			expectedSent:         0,
			expectedForwarded:    1,
			expectedTransmission: peerTransmission,
			expectedAPIHost:      "api2",
			expectedTraceSendBy:  now.Unix(),
		},
		{
			name:    "decision spans that already has decision should be ignored and discarded",
			traceID: "traceID3",
			span: &types.Span{TraceID: "traceID3", Event: types.Event{Dataset: "test3", Data: types.NewPayload(map[string]interface{}{
				"meta.refinery.min_span": true,
			})}},
			preRecordTrace:       true,
			expectedSent:         0,
			expectedForwarded:    0,
			expectedTransmission: transmission,
		},
		{
			name:    "decision spans that belongs to other peers should be ignored and discarded",
			traceID: "traceID2",
			span: &types.Span{TraceID: "traceID2", Event: types.Event{Dataset: "test4", Data: types.NewPayload(map[string]interface{}{
				"meta.refinery.min_span": true,
			})}},
			preRecordTrace:       false,
			expectedSent:         0,
			expectedForwarded:    0,
			expectedTransmission: peerTransmission,
		},
	}

	// Run test cases
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.span.Data.ExtractMetadata(conf.GetTraceIdFieldNames(), conf.GetParentIdFieldNames())

			// Optionally record the trace decision beforehand
			if tt.preRecordTrace {
				trace := &types.Trace{TraceID: tt.traceID}
				stc.Record(trace, true, "test")
			}

			// Call distributeSpansOnShutdown

			coll.distributeSpansOnShutdown(sentTraceChan, forwardTraceChan, &now, tt.span)
			require.Len(t, sentTraceChan, tt.expectedSent)
			require.Len(t, forwardTraceChan, tt.expectedForwarded)

			// if the span should be ignored, we don't need to send it
			if tt.expectedForwarded == 0 && tt.expectedSent == 0 {
				return
			}

			// Send the spans on shutdown
			ctx, cancel := context.WithCancel(context.Background())
			go coll.sendSpansOnShutdown(ctx, sentTraceChan, forwardTraceChan)

			// Check transmission or forwarding
			events := tt.expectedTransmission.GetBlock(1)
			require.Len(t, events, 1)
			require.Equal(t, tt.span.Dataset, events[0].Dataset)

			sendBy := events[0].Data.MetaRefinerySendBy
			if tt.expectedTraceSendBy != 0 {
				require.NotEqual(t, int64(0), sendBy)
				require.Equal(t, tt.expectedTraceSendBy, sendBy)
			} else {
				require.Equal(t, int64(0), sendBy)
			}

			if tt.expectedAPIHost != "" {
				require.Equal(t, tt.expectedAPIHost, events[0].APIHost)
			}

			// Cancel and flush transmissions
			cancel()
		})
	}
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
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
		GetSamplerTypeVal:    &config.DeterministicSamplerConfig{SampleRate: 2},
		AddSpanCountToRoot:   true,
		AddCountsToRoot:      true,
		ParentIdFieldNames:   []string{"trace.parent_id", "parentId"},
		AddRuleReasonToTrace: true,
		GetCollectionConfigVal: config.CollectionConfig{
			IncomingQueueSize: 500,
			PeerQueueSize:     500,
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)

	err := coll.Start()
	require.NoError(t, err, "collector should start")
	defer coll.Stop()

	// this name was chosen to be Kept with the deterministic/2 sampler
	var traceID = "myTrace"

	for i := 0; i < spanlimit; i++ {
		span := &types.Span{
			TraceID: traceID,
			Event: types.Event{
				Dataset: "aoeu",
				Data: types.NewPayload(map[string]interface{}{
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
			Data:    types.NewPayload(map[string]interface{}{}),
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
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
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
			Data: types.NewPayload(map[string]interface{}{
				"trace.parent_id":        "unused",
				"http.status_code":       200,
				"test":                   1,
				"should-not-be-included": 123,
			}),
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
		Data: types.NewPayload(map[string]interface{}{
			"http.status_code": 200,
			"test":             1,
		}),
	}
	// Set metadata fields directly
	expected.Data.MetaAnnotationType = strconv.Itoa(int(types.SpanAnnotationTypeUnknown))
	expected.Data.MetaRefineryMinSpan.Set(true)
	expected.Data.MetaRefineryRoot.Set(false)
	expected.Data.MetaRefinerySpanDataSize = 87
	expected.Data.MetaTraceID = traceID1

	assert.Equal(t, expected.APIHost, ds.APIHost)
	assert.Equal(t, expected.APIKey, ds.APIKey)
	assert.Equal(t, expected.Dataset, ds.Dataset)
	assert.Equal(t, expected.Environment, ds.Environment)
	assert.Equal(t, expected.SampleRate, ds.SampleRate)
	assert.Equal(t, expected.Timestamp, ds.Timestamp)
	assert.Equal(t, expected.Data, ds.Data)

	rootSpan := nonrootSpan
	rootSpan.IsRoot = true

	ds = coll.createDecisionSpan(rootSpan, trace, peerShard)
	expected.Data.MetaRefineryRoot.Set(true)

	assert.Equal(t, expected.APIHost, ds.APIHost)
	assert.Equal(t, expected.APIKey, ds.APIKey)
	assert.Equal(t, expected.Dataset, ds.Dataset)
	assert.Equal(t, expected.Environment, ds.Environment)
	assert.Equal(t, expected.SampleRate, ds.SampleRate)
	assert.Equal(t, expected.Timestamp, ds.Timestamp)
	assert.Equal(t, expected.Data, ds.Data)

}

func TestSendDropDecisions(t *testing.T) {
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
			ShutdownDelay:     config.Duration(1 * time.Millisecond),
			TraceLocalityMode: "distributed",
		},
	}
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)
	coll.dropDecisionBuffer = make(chan TraceDecision, 5)

	messages := make(chan string, 5)
	coll.PubSub.Subscribe(context.Background(), dropTraceDecisionTopic, func(ctx context.Context, msg string) {
		messages <- msg
	})

	// drop decisions should be sent once the timer expires
	collectionCfg := conf.GetCollectionConfig()
	collectionCfg.DropDecisionSendInterval = config.Duration(2 * time.Millisecond)
	conf.GetCollectionConfigVal = collectionCfg

	closed := make(chan struct{})
	coll.shutdownWG.Add(1)
	go func() {
		coll.sendDropDecisions()
		close(closed)
	}()

	coll.dropDecisionBuffer <- TraceDecision{TraceID: "trace1"}
	close(coll.dropDecisionBuffer)
	droppedMessage := <-messages

	// pretend we are a peer that received the message
	decompressedData, err := newDroppedTraceDecision(droppedMessage, "another-peer")
	assert.NoError(t, err)
	droppedTraceID := make([]string, 0)
	for _, td := range decompressedData {
		droppedTraceID = append(droppedTraceID, td.TraceID)
	}
	assert.Equal(t, []string{"trace1"}, droppedTraceID)

	<-closed

	// drop decision should be sent once it reaches the batch size
	collectionCfg = conf.GetCollectionConfig()
	collectionCfg.DropDecisionSendInterval = config.Duration(60 * time.Second)
	collectionCfg.MaxDropDecisionBatchSize = 5
	conf.GetCollectionConfigVal = collectionCfg
	coll.dropDecisionBuffer = make(chan TraceDecision, 5)

	closed = make(chan struct{})
	coll.shutdownWG.Add(1)
	go func() {
		coll.sendDropDecisions()
		close(closed)
	}()

	for i := 0; i < 5; i++ {
		coll.dropDecisionBuffer <- TraceDecision{
			TraceID: fmt.Sprintf("trace%d", i),
		}
	}
	close(coll.dropDecisionBuffer)
	droppedMessage = <-messages

	decompressedData, err = newDroppedTraceDecision(droppedMessage, "another-peer")
	assert.NoError(t, err)
	droppedTraceID = make([]string, 0)
	for _, td := range decompressedData {
		droppedTraceID = append(droppedTraceID, td.TraceID)
	}
	assert.Equal(t, "trace0,trace1,trace2,trace3,trace4", strings.Join(droppedTraceID, ","))

	<-closed
}

func TestExpiredTracesCleanup(t *testing.T) {
	conf := &config.MockConfig{
		GetTracesConfigVal: config.TracesConfig{
			SendTicker:   config.Duration(2 * time.Millisecond),
			SendDelay:    config.Duration(1 * time.Millisecond),
			TraceTimeout: config.Duration(500 * time.Millisecond),
			MaxBatchSize: 1500,
		},
		GetCollectionConfigVal: config.CollectionConfig{
			TraceLocalityMode: "distributed",
		},
		GetSamplerTypeVal:    &config.DeterministicSamplerConfig{SampleRate: 1},
		AddSpanCountToRoot:   true,
		AddCountsToRoot:      true,
		ParentIdFieldNames:   []string{"trace.parent_id", "parentId"},
		AddRuleReasonToTrace: true,
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)

	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 5)
	coll.keptDecisionBuffer = make(chan TraceDecision, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)

	for _, traceID := range peerTraceIDs {
		trace := &types.Trace{
			TraceID: traceID,
			SendBy:  coll.Clock.Now(),
		}
		trace.AddSpan(&types.Span{
			TraceID: trace.ID(),
			Event: types.Event{
				Context: context.Background(),
			},
		})
		coll.cache.Set(trace)
	}

	assert.Eventually(t, func() bool {
		return coll.cache.GetCacheEntryCount() == len(peerTraceIDs)
	}, 200*time.Millisecond, 10*time.Millisecond)

	traceTimeout := time.Duration(conf.GetTracesConfig().TraceTimeout)
	coll.sendExpiredTracesInCache(context.Background(), coll.Clock.Now().Add(3*traceTimeout))

	events := peerTransmission.GetBlock(3)
	assert.Len(t, events, 3)
	assert.True(t, events[0].Data.MetaRefineryExpiredTrace.HasValue)
	assert.True(t, events[0].Data.MetaRefineryExpiredTrace.Value)

	coll.sendExpiredTracesInCache(context.Background(), coll.Clock.Now().Add(5*traceTimeout))

	assert.Eventually(t, func() bool {
		return len(coll.outgoingTraces) == 3
	}, 100*time.Millisecond, 10*time.Millisecond)

	// at this point, the expired traces should have been removed from the trace cache
	assert.Zero(t, coll.cache.GetCacheEntryCount())

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
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay: config.Duration(1 * time.Millisecond),
		},
	}

	transmission := &transmit.MockTransmission{}
	transmission.Start()
	defer transmission.Stop()
	peerTransmission := &transmit.MockTransmission{}
	peerTransmission.Start()
	defer peerTransmission.Stop()
	coll := newTestCollector(conf, transmission, peerTransmission)

	c := cache.NewInMemCache(10, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	sampleTraceCache, err := newCache()
	require.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = sampleTraceCache
	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.outgoingTraces = make(chan sendableTrace, 5)

	defer coll.Stop()

	clock := clockwork.NewFakeClock()
	coll.Clock = clock

	traceID := "span-limit-trace"

	// Pre-create the trace with a known SendBy time
	now := coll.Clock.Now()
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
				Data: types.NewPayload(map[string]interface{}{
					"trace.parent_id": "unused",
				}),
				APIKey: legacyAPIKey,
			},
		})
	}
	coll.cache.Set(trace)

	clock.Advance(5 * time.Second)
	// process another span for the same trace that exceeds the span limit should not change the SendBy time
	lateSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "test-dataset",
			APIKey:  legacyAPIKey,
		},
	}
	coll.processSpan(context.Background(), lateSpan, "incoming")

	updatedTrace := coll.cache.Get(traceID)
	require.Equal(t, trace.SendBy.Unix(), updatedTrace.SendBy.Unix())

}

// BenchmarkCollectorWithSamplers runs benchmarks for different sampler configurations
func BenchmarkCollectorWithSamplers(b *testing.B) {
	// Common test scenarios to run for each sampler
	scenarios := []struct {
		name          string
		spansPerTrace int
	}{
		{"small_traces", 100},
		{"medium_traces", 10000},
		{"large_traces", 1_000_000},
	}

	// Different sampler configurations to test
	samplerConfigs := []struct {
		name   string
		config interface{}
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
			"emadynamic",
			&config.EMADynamicSamplerConfig{
				GoalSampleRate: 1,
				FieldList:      []string{"sampler-field-1", "sampler-field-2"},
				// TODO: using 1 second interval would lock up the benchmark after a few iterations. Why is that?
				AdjustmentInterval: config.Duration(5 * time.Second),
			},
		},
		{
			"rulesbased",
			&config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Name:       "greater than 10",
						Scope:      "trace",
						SampleRate: 1,
						Conditions: []*config.RulesBasedSamplerCondition{
							{
								Field:    "sampler-field-1",
								Operator: config.GT,
								Value:    10,
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

				sender := &mockSender{
					eventQueue: make(chan *types.Event, 10000),
				}
				collector := setupBenchmarkCollector(b, sampler.config, sender)
				collector.Start()

				defer collector.Stop()

				// use b.N as the number of traces to create
				totalSpans := b.N * scenario.spansPerTrace

				// Setup done channel that waits for all spans to be processed
				wg := &sync.WaitGroup{}
				wg.Add(1)
				go func() {
					sender.waitForCount(b, totalSpans)
					wg.Done()
				}()

				spans := make([]*types.Span, totalSpans)
				spanIdx := 0

				// Create spans for each trace
				for t := 0; t < b.N; t++ {
					traceID := fmt.Sprintf("trace-%d", t)

					// Create spans for this trace
					for s := 0; s < scenario.spansPerTrace; s++ {
						isRoot := (s == scenario.spansPerTrace-1) // Last span is root

						spans[spanIdx] = &types.Span{
							TraceID: traceID,
							IsRoot:  isRoot,
							Event: types.Event{
								Dataset: "benchmark-dataset",
								APIKey:  "test-api-key",
								Data: types.NewPayload(map[string]interface{}{
									"sampler-field-1": rand.Intn(20),
									"sampler-field-2": "static-value",
									"index":           spanIdx,
								}),
							},
						}

						// Add parent ID to non-root spans
						if !isRoot {
							spans[spanIdx].Data.Set("trace.parent_id", fmt.Sprintf("parent-%s", traceID))
						}

						spanIdx++
					}
				}

				b.StartTimer()
				for i := 0; i < totalSpans; i++ {
					collector.AddSpan(spans[i])
				}

				// Wait for all spans to be processed
				wg.Wait()
				b.StopTimer()
			})
		}
	}
}

func setupBenchmarkCollector(b *testing.B, samplerConfig interface{}, sender *mockSender) *InMemCollector {
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
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			ShutdownDelay:      config.Duration(1 * time.Millisecond),
			HealthCheckTimeout: config.Duration(100 * time.Millisecond),
			IncomingQueueSize:  100000,
			PeerQueueSize:      100000,
		},
	}

	coll := newTestCollector(conf, sender, sender)

	coll.BlockOnAddSpan = true

	return coll
}

// mockSender is a mock implementation of the Transmission
type mockSender struct {
	eventQueue chan *types.Event
}

func (c *mockSender) EnqueueEvent(event *types.Event) {
	select {
	case c.eventQueue <- event:
	}
}

func (c *mockSender) EnqueueSpan(span *types.Span) {
	select {
	case c.eventQueue <- &span.Event:
	}
}

func (c *mockSender) Flush() {
	// No-op for mock sender
}

func (c *mockSender) RegisterMetrics() {
	// No-op for mock sender
}

func (c *mockSender) waitForCount(b *testing.B, target int) {
	var count int
	for {
		select {
		case <-c.eventQueue:
			count += 1
			if count >= target {
				return
			}
		}
	}
}

package collect

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/centralstore"
	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/internal/redis"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestCentralCollector_AddSpan(t *testing.T) {
	conf := &config.MockConfig{
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		SendTickerVal:      2 * time.Millisecond,
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			CacheCapacity: 3,
		},
	}
	coll := &CentralCollector{BlockOnDecider: true}
	stop := startCollector(t, conf, coll, nil, clockwork.NewFakeClock())
	defer stop()

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

	// adding one span with parent ID should:
	// * create the trace in the cache
	// * send the trace to the central store
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.NotNil(collect, coll.SpanCache.Get(traceID1))
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
		Event: types.Event{
			Dataset: "aoeu",
			APIKey:  legacyAPIKey,
		},
	}
	require.NoError(t, coll.AddSpan(root))

	// adding one span with parent ID should:
	// * create the trace in the cache
	// * send the trace to the central store
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.NotNil(collect, coll.SpanCache.Get(traceID1))
	}, 5*time.Second, 500*time.Millisecond)
	trace, err = coll.Store.GetTrace(ctx, traceID1)
	require.NoError(t, err)
	assert.Equal(t, traceID1, trace.TraceID)
	assert.Len(t, trace.Spans, 2)
	assert.NotNil(t, trace.Root)
}

func TestCentralCollector_ProcessTraces(t *testing.T) {
	conf := &config.MockConfig{
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		SendTickerVal:      2 * time.Millisecond,
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			CacheCapacity: 100,
		},
	}
	transmission := &transmit.MockTransmission{}

	collector := &CentralCollector{}
	clock := clockwork.NewRealClock()
	stop := startCollector(t, conf, collector, transmission, clock)
	defer stop()

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
		}
		err := collector.AddSpan(span)
		require.NoError(t, err)
	}

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		count, ok := collector.Metrics.Get("trace_send_kept")
		require.True(collect, ok)
		assert.Equal(collect, float64(numberOfTraces), count)
	}, 2*time.Second, 500*time.Millisecond)
}

func TestCentralCollector_Decider(t *testing.T) {
	conf := &config.MockConfig{
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 2},
		SendTickerVal:      2 * time.Millisecond,
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{
			CacheCapacity: 100,
		},
	}
	transmission := &transmit.MockTransmission{}

	collector := &CentralCollector{}
	clock := clockwork.NewRealClock()
	stop := startCollector(t, conf, collector, transmission, clock)
	defer stop()

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
		}
		err := collector.AddSpan(span)
		require.NoError(t, err)
	}

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		count, ok := collector.Metrics.Get("trace_send_kept")
		require.True(collect, ok)
		require.Equal(collect, float64(6), count)
	}, 2*time.Second, 500*time.Millisecond)
}

func startCollector(t *testing.T, cfg *config.MockConfig, collector *CentralCollector, transmission transmit.Transmission, clock clockwork.Clock) func() {
	if cfg == nil {
		cfg = &config.MockConfig{}
	}

	if transmission == nil {
		transmission = &transmit.MockTransmission{}
	}

	cfg.StoreOptions = config.SmartWrapperOptions{
		SpanChannelSize: 200,
		StateTicker:     duration("50ms"),
		SendDelay:       duration("200ms"),
		TraceTimeout:    duration("500ms"),
		DecisionTimeout: duration("500ms"),
	}
	cfg.SampleCache = config.SampleCacheConfig{
		KeptSize:          1000,
		DroppedSize:       1000,
		SizeCheckInterval: duration("1s"),
	}

	cfg.GetTraceTimeoutVal = time.Duration(500 * time.Microsecond)

	basicStore := &centralstore.RedisBasicStore{}
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
		{Value: transmission, Name: "upstreamTransmission"},
		{Value: &peer.MockPeers{Peers: []string{"foo", "bar"}}},
		{Value: samplerFactory},
		{Value: redis, Name: "redis"},
		{Value: clock},
		{Value: basicStore},
		{Value: sw},
		{Value: collector},
	}
	g := inject.Graph{}
	require.NoError(t, g.Provide(objects...))
	require.NoError(t, g.Populate())

	require.NoError(t, startstop.Start(g.Objects(), nil))

	stopper := func() {
		conn := redis.Get()
		conn.Do("FLUSHDB")
		conn.Close()
		require.NoError(t, startstop.Stop(g.Objects(), nil))
	}

	return stopper

}

func duration(s string) config.Duration {
	d, _ := time.ParseDuration(s)
	return config.Duration(d)
}

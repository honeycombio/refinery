package collect

import (
	"context"
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
	coll, stop := newTestCentralCollector(t, conf)
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
	// * trace is removed from cache due to sendTickerInterval
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Nil(collect, coll.getFromCache(traceID1), "after sending the span, it should be removed from the cache")
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
	// * trace is removed from cache due to sendTickerInterval
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Nil(collect, coll.getFromCache(traceID1), "after sending the span, it should be removed from the cache")
	}, 5*time.Second, 500*time.Millisecond)
	trace, err = coll.Store.GetTrace(ctx, traceID1)
	require.NoError(t, err)
	assert.Equal(t, traceID1, trace.TraceID)
	assert.Len(t, trace.Spans, 2)
	assert.NotNil(t, trace.Root)
}

func newTestCentralCollector(t *testing.T, cfg *config.MockConfig) (*CentralCollector, func()) {
	if cfg == nil {
		cfg = &config.MockConfig{}
	}

	cfg.StoreOptions = config.SmartWrapperOptions{
		SpanChannelSize: 100,
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

	basicStore := &centralstore.RedisBasicStore{}
	decisionCache := &cache.CuckooSentCache{}
	sw := &centralstore.SmartWrapper{}
	redis := &redis.DefaultClient{}
	clock := clockwork.NewFakeClock()
	samplerFactory := &sample.SamplerFactory{
		Config: cfg,
		Logger: &logger.NullLogger{},
	}

	collector := &CentralCollector{}
	objects := []*inject.Object{
		{Value: "version", Name: "version"},
		{Value: cfg},
		{Value: &logger.NullLogger{}},
		{Value: &metrics.NullMetrics{}, Name: "genericMetrics"},
		{Value: trace.Tracer(noop.Tracer{}), Name: "tracer"},
		{Value: decisionCache},
		{Value: &transmit.MockTransmission{}, Name: "upstreamTransmission"},
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
		conn.Do("FLUSHALL")
		conn.Close()
		require.NoError(t, startstop.Stop(g.Objects(), nil))
	}

	return collector, stopper

}

func duration(s string) config.Duration {
	d, _ := time.ParseDuration(s)
	return config.Duration(d)
}

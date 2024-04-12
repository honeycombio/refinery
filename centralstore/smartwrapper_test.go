package centralstore

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/redis"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func duration(s string) config.Duration {
	d, _ := time.ParseDuration(s)
	return config.Duration(d)
}

var storeType = "redis"

type dummyLogger struct{}

func (d dummyLogger) Debugf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
	fmt.Println()
}

func (d dummyLogger) Errorf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
	fmt.Println()
}

func getAndStartSmartWrapper(storetype string, redisClient redis.Client) (*SmartWrapper, func(), error) {

	cfg := config.MockConfig{
		StoreOptions: config.SmartWrapperOptions{
			SpanChannelSize: 200,
			StateTicker:     duration("50ms"),
			SendDelay:       duration("200ms"),
			TraceTimeout:    duration("500ms"),
			DecisionTimeout: duration("500ms"),
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          1000,
			DroppedSize:       1000,
			SizeCheckInterval: duration("1s"),
		},
	}

	decisionCache := &cache.CuckooSentCache{}
	sw := &SmartWrapper{}
	clock := clockwork.NewRealClock()
	objects := []*inject.Object{
		{Value: "version", Name: "version"},
		{Value: &cfg},
		{Value: &logger.NullLogger{}},
		{Value: &metrics.MockMetrics{}, Name: "genericMetrics"},
		{Value: trace.Tracer(noop.Tracer{}), Name: "tracer"},
		{Value: decisionCache},
		{Value: clock},
		{Value: sw},
	}
	g := inject.Graph{Logger: dummyLogger{}}

	var store BasicStorer
	switch storetype {
	case "local":
		store = &LocalRemoteStore{}
	case "redis":
		if redisClient == nil {
			redisClient = &redis.TestService{}
		}
		objects = append(objects, &inject.Object{Value: redisClient, Name: "redis"})
		store = &RedisBasicStore{}
	default:
		return nil, nil, fmt.Errorf("unknown store type %s", storetype)
	}
	objects = append(objects, &inject.Object{Value: store})
	err := g.Provide(objects...)
	if err != nil {
		return nil, nil, err
	}

	if err := g.Populate(); err != nil {
		fmt.Printf("failed to populate injection graph. error: %+v\n", err)
		return nil, nil, err
	}

	ststLogger := dummyLogger{}

	if err := startstop.Start(g.Objects(), ststLogger); err != nil {
		fmt.Printf("failed to start injected dependencies. error: %+v\n", err)
		os.Exit(1)
	}

	stopper := func() {
		startstop.Stop(g.Objects(), ststLogger)
	}

	return sw, stopper, err
}

func TestSingleSpanGetsCollected(t *testing.T) {
	store, stopper, err := getAndStartSmartWrapper(storeType, nil)
	require.NoError(t, err)
	defer stopper()

	randomNum := rand.Intn(500)
	span := &CentralSpan{
		TraceID: fmt.Sprintf("trace%d", randomNum),
		SpanID:  fmt.Sprintf("span%d", randomNum),
		IsRoot:  false,
	}
	ctx := context.Background()
	err = store.WriteSpan(ctx, span)
	require.NoError(t, err)

	// make sure that it arrived in the collecting state
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		states, err := store.GetStatusForTraces(ctx, []string{span.TraceID})
		assert.NoError(collect, err)
		assert.Equal(collect, 1, len(states))
		if len(states) > 0 {
			assert.Equal(collect, span.TraceID, states[0].TraceID)
			assert.Equal(collect, Collecting, states[0].State)
		}
	}, 100*time.Second, 100*time.Millisecond)
}

func TestSingleTraceOperation(t *testing.T) {
	store, stopper, err := getAndStartSmartWrapper(storeType, nil)
	require.NoError(t, err)
	defer stopper()

	span := &CentralSpan{
		TraceID: "trace1",
		SpanID:  "span1",
		IsRoot:  false,
	}
	ctx := context.Background()
	err = store.WriteSpan(ctx, span)
	require.NoError(t, err)

	// make sure that it arrived in the collecting state
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		states, err := store.GetStatusForTraces(ctx, []string{span.TraceID})
		assert.NoError(collect, err)
		assert.Equal(collect, 1, len(states))
		if len(states) > 0 {
			assert.Equal(collect, span.TraceID, states[0].TraceID)
			assert.Equal(collect, Collecting, states[0].State)
		}
	}, 1*time.Second, 10*time.Millisecond)

	// it should automatically time out to the Waiting state
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		states, err := store.GetStatusForTraces(ctx, []string{span.TraceID})
		assert.NoError(collect, err)
		assert.Equal(collect, 1, len(states))
		if len(states) > 0 {
			assert.Equal(collect, span.TraceID, states[0].TraceID)
			assert.Equal(collect, DecisionDelay, states[0].State, fmt.Sprintf("unexpected state for trace %s", states[0].TraceID))
		}
	}, 1*time.Second, 10*time.Millisecond)

	// and then to the Ready state
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		states, err := store.GetStatusForTraces(ctx, []string{span.TraceID})
		assert.NoError(collect, err)
		assert.Equal(collect, 1, len(states))
		if len(states) > 0 {
			assert.Equal(collect, span.TraceID, states[0].TraceID)
			assert.Equal(collect, ReadyToDecide, states[0].State, fmt.Sprintf("unexpected state for trace %s", states[0].TraceID))
		}
	}, 3*time.Second, 100*time.Millisecond)
}

func TestBasicStoreOperation(t *testing.T) {
	store, stopper, err := getAndStartSmartWrapper(storeType, nil)
	require.NoError(t, err)
	defer stopper()

	ctx := context.Background()
	traceids := make([]string, 0)

	for tr := 0; tr < 10; tr++ {
		tid := fmt.Sprintf("trace%d", tr)
		traceids = append(traceids, tid)
		// write 9 child spans to the store
		for s := 1; s < 10; s++ {
			span := &CentralSpan{
				TraceID: tid,
				SpanID:  fmt.Sprintf("span%d", s),
				IsRoot:  false,
			}
			err = store.WriteSpan(ctx, span)
			require.NoError(t, err)
		}
		// now write the root span
		span := &CentralSpan{
			TraceID: tid,
			SpanID:  "span0",
			IsRoot:  true,
		}
		err = store.WriteSpan(ctx, span)
		require.NoError(t, err)
	}

	assert.Equal(t, 10, len(traceids))
	assert.Eventually(t, func() bool {
		states, err := store.GetStatusForTraces(ctx, traceids)
		return err == nil && len(states) == 10
	}, 1*time.Second, 100*time.Millisecond)

	// wait for it to reach the Ready state
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		states, err := store.GetStatusForTraces(ctx, traceids)
		assert.NoError(collect, err)
		assert.Equal(collect, 10, len(states))
		for _, state := range states {
			assert.Equal(collect, ReadyToDecide, state.State, fmt.Sprintf("unexpected state for trace %s", state.TraceID))
		}
	}, 3*time.Second, 100*time.Millisecond)

	// check that the spans are in the store
	for _, tid := range traceids {
		trace, err := store.GetTrace(ctx, tid)
		assert.NoError(t, err)
		if err == nil {
			assert.Equal(t, 10, len(trace.Spans))
			assert.NotNil(t, trace.Root)
		}
	}
}

func TestReadyForDecisionLoop(t *testing.T) {
	store, stopper, err := getAndStartSmartWrapper(storeType, nil)
	require.NoError(t, err)
	defer stopper()

	ctx := context.Background()
	numberOfTraces := 11
	traceids := make([]string, 0)

	for tr := 0; tr < numberOfTraces; tr++ {
		tid := fmt.Sprintf("trace%02d", tr)
		traceids = append(traceids, tid)
		// write 9 child spans to the store
		for s := 1; s < 10; s++ {
			span := &CentralSpan{
				TraceID: tid,
				SpanID:  fmt.Sprintf("span%d", s),
				IsRoot:  false,
			}
			err := store.WriteSpan(ctx, span)
			require.NoError(t, err)
		}
		// now write the root span
		span := &CentralSpan{
			TraceID: tid,
			SpanID:  "span0",
		}
		err = store.WriteSpan(ctx, span)
		require.NoError(t, err)
	}

	assert.Equal(t, numberOfTraces, len(traceids))
	assert.Eventually(t, func() bool {
		states, err := store.GetStatusForTraces(ctx, traceids)
		return err == nil && len(states) == numberOfTraces
	}, 1*time.Second, 100*time.Millisecond)

	// wait for it to reach the Ready state
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		states, err := store.GetStatusForTraces(ctx, traceids)
		assert.NoError(collect, err)
		assert.Equal(collect, numberOfTraces, len(states))
		for _, state := range states {
			assert.Equal(collect, ReadyToDecide, state.State, fmt.Sprintf("unexpected state for trace %s", state.TraceID))
		}
	}, 3*time.Second, 100*time.Millisecond)

	// get the traces in the Ready state
	toDecide, err := store.GetTracesNeedingDecision(ctx, numberOfTraces)
	require.NoError(t, err)
	sort.Strings(toDecide)
	assert.Equal(t, numberOfTraces, len(toDecide))
	assert.EqualValues(t, traceids[:numberOfTraces], toDecide)
}

func TestSetTraceStatuses(t *testing.T) {
	store, stopper, err := getAndStartSmartWrapper(storeType, &redis.DefaultClient{})
	require.NoError(t, err)
	defer stopper()

	ctx := context.Background()
	numberOfTraces := 5
	traceids := make([]string, 0)

	err = store.RecordMetrics(ctx)
	require.NoError(t, err)
	_, ok := store.Metrics.Get("redisstore_count_traces")
	require.True(t, ok)

	for tr := 0; tr < numberOfTraces; tr++ {
		tid := fmt.Sprintf("trace%02d", rand.Intn(1000))
		traceids = append(traceids, tid)
		// write 9 child spans to the store
		for s := 1; s < 10; s++ {
			span := &CentralSpan{
				TraceID: tid,
				SpanID:  fmt.Sprintf("span%d", s),
				IsRoot:  false,
			}
			err = store.WriteSpan(ctx, span)
			require.NoError(t, err)
		}
		// now write the root span
		span := &CentralSpan{
			TraceID: tid,
			SpanID:  "span0",
		}
		err = store.WriteSpan(ctx, span)
		require.NoError(t, err)
	}

	assert.Equal(t, numberOfTraces, len(traceids))
	assert.Eventually(t, func() bool {
		states, err := store.GetStatusForTraces(ctx, traceids)
		return err == nil && len(states) == numberOfTraces
	}, 1*time.Second, 100*time.Millisecond)

	// wait for it to reach the Ready state
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		states, err := store.GetStatusForTraces(ctx, traceids)
		assert.NoError(collect, err)
		assert.Equal(collect, numberOfTraces, len(states))
		for _, state := range states {
			assert.Equal(collect, ReadyToDecide, state.State)
		}

	}, 3*time.Second, 100*time.Millisecond)

	// get the traces in the Ready state
	toDecide, err := store.GetTracesNeedingDecision(ctx, numberOfTraces)
	assert.NoError(t, err)
	assert.Equal(t, numberOfTraces, len(toDecide))
	sort.Strings(toDecide)
	expected := traceids[:numberOfTraces]
	sort.Strings(expected)
	assert.EqualValues(t, expected, toDecide)

	statuses := make([]*CentralTraceStatus, 0)
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		statuses, err = store.GetStatusForTraces(ctx, toDecide)
		assert.NoError(collect, err)
		assert.Equal(collect, numberOfTraces, len(statuses))
		for _, state := range statuses {
			assert.Equal(collect, AwaitingDecision, state.State)
		}
		err = store.RecordMetrics(ctx)
		require.NoError(t, err)
		value, ok := store.Metrics.Get("redisstore_count_awaiting_decision")
		require.True(t, ok)
		assert.EqualValues(t, numberOfTraces, value)
	}, 3*time.Second, 100*time.Millisecond)

	for _, status := range statuses {
		if status.TraceID == traceids[0] {
			status.State = DecisionKeep
			status.KeepReason = "because"
		} else {
			status.State = DecisionDrop
		}
	}
	require.NotEmpty(t, statuses)
	err = store.SetTraceStatuses(ctx, statuses)
	assert.NoError(t, err)

	// we need to give the dropped traces cache a chance to run or it might not process everything
	time.Sleep(50 * time.Millisecond)
	statuses, err = store.GetStatusForTraces(ctx, traceids)
	assert.NoError(t, err)
	assert.Equal(t, numberOfTraces, len(statuses))
	for _, status := range statuses {
		if status.TraceID == traceids[0] {
			assert.Equal(t, DecisionKeep, status.State)
			assert.Equal(t, "because", status.KeepReason)
		} else {
			assert.Equal(t, DecisionDrop, status.State)
		}
	}

	err = store.RecordMetrics(ctx)
	require.NoError(t, err)
	count, ok := store.Metrics.Get("redisstore_count_awaiting_decision")
	require.True(t, ok)
	assert.Equal(t, float64(0), count)
	count, ok = store.Metrics.Get("redisstore_count_traces")
	require.True(t, ok)
	assert.GreaterOrEqual(t, count, float64(numberOfTraces))
	count, ok = store.Metrics.Get("redisstore_memory_used_total")
	require.True(t, ok)
	assert.Greater(t, count, float64(0))
	count, ok = store.Metrics.Get("redisstore_memory_used_peak")
	require.True(t, ok)
	assert.Greater(t, count, float64(0))
	count, ok = store.Metrics.Get("redisstore_count_keys")
	require.True(t, ok)
	assert.Greater(t, count, float64(0))
}

func BenchmarkStoreWriteSpan(b *testing.B) {
	store, stopper, err := getAndStartSmartWrapper(storeType, &redis.DefaultClient{})
	require.NoError(b, err)
	defer stopper()

	spans := make([]*CentralSpan, 0)
	for i := 0; i < 100; i++ {
		span := &CentralSpan{
			TraceID: fmt.Sprintf("trace%d", i),
			SpanID:  "span1",
		}
		spans = append(spans, span)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.WriteSpan(context.Background(), spans[i%100])
	}
}

func BenchmarkStoreGetStatus(b *testing.B) {
	store, stopper, err := getAndStartSmartWrapper(storeType, &redis.DefaultClient{})
	require.NoError(b, err)
	defer stopper()

	ctx := context.Background()
	spans := make([]*CentralSpan, 0)
	for i := 0; i < 100; i++ {
		span := &CentralSpan{
			TraceID: fmt.Sprintf("trace%d", i),
			SpanID:  "span1",
		}
		spans = append(spans, span)
		store.WriteSpan(ctx, spans[i%100])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.GetStatusForTraces(ctx, []string{spans[i%100].TraceID})
	}
}

func BenchmarkStoreGetTrace(b *testing.B) {
	store, stopper, err := getAndStartSmartWrapper(storeType, &redis.DefaultClient{})
	require.NoError(b, err)
	defer stopper()

	ctx := context.Background()
	spans := make([]*CentralSpan, 0)
	for i := 0; i < 100; i++ {
		span := &CentralSpan{
			TraceID: fmt.Sprintf("trace%d", i),
			SpanID:  "span1",
		}
		spans = append(spans, span)
		store.WriteSpan(ctx, spans[i%100])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.GetTrace(ctx, spans[i%100].TraceID)
	}
}

func BenchmarkStoreGetTracesForState(b *testing.B) {
	// we want things to happen fast, so we'll set the timeouts low
	// sopts.SendDelay = duration("100ms")
	// sopts.TraceTimeout = duration("100ms")
	store, stopper, err := getAndStartSmartWrapper(storeType, &redis.DefaultClient{})
	require.NoError(b, err)
	defer stopper()

	ctx := context.Background()
	spans := make([]*CentralSpan, 0)
	for i := 0; i < 100; i++ {
		span := &CentralSpan{
			TraceID: fmt.Sprintf("trace%d", i),
			SpanID:  "span1",
		}
		spans = append(spans, span)
		store.WriteSpan(ctx, spans[i%100])
	}
	time.Sleep(500 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.GetTracesForState(ctx, ReadyToDecide)
	}
}

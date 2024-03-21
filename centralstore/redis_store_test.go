package centralstore

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/redis"
	"github.com/honeycombio/refinery/metrics"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestRedisBasicStore_TraceStatus(t *testing.T) {
	ctx := context.Background()
	traceID := "traceID0"
	store := NewTestRedisBasicStore(ctx, t)
	defer store.Stop()

	status, err := store.GetStatusForTraces(ctx, []string{traceID})
	require.NoError(t, err)
	require.Len(t, status, 1)
	require.NotNil(t, status[0])
	assert.Equal(t, Unknown, status[0].State)

	testcases := []struct {
		name               string
		span               *CentralSpan
		expectedState      CentralTraceState
		expectedSpanCount  uint32
		expectedEventCount uint32
		expectedLinkCount  uint32
	}{
		{
			name: "first nonroot span",
			span: &CentralSpan{
				TraceID:   traceID,
				SpanID:    "spanID0",
				ParentID:  traceID,
				KeyFields: map[string]interface{}{"foo": "bar"},
				IsRoot:    false,
			},
			expectedState:     Collecting,
			expectedSpanCount: 1,
		},
		{
			name: "event span",
			span: &CentralSpan{
				TraceID:   traceID,
				SpanID:    "spanID1",
				ParentID:  traceID,
				Type:      SpanTypeEvent,
				KeyFields: map[string]interface{}{"event": "bar"},
				IsRoot:    false,
			},
			expectedState:      Collecting,
			expectedSpanCount:  1,
			expectedEventCount: 1,
		},
		{
			name: "link span",
			span: &CentralSpan{
				TraceID:   traceID,
				SpanID:    "spanID2",
				ParentID:  traceID,
				Type:      SpanTypeLink,
				KeyFields: map[string]interface{}{"link": "bar"},
				IsRoot:    false,
			},
			expectedState:      Collecting,
			expectedSpanCount:  1,
			expectedLinkCount:  1,
			expectedEventCount: 1,
		},
		{
			name: "root span",
			span: &CentralSpan{
				TraceID:   traceID,
				SpanID:    "spanID3",
				ParentID:  "",
				KeyFields: map[string]interface{}{"root": "bar"},
				IsRoot:    true,
			},
			expectedState:      DecisionDelay,
			expectedSpanCount:  2,
			expectedLinkCount:  1,
			expectedEventCount: 1,
		},
	}

	var initialTimestamp time.Time
	for i, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, store.WriteSpan(ctx, tc.span))
			status, err := store.GetStatusForTraces(ctx, []string{tc.span.TraceID})
			require.NoError(t, err)
			require.Len(t, status, 1)
			require.NotNil(t, status[0])
			assert.Equal(t, tc.expectedState, status[0].State)
			if i == 0 {
				initialTimestamp = status[0].Timestamp
			}
			assert.NotNil(t, status[0].Timestamp)
			if i > 0 {
				assert.Equal(t, initialTimestamp, status[0].Timestamp)
			}

			assert.Equal(t, tc.expectedEventCount, status[0].SpanEventCount())
			assert.Equal(t, tc.expectedLinkCount, status[0].SpanLinkCount())
			assert.Equal(t, tc.expectedSpanCount, status[0].SpanCount())
		})
	}

}

func TestRedisBasicStore_GetTrace(t *testing.T) {
	ctx := context.Background()
	traceID := "traceID0"
	store := NewTestRedisBasicStore(ctx, t)
	defer store.Stop()
	testSpans := []*CentralSpan{
		{
			TraceID:   traceID,
			SpanID:    "spanID0",
			ParentID:  traceID,
			KeyFields: map[string]interface{}{"foo": "bar"},
			IsRoot:    false,
		},
		{
			TraceID:   traceID,
			SpanID:    "spanID3",
			ParentID:  "",
			KeyFields: map[string]interface{}{"root": "bar"},
			IsRoot:    true,
		},
	}

	for _, span := range testSpans {
		require.NoError(t, store.WriteSpan(ctx, span))
	}

	trace, err := store.GetTrace(ctx, traceID)
	require.NoError(t, err)
	require.NotNil(t, trace)
	require.Equal(t, traceID, trace.TraceID)
	require.Len(t, trace.Spans, 2)
	assert.EqualValues(t, testSpans, trace.Spans)
	assert.EqualValues(t, testSpans[1], trace.Root)
}

func TestRedisBasicStore_ChangeTraceStatus(t *testing.T) {
	ctx := context.Background()

	store := NewTestRedisBasicStore(ctx, t)
	defer store.Stop()

	// write a span to create a trace
	//	test that it can go through different states

	span := &CentralSpan{
		TraceID:   "traceID0",
		SpanID:    "spanID0",
		ParentID:  "parent-spanID0",
		KeyFields: map[string]interface{}{"foo": "bar"},
	}

	require.NoError(t, store.WriteSpan(ctx, span))

	collectingStatus, err := store.GetStatusForTraces(ctx, []string{span.TraceID})
	require.NoError(t, err)
	require.Len(t, collectingStatus, 1)
	assert.Equal(t, Collecting, collectingStatus[0].State)

	store.clock.Advance(time.Duration(1 * time.Second))

	require.NoError(t, store.ChangeTraceStatus(ctx, []string{span.TraceID}, Collecting, DecisionDelay))

	waitingStatus, err := store.GetStatusForTraces(ctx, []string{span.TraceID})
	require.NoError(t, err)
	require.Len(t, waitingStatus, 1)
	assert.Equal(t, DecisionDelay, waitingStatus[0].State)
	assert.True(t, waitingStatus[0].Timestamp.After(collectingStatus[0].Timestamp))

	store.clock.Advance(time.Duration(1 * time.Second))
	require.NoError(t, store.ChangeTraceStatus(ctx, []string{span.TraceID}, DecisionDelay, ReadyToDecide))

	readyStatus, err := store.GetStatusForTraces(ctx, []string{span.TraceID})
	require.NoError(t, err)
	require.Len(t, readyStatus, 1)
	assert.Equal(t, ReadyToDecide, readyStatus[0].State)
	assert.True(t, readyStatus[0].Timestamp.After(waitingStatus[0].Timestamp))

	store.clock.Advance(time.Duration(1 * time.Second))
	require.NoError(t, store.ChangeTraceStatus(ctx, []string{span.TraceID}, ReadyToDecide, AwaitingDecision))

	awaitingStatus, err := store.GetStatusForTraces(ctx, []string{span.TraceID})
	require.NoError(t, err)
	require.Len(t, awaitingStatus, 1)
	assert.Equal(t, AwaitingDecision, awaitingStatus[0].State)
	assert.True(t, awaitingStatus[0].Timestamp.After(readyStatus[0].Timestamp))

	store.clock.Advance(time.Duration(1 * time.Second))
	require.NoError(t, store.ChangeTraceStatus(ctx, []string{span.TraceID}, AwaitingDecision, ReadyToDecide))

	readyStatus, err = store.GetStatusForTraces(ctx, []string{span.TraceID})
	require.NoError(t, err)
	require.Len(t, readyStatus, 1)
	assert.Equal(t, ReadyToDecide, readyStatus[0].State)
	assert.True(t, readyStatus[0].Timestamp.After(awaitingStatus[0].Timestamp))

	store.clock.Advance(time.Duration(1 * time.Second))
	require.NoError(t, store.ChangeTraceStatus(ctx, []string{span.TraceID}, ReadyToDecide, AwaitingDecision))
	awaitingStatus, err = store.GetStatusForTraces(ctx, []string{span.TraceID})
	require.NoError(t, err)

	require.NoError(t, store.KeepTraces(ctx, awaitingStatus))

	keepStatus, err := store.GetStatusForTraces(ctx, []string{span.TraceID})
	require.NoError(t, err)
	require.Len(t, keepStatus, 1)
	assert.Equal(t, DecisionKeep, keepStatus[0].State)
}

func TestRedisBasicStore_GetTracesNeedingDecision(t *testing.T) {
	ctx := context.Background()
	store := NewTestRedisBasicStore(ctx, t)
	defer store.Stop()

	conn := store.RedisClient.Get()
	defer conn.Close()

	traces := []string{"traceID0", "traceID1", "traceID2"}
	for _, id := range traces {
		store.ensureInitialState(t, ctx, conn, id, ReadyToDecide)
	}

	decisionTraces, err := store.GetTracesNeedingDecision(ctx, 1)
	require.NoError(t, err)
	require.Len(t, decisionTraces, 1)
	require.False(t, store.states.exists(ctx, conn, ReadyToDecide, decisionTraces[0]))
	require.False(t, store.states.exists(ctx, conn, ReadyToDecide, decisionTraces[0]))

	decisionTraces, err = store.GetTracesNeedingDecision(ctx, 2)
	require.NoError(t, err)
	require.Len(t, decisionTraces, 2)

	for _, id := range traces {
		require.False(t, store.states.exists(ctx, conn, ReadyToDecide, id))
		require.True(t, store.states.exists(ctx, conn, AwaitingDecision, id))
	}
}

func TestRedisBasicStore_KeepTraces(t *testing.T) {
	ctx := context.Background()
	traceID := "traceID0"
	store := NewTestRedisBasicStore(ctx, t)
	defer store.Stop()

	conn := store.RedisClient.Get()
	defer conn.Close()

	store.ensureInitialState(t, ctx, conn, traceID, AwaitingDecision)
	status, err := store.GetStatusForTraces(ctx, []string{traceID})
	require.NoError(t, err)
	require.NoError(t, store.KeepTraces(ctx, status))

	// make sure it's stored in the decision cache
	record, _, exist := store.DecisionCache.Test(traceID)
	require.True(t, exist)
	require.Equal(t, uint(1), record.DescendantCount())

	// make sure it's state is updated to keep
	status, err = store.GetStatusForTraces(ctx, []string{traceID})
	require.NoError(t, err)
	require.Len(t, status, 1)
	require.Equal(t, DecisionKeep, status[0].State)

	// remove spans linked to trace
	trace, err := store.GetTrace(ctx, traceID)
	require.NoError(t, err)
	require.Empty(t, trace.Spans)
	require.Nil(t, trace.Root)
}

func TestRedisBasicStore_ConcurrentStateChange(t *testing.T) {
	ctx := context.Background()

	traceID := "traceID0"
	store := NewTestRedisBasicStore(ctx, t)
	defer store.Stop()

	require.NoError(t, store.WriteSpan(ctx, &CentralSpan{
		TraceID:   traceID,
		SpanID:    "spanID0",
		ParentID:  traceID,
		KeyFields: map[string]interface{}{"foo": "bar"},
		IsRoot:    false,
	}))

	status, err := store.GetStatusForTraces(ctx, []string{traceID})
	require.NoError(t, err)
	require.Len(t, status, 1)
	require.Equal(t, Collecting, status[0].State)
	initialTimestamp := status[0].Timestamp

	store.clock.Advance(1 * time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = store.ChangeTraceStatus(ctx, []string{traceID}, Collecting, DecisionDelay)
			_ = store.ChangeTraceStatus(ctx, []string{traceID}, DecisionDelay, ReadyToDecide)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		_ = store.ChangeTraceStatus(ctx, []string{traceID}, Collecting, DecisionDelay)
		_ = store.ChangeTraceStatus(ctx, []string{traceID}, DecisionDelay, ReadyToDecide)
		_ = store.ChangeTraceStatus(ctx, []string{traceID}, ReadyToDecide, AwaitingDecision)
	}()

	wg.Wait()

	conn := store.RedisClient.Get()
	defer conn.Close()

	require.False(t, store.states.exists(ctx, conn, Collecting, traceID))
	decisionDelay := store.states.exists(ctx, conn, DecisionDelay, traceID)
	status, err = store.GetStatusForTraces(ctx, []string{traceID})
	require.NoError(t, err)
	require.Len(t, status, 1)
	fmt.Println("state: ", status[0].State)
	require.False(t, decisionDelay)
	require.False(t, store.states.exists(ctx, conn, ReadyToDecide, traceID))
	require.True(t, store.states.exists(ctx, conn, AwaitingDecision, traceID))

	status, err = store.GetStatusForTraces(ctx, []string{traceID})
	require.NoError(t, err)
	require.Len(t, status, 1)
	assert.Equal(t, AwaitingDecision, status[0].State)
	assert.True(t, status[0].Timestamp.After(initialTimestamp))
}

func TestRedisBasicStore_Cleanup(t *testing.T) {
	ctx := context.Background()
	testRedis := &redis.TestService{}
	testRedis.Start()
	defer testRedis.Stop()

	ts := newTestTraceStateProcessor(t, testRedis, nil, noop.NewTracerProvider().Tracer("test"))
	ts.config.maxTraceRetention = 1 * time.Minute
	ts.config.reaperRunInterval = 500 * time.Millisecond
	require.NoError(t, ts.init(testRedis))

	conn := testRedis.Get()
	defer conn.Close()

	traceIDToBeRemoved := "traceID0"
	err := ts.addNewTrace(ctx, conn, traceIDToBeRemoved)
	require.NoError(t, err)
	_, err = ts.toNextState(ctx, conn, newTraceStateChangeEvent(Collecting, DecisionDelay), traceIDToBeRemoved)
	require.NoError(t, err)
	require.True(t, ts.exists(ctx, conn, DecisionDelay, traceIDToBeRemoved))

	ts.clock.Advance(time.Duration(10 * time.Minute))
	traceIDToKeep := "traceID1"
	err = ts.addNewTrace(ctx, conn, traceIDToKeep)
	require.NoError(t, err)
	_, err = ts.toNextState(ctx, conn, newTraceStateChangeEvent(Collecting, DecisionDelay), traceIDToKeep)
	require.NoError(t, err)
	require.True(t, ts.exists(ctx, conn, DecisionDelay, traceIDToKeep))

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		require.False(collect, ts.exists(ctx, conn, DecisionDelay, traceIDToBeRemoved))
		require.True(collect, ts.exists(ctx, conn, DecisionDelay, traceIDToKeep))
	}, 1*time.Second, 200*time.Millisecond)
}

func TestRedisBasicStore_GetMetrics(t *testing.T) {
	ctx := context.Background()
	store := NewTestRedisBasicStore(ctx, t)
	defer store.Stop()

	conn := store.RedisClient.Get()
	defer conn.Close()

	traceID := "traceID0"
	store.ensureInitialState(t, ctx, conn, traceID, ReadyToDecide)

	err := store.RecordMetrics(ctx)
	require.NoError(t, err)
	count, ok := store.Metrics.Get(metricsPrefixCount + ReadyToDecide.String())
	require.True(t, ok)
	assert.EqualValues(t, 1, count)
	count, ok = store.Metrics.Get(metricsPrefixCount + Collecting.String())
	require.True(t, ok)
	assert.EqualValues(t, 0, count)
	count, ok = store.Metrics.Get(metricsPrefixCount + AwaitingDecision.String())
	require.True(t, ok)
	assert.EqualValues(t, 0, count)

	// this is 0 because miniredis doesn't support memory stats command
	// however, the test is still useful to ensure the metrics are being initialized
	// and the function is able to proceed without error in the event of a failure from calling
	// the memory stats command
	count, ok = store.Metrics.Get(metricsPrefixCount + "keys")
	require.True(t, ok)
	assert.EqualValues(t, 0, count)
	count, ok = store.Metrics.Get(metricsPrefixMemory + "used_total")
	require.True(t, ok)
	assert.EqualValues(t, 0, count)
	count, ok = store.Metrics.Get(metricsPrefixMemory + "used_peak")
	require.True(t, ok)
	assert.EqualValues(t, 0, count)

	count, ok = store.Metrics.Get(metricsPrefixConnection + "active")
	require.True(t, ok)
	assert.EqualValues(t, 0, count)
	count, ok = store.Metrics.Get(metricsPrefixConnection + "idle")
	require.True(t, ok)
	assert.EqualValues(t, 0, count)
	count, ok = store.Metrics.Get(metricsPrefixConnection + "wait")
	require.True(t, ok)
	assert.EqualValues(t, 0, count)
	count, ok = store.Metrics.Get(metricsPrefixConnection + "wait_duration")
	require.True(t, ok)
	assert.EqualValues(t, 0, count)
}

func TestRedisBasicStore_ValidStateTransition(t *testing.T) {
	ctx := context.Background()
	testRedis := &redis.TestService{}
	testRedis.Start()
	defer testRedis.Stop()

	traceID := "traceID0"
	ts := newTestTraceStateProcessor(t, testRedis, nil, noop.NewTracerProvider().Tracer("test"))
	require.NoError(t, ts.init(testRedis))
	defer ts.Stop()

	type stateChange struct {
		to      CentralTraceState
		isValid bool
	}

	for _, tc := range []struct {
		state  CentralTraceState
		change stateChange
	}{
		{Collecting, stateChange{Collecting, false}},
		{Collecting, stateChange{DecisionDelay, true}},
		{Collecting, stateChange{ReadyToDecide, false}},
		{Collecting, stateChange{AwaitingDecision, false}},
		{DecisionDelay, stateChange{Collecting, false}},
		{DecisionDelay, stateChange{DecisionDelay, false}},
		{DecisionDelay, stateChange{ReadyToDecide, true}},
		{DecisionDelay, stateChange{AwaitingDecision, false}},
		{ReadyToDecide, stateChange{Collecting, false}},
		{ReadyToDecide, stateChange{DecisionDelay, false}},
		{ReadyToDecide, stateChange{ReadyToDecide, false}},
		{ReadyToDecide, stateChange{AwaitingDecision, true}},
		{AwaitingDecision, stateChange{Collecting, false}},
		{AwaitingDecision, stateChange{DecisionDelay, false}},
		{AwaitingDecision, stateChange{ReadyToDecide, true}},
		{AwaitingDecision, stateChange{AwaitingDecision, false}},
	} {
		t.Run(fmt.Sprintf("%s-%s", tc.state.String(), tc.change.to.String()), func(t *testing.T) {
			tc := tc

			conn := testRedis.Get()
			defer conn.Close()

			ts.ensureInitialState(t, ctx, conn, traceID, tc.state)

			result, err := ts.applyStateChange(ctx, conn, newTraceStateChangeEvent(tc.state, tc.change.to), []string{traceID})
			if tc.change.isValid {
				require.Len(t, result, 1)
				require.Equal(t, traceID, result[0])
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}

		})
	}
}

func TestRedisBasicStore_normalizeCentralTraceStatusRedis(t *testing.T) {
	getFields := func(i interface{}) []string {
		val := reflect.ValueOf(i).Elem()
		typeOfT := val.Type()

		fields := make([]string, val.NumField())
		for i := 0; i < val.NumField(); i++ {
			fields[i] = typeOfT.Field(i).Name
		}
		return fields
	}

	statusInRedis := &centralTraceStatusRedis{}
	status := normalizeCentralTraceStatusRedis(statusInRedis)

	expected := getFields(&CentralTraceStatus{})
	after := getFields(status)

	require.EqualValues(t, expected, after)

}

type TestRedisBasicStore struct {
	*RedisBasicStore
	clock              clockwork.FakeClock
	testStateProcessor *testTraceStateProcessor
	Stop               func()
}

func NewTestRedisBasicStore(ctx context.Context, t *testing.T) *TestRedisBasicStore {
	cfg := config.MockConfig{
		StoreOptions: config.SmartWrapperOptions{
			MaxTraceRetention: duration("1m"),
			ReaperRunInterval: duration("1m"),
		},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          1000,
			DroppedSize:       1000,
			SizeCheckInterval: duration("1s"),
		},
	}
	decisionCache := &cache.CuckooSentCache{}
	clock := clockwork.NewFakeClock()
	metrics := &metrics.MockMetrics{}
	tracer := noop.NewTracerProvider().Tracer("redis_test")
	redis := &redis.TestService{}
	store := &RedisBasicStore{}
	redis.Start()

	objects := []*inject.Object{
		{Value: &cfg},
		{Value: tracer, Name: "tracer"},
		{Value: decisionCache},
		{Value: clock},
		{Value: metrics, Name: "genericMetrics"},
		{Value: redis, Name: "redis"},
		{Value: store},
		{Value: &metrics.NullMetrics{}, Name: "genericMetrics"},
	}
	g := inject.Graph{Logger: dummyLogger{}}
	err := g.Provide(objects...)
	require.NoError(t, err)

	err = g.Populate()
	require.NoError(t, err)

	ststLogger := dummyLogger{}

	fmt.Println(g.Objects())

	err = startstop.Start(g.Objects(), ststLogger)
	require.NoError(t, err)

	stopper := func() {
		startstop.Stop(g.Objects(), ststLogger)
	}

	ts := newTestTraceStateProcessor(t, redis, clock, tracer)
	require.NoError(t, ts.init(redis))
	return &TestRedisBasicStore{
		RedisBasicStore:    store,
		testStateProcessor: ts,
		clock:              clock,
		Stop:               stopper,
	}
}

func (store *TestRedisBasicStore) ensureInitialState(t *testing.T, ctx context.Context, conn redis.Conn, traceID string, state CentralTraceState) {
	store.WriteSpan(context.Background(), &CentralSpan{
		TraceID:   traceID,
		SpanID:    "spanID0",
		ParentID:  "parentID0",
		KeyFields: map[string]interface{}{"foo": "bar"},
		IsRoot:    false,
	})
	store.testStateProcessor.ensureInitialState(t, ctx, conn, traceID, state)
}

type testTraceStateProcessor struct {
	*traceStateProcessor
	clock clockwork.FakeClock
}

func newTestTraceStateProcessor(_ *testing.T, redisClient redis.Client, clock clockwork.FakeClock, tracer trace.Tracer) *testTraceStateProcessor {
	if clock == nil {
		clock = clockwork.NewFakeClock()
	}
	ts := &testTraceStateProcessor{
		traceStateProcessor: newTraceStateProcessor(traceStateProcessorConfig{
			changeState: redisClient.NewScript(stateChangeKey, stateChangeScript),
		}, clock, tracer),
		clock: clock,
	}
	return ts
}

func (ts *testTraceStateProcessor) ensureInitialState(t *testing.T, ctx context.Context, conn redis.Conn, traceID string, state CentralTraceState) {
	for _, state := range ts.states {
		require.NoError(t, ts.remove(ctx, conn, state, traceID))
	}
	_, err := conn.Del(ts.traceStatesKey(traceID))
	require.NoError(t, err)

	require.NoError(t, ts.addNewTrace(ctx, conn, traceID))
	if state == Collecting {
		return
	}

	_, err = ts.toNextState(ctx, conn, newTraceStateChangeEvent(Collecting, DecisionDelay), traceID)
	require.NoError(t, err)
	if state == DecisionDelay {
		return
	}

	_, err = ts.toNextState(ctx, conn, newTraceStateChangeEvent(DecisionDelay, ReadyToDecide), traceID)
	require.NoError(t, err)
	if state == ReadyToDecide {
		return
	}

	_, err = ts.toNextState(ctx, conn, newTraceStateChangeEvent(ReadyToDecide, AwaitingDecision), traceID)
	require.NoError(t, err)
}

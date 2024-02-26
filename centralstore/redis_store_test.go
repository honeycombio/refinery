package centralstore

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/redis"
	"github.com/honeycombio/refinery/metrics"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisBasicStore_TraceStateProcessor(t *testing.T) {
	ctx := context.Background()
	redisClient := NewTestRedis()
	defer redisClient.Stop(ctx)

	ts := newTestTraceStateProcessor(t, redisClient)
	conn := redisClient.Get()
	defer conn.Close()
	traceID := "traceID0"
	require.NoError(t, ts.addNewTrace(conn, traceID))
	require.True(t, ts.exists(conn, Collecting, traceID))

	type stateChange struct {
		to      CentralTraceState
		isValid bool
	}

	for _, tc := range []struct {
		state   CentralTraceState
		changes []stateChange
	}{
		{Collecting, []stateChange{
			{Collecting, false},
			{DecisionDelay, true},
			{ReadyToDecide, false},
			{AwaitingDecision, false},
		}},
		{DecisionDelay, []stateChange{
			{Collecting, false},
			{DecisionDelay, false},
			{ReadyToDecide, true},
			{AwaitingDecision, false},
		}},
		{ReadyToDecide, []stateChange{
			{Collecting, false},
			{DecisionDelay, false},
			{ReadyToDecide, false},
			{AwaitingDecision, true},
		}},
		{AwaitingDecision, []stateChange{
			{Collecting, false},
			{DecisionDelay, false},
			{ReadyToDecide, true},
			{AwaitingDecision, false},
		}},
	} {
		t.Run(tc.state.String(), func(t *testing.T) {
			tc := tc
			for _, change := range tc.changes {
				stateChangeEvent := newTraceStateChangeEvent(tc.state, change.to)
				err := ts.toNextState(conn, stateChangeEvent, traceID)
				if !ts.isValidStateChange(conn, stateChangeEvent, traceID) {
					require.Error(t, err)
					continue
				}

				require.NoError(t, err)

				require.True(t, ts.exists(conn, change.to, traceID))
				require.False(t, ts.exists(conn, tc.state, traceID))
				//TODO: make sure the timestamp is updated
			}
		})
	}
}

func TestRedisBasicStore_ConcurrentStateChange(t *testing.T) {
	ctx := context.Background()
	redisClient := NewTestRedis()
	defer redisClient.Stop(ctx)

	traceID := "traceID0"
	store := NewTestRedisBasicStore(t, redisClient)
	defer store.Stop()

	require.NoError(t, store.WriteSpan(&CentralSpan{
		TraceID:   traceID,
		SpanID:    "spanID0",
		ParentID:  traceID,
		KeyFields: map[string]interface{}{"foo": "bar"},
		IsRoot:    false,
	}))

	status, err := store.GetStatusForTraces([]string{traceID})
	require.NoError(t, err)
	require.Len(t, status, 1)
	initialTimestamp := status[0].Timestamp

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = store.ChangeTraceStatus([]string{traceID}, Collecting, DecisionDelay)
			_ = store.ChangeTraceStatus([]string{traceID}, DecisionDelay, ReadyToDecide)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn := redisClient.Get()
		defer conn.Close()

		_ = store.ChangeTraceStatus([]string{traceID}, Collecting, DecisionDelay)
		_ = store.ChangeTraceStatus([]string{traceID}, DecisionDelay, ReadyToDecide)
		_ = store.ChangeTraceStatus([]string{traceID}, ReadyToDecide, AwaitingDecision)
	}()

	wg.Wait()

	conn := redisClient.Get()
	defer conn.Close()

	require.False(t, store.states.exists(conn, Collecting, traceID))
	require.False(t, store.states.exists(conn, DecisionDelay, traceID))
	require.False(t, store.states.exists(conn, ReadyToDecide, traceID))
	require.True(t, store.states.exists(conn, AwaitingDecision, traceID))
	//TODO: make sure the timestamp is updated

	status, err = store.GetStatusForTraces([]string{traceID})
	require.NoError(t, err)
	require.Len(t, status, 1)
	assert.Equal(t, AwaitingDecision, status[0].State)
	assert.True(t, status[0].Timestamp.After(initialTimestamp))

}

func TestRedisBasicStore_Cleanup(t *testing.T) {
	ctx := context.Background()
	redisClient := NewTestRedis()
	defer redisClient.Stop(ctx)

	ts := newTestTraceStateProcessor(t, redisClient)
	ts.config.maxTraceRetention = 1 * time.Minute

	conn := redisClient.Get()
	defer conn.Close()

	traceIDToBeRemoved := "traceID0"
	err := ts.addNewTrace(conn, traceIDToBeRemoved)
	require.NoError(t, err)
	err = ts.toNextState(conn, newTraceStateChangeEvent(Collecting, DecisionDelay), traceIDToBeRemoved)
	require.NoError(t, err)
	require.True(t, ts.exists(conn, DecisionDelay, traceIDToBeRemoved))

	ts.clock.Advance(time.Duration(10 * time.Minute))
	traceIDToKeep := "traceID1"
	err = ts.addNewTrace(conn, traceIDToKeep)
	require.NoError(t, err)
	err = ts.toNextState(conn, newTraceStateChangeEvent(Collecting, DecisionDelay), traceIDToKeep)
	require.NoError(t, err)
	require.True(t, ts.exists(conn, DecisionDelay, traceIDToKeep))

	ts.removeExpiredTraces(redisClient.Client)
	require.False(t, ts.exists(conn, DecisionDelay, traceIDToBeRemoved))
	require.True(t, ts.exists(conn, DecisionDelay, traceIDToKeep))
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

func TestRedisBasicStore_TraceStatus(t *testing.T) {
	// make sure the trace status is not override by spans arrived after the trace status is set
	ctx := context.Background()
	redisClient := NewTestRedis()
	defer redisClient.Stop(ctx)

	statusStore := newTraceStatusStore(clockwork.NewFakeClock())

	conn := redisClient.Get()
	defer conn.Close()

	traceID0 := "traceID0"
	span := &CentralSpan{
		TraceID:   traceID0,
		SpanID:    "spanID0",
		ParentID:  traceID0,
		KeyFields: map[string]interface{}{"foo": "bar"},
		IsRoot:    false,
	}
	require.NoError(t, statusStore.addStatus(conn, span))
	status, err := statusStore.getTraceStatuses(conn, []string{traceID0})
	require.NoError(t, err)
	require.Len(t, status, 1)
	require.NotNil(t, status[traceID0])
	assert.Equal(t, Unknown, status[traceID0].State)
	span0Timestamp := status[traceID0].Timestamp
	assert.NotNil(t, span0Timestamp)

	span.SpanID = "spanID1"
	require.NoError(t, statusStore.addStatus(conn, span))
	status, err = statusStore.getTraceStatuses(conn, []string{traceID0})
	require.NoError(t, err)
	require.Len(t, status, 1)
	require.NotNil(t, status[traceID0])
	assert.Equal(t, Unknown, status[traceID0].State)
	span1Timestamp := status[traceID0].Timestamp
	assert.NotNil(t, span1Timestamp)
	assert.Equal(t, span0Timestamp, span1Timestamp)
}

type TestRedisBasicStore struct {
	*RedisBasicStore
	clock clockwork.FakeClock
}

func NewTestRedisBasicStore(t *testing.T, redisClient *TestRedisClient) *TestRedisBasicStore {
	clock := clockwork.NewFakeClock()
	opt := RedisBasicStoreOptions{Cache: config.SampleCacheConfig{
		KeptSize:          100,
		DroppedSize:       10000,
		SizeCheckInterval: config.Duration(10 * time.Second),
	}}
	decisionCache, err := cache.NewCuckooSentCache(opt.Cache, &metrics.NullMetrics{})
	require.NoError(t, err)

	ts := newTraceStateProcessor(traceStateProcessorConfig{
		changeState: redisClient.NewScript(stateChangeKey, stateChangeScript),
	}, clock)
	err = ts.Start(context.Background(), redisClient)
	require.NoError(t, err)
	return &TestRedisBasicStore{
		RedisBasicStore: &RedisBasicStore{
			client:        redisClient,
			states:        ts,
			traces:        newTraceStatusStore(clock),
			decisionCache: decisionCache,
			errs:          make(chan error, defaultPendingWorkCapacity),
		},
		clock: clock,
	}
}

type TestRedisClient struct {
	Server *miniredis.Miniredis
	redis.Client
}

func NewTestRedis() *TestRedisClient {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	return &TestRedisClient{
		Server: s,
		Client: redis.NewClient(&redis.Config{Addr: s.Addr()}),
	}
}

func (tr *TestRedisClient) Stop(ctx context.Context) error {
	tr.Client.Stop(ctx)
	tr.Server.Close()
	return nil
}

type testTraceStateProcessor struct {
	*traceStateProcessor
	clock clockwork.FakeClock
}

func newTestTraceStateProcessor(t *testing.T, redisClient *TestRedisClient) *testTraceStateProcessor {
	clock := clockwork.NewFakeClock()
	ts := &testTraceStateProcessor{
		traceStateProcessor: newTraceStateProcessor(traceStateProcessorConfig{
			changeState: redisClient.NewScript(stateChangeKey, stateChangeScript),
		}, clock),
		clock: clock,
	}
	require.NoError(t, ts.Start(context.TODO(), redisClient))
	return ts
}

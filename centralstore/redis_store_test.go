package centralstore

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/honeycombio/refinery/internal/redis"
	"github.com/jonboulle/clockwork"
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
			{WaitingToDecide, true},
			{ReadyForDecision, false},
			{AwaitingDecision, false},
		}},
		{WaitingToDecide, []stateChange{
			{Collecting, false},
			{WaitingToDecide, false},
			{ReadyForDecision, true},
			{AwaitingDecision, false},
		}},
		{ReadyForDecision, []stateChange{
			{Collecting, false},
			{WaitingToDecide, false},
			{ReadyForDecision, false},
			{AwaitingDecision, true},
		}},
		{AwaitingDecision, []stateChange{
			{Collecting, false},
			{WaitingToDecide, false},
			{ReadyForDecision, true},
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
			}
		})
	}
}

func TestRedisBasicStore_ConcurrentStateChange(t *testing.T) {
	ctx := context.Background()
	redisClient := NewTestRedis()
	defer redisClient.Stop(ctx)

	traceID := "traceID0"
	ts := newTestTraceStateProcessor(t, redisClient)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := redisClient.Get()
			defer conn.Close()
			_ = ts.addNewTrace(conn, traceID)
			_ = ts.toNextState(conn, newTraceStateChangeEvent(Collecting, WaitingToDecide), traceID)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn := redisClient.Get()
		defer conn.Close()

		_ = ts.addNewTrace(conn, traceID)
		_ = ts.toNextState(conn, newTraceStateChangeEvent(Collecting, WaitingToDecide), traceID)
		_ = ts.toNextState(conn, newTraceStateChangeEvent(WaitingToDecide, ReadyForDecision), traceID)
	}()

	wg.Wait()

	conn := redisClient.Get()
	defer conn.Close()

	require.False(t, ts.exists(conn, Collecting, traceID))
	require.False(t, ts.exists(conn, WaitingToDecide, traceID))
	require.True(t, ts.exists(conn, ReadyForDecision, traceID))
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
	err = ts.toNextState(conn, newTraceStateChangeEvent(Collecting, WaitingToDecide), traceIDToBeRemoved)
	require.NoError(t, err)
	require.True(t, ts.exists(conn, WaitingToDecide, traceIDToBeRemoved))

	ts.clock.Advance(time.Duration(10 * time.Minute))
	traceIDToKeep := "traceID1"
	err = ts.addNewTrace(conn, traceIDToKeep)
	require.NoError(t, err)
	err = ts.toNextState(conn, newTraceStateChangeEvent(Collecting, WaitingToDecide), traceIDToKeep)
	require.NoError(t, err)
	require.True(t, ts.exists(conn, WaitingToDecide, traceIDToKeep))

	ts.removeExpiredTraces(redisClient.Client)
	require.False(t, ts.exists(conn, WaitingToDecide, traceIDToBeRemoved))
	require.True(t, ts.exists(conn, WaitingToDecide, traceIDToKeep))
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

func (tr *TestRedisClient) Stop(ctx context.Context) {
	tr.Client.Stop(ctx)
	tr.Server.Close()
}

type testTraceStateProcessor struct {
	*testStateProcessor
	clock clockwork.FakeClock
}

func newTestTraceStateProcessor(t *testing.T, redisClient *TestRedisClient) *testTraceStateProcessor {
	require.NoError(t, ensureValidStateChangeEvents(redisClient.Client))
	clock := clockwork.NewFakeClock()
	return &testTraceStateProcessor{
		testStateProcessor: newTraceStateProcessor(traceStateProcessorConfig{
			changeState: redisClient.NewScript(stateChangeKey, stateChangeScript),
		}, clock),
		clock: clock,
	}
}

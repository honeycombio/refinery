//go:build all || !race

package centralstore

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/honeycombio/refinery/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordMetrics(t *testing.T) {
	t.Skip("This test can't run unless there's a real redis available, and it's excluded by build tags locally")

	store, stopper, err := getAndStartSmartWrapper("redis", &redis.DefaultClient{})
	require.NoError(t, err)
	defer stopper()

	ctx := context.Background()
	numberOfTraces := 5
	traceids := make([]string, 0)

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
		states, err := store.GetStatusForTraces(ctx, traceids, []CentralTraceState{Collecting, ReadyToDecide})
		return err == nil && len(states) == numberOfTraces
	}, 1*time.Second, 100*time.Millisecond)

	// wait for it to reach the Ready state
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		states, err := store.GetStatusForTraces(ctx, traceids, []CentralTraceState{ReadyToDecide})
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
		statuses, err = store.GetStatusForTraces(ctx, toDecide, []CentralTraceState{AwaitingDecision})
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
	statuses, err = store.GetStatusForTraces(ctx, traceids, []CentralTraceState{DecisionKeep, DecisionDrop})
	assert.NoError(t, err)
	assert.Equal(t, numberOfTraces, len(statuses))
	for _, status := range statuses {
		if status.TraceID == traceids[0] {
			assert.Equal(t, DecisionKeep, status.State)
			assert.Equal(t, "because", status.KeepReason)
		} else {
			assert.Equal(t, DecisionDrop, status.State)
		}

		err = store.RecordMetrics(ctx)
		require.NoError(t, err)
		_, ok := store.Metrics.Get("redisstore_count_traces")
		require.True(t, ok)

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
}

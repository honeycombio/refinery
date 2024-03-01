package centralstore

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func duration(s string) config.Duration {
	d, _ := time.ParseDuration(s)
	return config.Duration(d)
}

func standardOptions() SmartWrapperOptions {
	sopts := SmartWrapperOptions{
		SpanChannelSize: 100,
		StateTicker:     duration("50ms"),
		SendDelay:       duration("200ms"),
		TraceTimeout:    duration("500ms"),
		DecisionTimeout: duration("500ms"),
	}
	return sopts
}

func noopTracer() trace.Tracer {
	pr := noop.NewTracerProvider()
	return pr.Tracer("test")
}

var storeType = "redis"

func makeRemoteStore() BasicStorer {
	switch storeType {
	case "mysql":
		// this connection string works if you don't have a root password on your local mysql
	// 	s, err := NewMySQLRemoteStore(MySQLRemoteStoreOptions{DSN: "root:@(localhost:3306)/refinery_test"})
	// 	if err != nil {
	// 		panic(fmt.Sprintf("failed to create mysql store: %s", err))
	// 	}
	// 	s.DeleteAllData()
	// 	s.SetupDatabase()
	// 	return s
	case "redis":
		return NewRedisBasicStore(&RedisBasicStoreOptions{})
	case "local":
		return NewLocalRemoteStore()
	}
	return nil
}

func TestSingleSpanGetsCollected(t *testing.T) {
	sopts := standardOptions()
	remoteStore := makeRemoteStore()
	store := NewSmartWrapper(sopts, remoteStore, noopTracer())
	defer store.Stop()
	//defer cleanupRedisStore(t, remoteStore)

	randomNum := rand.Intn(500)
	span := &CentralSpan{
		TraceID:  fmt.Sprintf("trace%d", randomNum),
		SpanID:   fmt.Sprintf("span%d", randomNum),
		ParentID: fmt.Sprintf("parent%d", randomNum), // we don't want this to be a root span
	}
	store.WriteSpan(span)

	// make sure that it arrived in the collecting state
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		states, err := store.GetStatusForTraces([]string{span.TraceID})
		assert.NoError(collect, err)
		assert.Equal(collect, 1, len(states))
		if len(states) > 0 {
			assert.Equal(collect, span.TraceID, states[0].TraceID)
			assert.Equal(collect, Collecting, states[0].State)
		}
	}, 1*time.Second, 10*time.Millisecond)
}

func TestSingleTraceOperation(t *testing.T) {
	sopts := standardOptions()
	remoteStore := makeRemoteStore()
	store := NewSmartWrapper(sopts, remoteStore, noopTracer())
	defer store.Stop()
	defer cleanupRedisStore(t, remoteStore)

	span := &CentralSpan{
		TraceID:  "trace1",
		SpanID:   "span1",
		ParentID: "parent1", // we don't want this to be a root span
	}
	store.WriteSpan(span)

	// make sure that it arrived in the collecting state
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		states, err := store.GetStatusForTraces([]string{span.TraceID})
		assert.NoError(collect, err)
		assert.Equal(collect, 1, len(states))
		if len(states) > 0 {
			assert.Equal(collect, span.TraceID, states[0].TraceID)
			assert.Equal(collect, Collecting, states[0].State)
		}
	}, 1*time.Second, 10*time.Millisecond)

	// it should automatically time out to the Waiting state
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		states, err := store.GetStatusForTraces([]string{span.TraceID})
		assert.NoError(collect, err)
		assert.Equal(collect, 1, len(states))
		if len(states) > 0 {
			assert.Equal(collect, span.TraceID, states[0].TraceID)
			assert.Equal(collect, DecisionDelay, states[0].State)
		}
	}, 1*time.Second, 10*time.Millisecond)

	// and then to the Ready state
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		states, err := store.GetStatusForTraces([]string{span.TraceID})
		assert.NoError(collect, err)
		assert.Equal(collect, 1, len(states))
		if len(states) > 0 {
			assert.Equal(collect, span.TraceID, states[0].TraceID)
			assert.Equal(collect, DecisionDelay, states[0].State)
		}
	}, 1*time.Second, 10*time.Millisecond)
}

func TestBasicStoreOperation(t *testing.T) {
	sopts := standardOptions()
	rs := makeRemoteStore()
	store := NewSmartWrapper(sopts, rs, noopTracer())
	defer store.Stop()
	defer cleanupRedisStore(t, rs)

	traceids := make([]string, 0)

	for t := 0; t < 10; t++ {
		tid := fmt.Sprintf("trace%d", t)
		traceids = append(traceids, tid)
		// write 9 child spans to the store
		for s := 1; s < 10; s++ {
			span := &CentralSpan{
				TraceID:  tid,
				SpanID:   fmt.Sprintf("span%d", s),
				ParentID: fmt.Sprintf("span%d", s-1),
			}
			store.WriteSpan(span)
		}
		// now write the root span
		span := &CentralSpan{
			TraceID: tid,
			SpanID:  "span0",
		}
		store.WriteSpan(span)
	}

	assert.Equal(t, 10, len(traceids))
	fmt.Println(traceids)
	assert.Eventually(t, func() bool {
		states, err := store.GetStatusForTraces(traceids)
		fmt.Println(states, err)
		return err == nil && len(states) == 10
	}, 1*time.Second, 100*time.Millisecond)

	// wait for it to reach the Ready state
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		states, err := store.GetStatusForTraces(traceids)
		assert.NoError(collect, err)
		assert.Equal(collect, 10, len(states))
		for _, state := range states {
			assert.Equal(collect, ReadyToDecide, state.State)
		}
	}, 3*time.Second, 100*time.Millisecond)

	// check that the spans are in the store
	for _, tid := range traceids {
		trace, err := store.GetTrace(tid)
		assert.NoError(t, err)
		if err == nil {
			assert.Equal(t, 10, len(trace.Spans))
			assert.NotNil(t, trace.Root)
		}
	}
}

func TestReadyForDecisionLoop(t *testing.T) {
	sopts := standardOptions()
	remoteStore := makeRemoteStore()
	store := NewSmartWrapper(sopts, remoteStore, noopTracer())
	defer store.Stop()
	defer cleanupRedisStore(t, remoteStore)

	numberOfTraces := 11
	traceids := make([]string, 0)

	for t := 0; t < numberOfTraces; t++ {
		tid := fmt.Sprintf("trace%02d", t)
		traceids = append(traceids, tid)
		// write 9 child spans to the store
		for s := 1; s < 10; s++ {
			span := &CentralSpan{
				TraceID:  tid,
				SpanID:   fmt.Sprintf("span%d", s),
				ParentID: fmt.Sprintf("span%d", s-1),
			}
			store.WriteSpan(span)
		}
		// now write the root span
		span := &CentralSpan{
			TraceID: tid,
			SpanID:  "span0",
		}
		store.WriteSpan(span)
	}

	assert.Equal(t, numberOfTraces, len(traceids))
	fmt.Println(traceids)
	assert.Eventually(t, func() bool {
		states, err := store.GetStatusForTraces(traceids)
		fmt.Println(states, err)
		return err == nil && len(states) == numberOfTraces
	}, 1*time.Second, 100*time.Millisecond)

	// wait for it to reach the Ready state
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		states, err := store.GetStatusForTraces(traceids)
		assert.NoError(collect, err)
		assert.Equal(collect, numberOfTraces, len(states))
		for _, state := range states {
			assert.Equal(collect, ReadyToDecide, state.State)
		}
	}, 3*time.Second, 100*time.Millisecond)

	// get the traces in the Ready state
	toDecide, err := store.GetTracesNeedingDecision(numberOfTraces)
	require.NoError(t, err)
	sort.Strings(toDecide)
	assert.Equal(t, numberOfTraces, len(toDecide))
	assert.EqualValues(t, traceids[:numberOfTraces], toDecide)
}

func TestSetTraceStatuses(t *testing.T) {
	sopts := standardOptions()
	remoteStore := makeRemoteStore()
	store := NewSmartWrapper(sopts, remoteStore, noopTracer())
	defer store.Stop()
	defer cleanupRedisStore(t, remoteStore)

	numberOfTraces := 5
	traceids := make([]string, 0)

	for t := 0; t < numberOfTraces; t++ {
		tid := fmt.Sprintf("trace%02d", t)
		traceids = append(traceids, tid)
		// write 9 child spans to the store
		for s := 1; s < 10; s++ {
			span := &CentralSpan{
				TraceID:  tid,
				SpanID:   fmt.Sprintf("span%d", s),
				ParentID: fmt.Sprintf("span%d", s-1),
			}
			store.WriteSpan(span)
		}
		// now write the root span
		span := &CentralSpan{
			TraceID: tid,
			SpanID:  "span0",
		}
		store.WriteSpan(span)
	}

	assert.Equal(t, numberOfTraces, len(traceids))
	fmt.Println(traceids)
	assert.Eventually(t, func() bool {
		states, err := store.GetStatusForTraces(traceids)
		fmt.Println(states, err)
		return err == nil && len(states) == numberOfTraces
	}, 1*time.Second, 100*time.Millisecond)

	// wait for it to reach the Ready state
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		states, err := store.GetStatusForTraces(traceids)
		assert.NoError(collect, err)
		assert.Equal(collect, numberOfTraces, len(states))
		for _, state := range states {
			assert.Equal(collect, ReadyToDecide, state.State)
		}
	}, 3*time.Second, 100*time.Millisecond)

	// get the traces in the Ready state
	toDecide, err := store.GetTracesNeedingDecision(numberOfTraces)
	assert.NoError(t, err)
	assert.Equal(t, numberOfTraces, len(toDecide))
	sort.Strings(toDecide)
	assert.EqualValues(t, traceids[:numberOfTraces], toDecide)

	statuses := make([]*CentralTraceStatus, 0)
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		statuses, err = store.GetStatusForTraces(toDecide)
		assert.NoError(collect, err)
		assert.Equal(collect, numberOfTraces, len(statuses))
		for _, state := range statuses {
			assert.Equal(collect, AwaitingDecision, state.State)
		}
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
	err = store.SetTraceStatuses(statuses)
	assert.NoError(t, err)

	// we need to give the dropped traces cache a chance to run or it might not process everything
	time.Sleep(50 * time.Millisecond)
	statuses, err = store.GetStatusForTraces(traceids)
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

}

func BenchmarkStoreWriteSpan(b *testing.B) {
	sopts := standardOptions()
	rs := makeRemoteStore()
	store := NewSmartWrapper(sopts, rs, noopTracer())
	defer store.Stop()
	defer cleanupRedisStore(b, rs)

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
		store.WriteSpan(spans[i%100])
	}
}

func BenchmarkStoreGetStatus(b *testing.B) {
	sopts := standardOptions()
	rs := makeRemoteStore()
	store := NewSmartWrapper(sopts, rs, noopTracer())
	defer store.Stop()
	defer cleanupRedisStore(b, rs)

	spans := make([]*CentralSpan, 0)
	for i := 0; i < 100; i++ {
		span := &CentralSpan{
			TraceID: fmt.Sprintf("trace%d", i),
			SpanID:  "span1",
		}
		spans = append(spans, span)
		store.WriteSpan(spans[i%100])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.GetStatusForTraces([]string{spans[i%100].TraceID})
	}
}

func BenchmarkStoreGetTrace(b *testing.B) {
	sopts := standardOptions()
	rs := makeRemoteStore()
	store := NewSmartWrapper(sopts, rs, noopTracer())
	defer store.Stop()
	defer cleanupRedisStore(b, rs)

	spans := make([]*CentralSpan, 0)
	for i := 0; i < 100; i++ {
		span := &CentralSpan{
			TraceID: fmt.Sprintf("trace%d", i),
			SpanID:  "span1",
		}
		spans = append(spans, span)
		store.WriteSpan(spans[i%100])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.GetTrace(spans[i%100].TraceID)
	}
}

func BenchmarkStoreGetTracesForState(b *testing.B) {
	// we want things to happen fast, so we'll set the timeouts low
	sopts := standardOptions()
	sopts.SendDelay = duration("100ms")
	sopts.TraceTimeout = duration("100ms")
	rs := makeRemoteStore()
	store := NewSmartWrapper(sopts, rs, noopTracer())
	defer store.Stop()
	defer cleanupRedisStore(b, rs)

	spans := make([]*CentralSpan, 0)
	for i := 0; i < 100; i++ {
		span := &CentralSpan{
			TraceID: fmt.Sprintf("trace%d", i),
			SpanID:  "span1",
		}
		spans = append(spans, span)
		store.WriteSpan(spans[i%100])
	}
	time.Sleep(500 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.GetTracesForState(ReadyToDecide)
	}
}

func cleanupRedisStore(t testing.TB, store BasicStorer) {
	if r, ok := store.(*RedisBasicStore); ok {
		conn := r.client.Get()
		defer conn.Close()

		_, err := conn.Do("FLUSHALL")
		if err != nil {
			t.Logf("failed to flush redis: %s", err)
		}
	}
}

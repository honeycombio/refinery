package centralstore

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/stretchr/testify/assert"
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

type remoteStoreType string

var (
	localStore remoteStoreType = "local"
	redisStore remoteStoreType = "redis"
)

func makeRemoteStore(storeType remoteStoreType) BasicStorer {
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
	case redisStore:
		return NewRedisBasicStore(RedisBasicStoreOptions{Cache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       10000,
			SizeCheckInterval: config.Duration(10 * time.Second),
		}})
	case localStore:
		return NewLocalRemoteStore()
	}
	return nil
}

func TestSingleSpanGetsCollected(t *testing.T) {
	sopts := standardOptions()
	remoteStore := makeRemoteStore(redisStore)
	defer cleanupRedisStore(remoteStore)
	store := NewSmartWrapper(sopts, remoteStore)
	defer store.Stop()

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
	remoteStore := makeRemoteStore(redisStore)
	defer cleanupRedisStore(remoteStore)
	store := NewSmartWrapper(sopts, remoteStore)
	defer store.Stop()

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
			assert.Equal(collect, WaitingToDecide, states[0].State)
		}
	}, 1*time.Second, 10*time.Millisecond)

	// and then to the Ready state
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		states, err := store.GetStatusForTraces([]string{span.TraceID})
		assert.NoError(collect, err)
		assert.Equal(collect, 1, len(states))
		if len(states) > 0 {
			assert.Equal(collect, span.TraceID, states[0].TraceID)
			assert.Equal(collect, WaitingToDecide, states[0].State)
		}
	}, 1*time.Second, 10*time.Millisecond)
}

func TestBasicStoreOperation(t *testing.T) {
	sopts := standardOptions()
	redisStore := NewRedisBasicStore(RedisBasicStoreOptions{Cache: config.SampleCacheConfig{
		KeptSize:          100,
		DroppedSize:       10000,
		SizeCheckInterval: config.Duration(10 * time.Second),
	}})
	defer cleanupRedisStore(redisStore)
	store := NewSmartWrapper(sopts, redisStore)
	defer store.Stop()

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
			assert.Equal(collect, ReadyForDecision, state.State)
		}
	}, 3*time.Second, 100*time.Millisecond)

	// check that the spans are in the store
	for _, tid := range traceids {
		trace, err := store.GetTrace(tid)
		assert.NoError(t, err)
		if err == nil {
			assert.Equal(t, 10, len(trace.Spans))
			// assert.NotNil(t, trace.Root)
		}
	}
}

func TestReadyForDecisionLoop(t *testing.T) {
	sopts := standardOptions()
	remoteStore := makeRemoteStore(redisStore)
	defer cleanupRedisStore(remoteStore)
	store := NewSmartWrapper(sopts, remoteStore)
	defer store.Stop()

	numberOfTraces := 11
	traceids := make([]string, 0)

	for t := 0; t < numberOfTraces; t++ {
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
			assert.Equal(collect, ReadyForDecision, state.State)
		}
	}, 3*time.Second, 100*time.Millisecond)

	// get the traces in the Ready state
	traceIDs, err := store.GetTracesNeedingDecision(numberOfTraces - 1)
	assert.NoError(t, err)
	assert.Equal(t, numberOfTraces-1, len(traceIDs))
	assert.EqualValues(t, traceids[:numberOfTraces-1], traceIDs)
}

func BenchmarkStoreWriteSpan(b *testing.B) {
	sopts := standardOptions()
	redisStore := NewRedisBasicStore(RedisBasicStoreOptions{Cache: config.SampleCacheConfig{
		KeptSize:          100,
		DroppedSize:       10000,
		SizeCheckInterval: config.Duration(10 * time.Second),
	}})
	defer cleanupRedisStore(redisStore)
	store := NewSmartWrapper(sopts, redisStore)
	defer store.Stop()

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
	redisStore := NewRedisBasicStore(RedisBasicStoreOptions{Cache: config.SampleCacheConfig{
		KeptSize:          100,
		DroppedSize:       10000,
		SizeCheckInterval: config.Duration(10 * time.Second),
	}})
	defer cleanupRedisStore(redisStore)
	store := NewSmartWrapper(sopts, redisStore)
	defer store.Stop()

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
	redisStore := NewRedisBasicStore(RedisBasicStoreOptions{Cache: config.SampleCacheConfig{
		KeptSize:          100,
		DroppedSize:       10000,
		SizeCheckInterval: config.Duration(10 * time.Second),
	}})
	defer cleanupRedisStore(redisStore)
	store := NewSmartWrapper(sopts, redisStore)
	defer store.Stop()

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
	redisStore := NewRedisBasicStore(RedisBasicStoreOptions{Cache: config.SampleCacheConfig{
		KeptSize:          100,
		DroppedSize:       10000,
		SizeCheckInterval: config.Duration(10 * time.Second),
	}})
	defer cleanupRedisStore(redisStore)
	store := NewSmartWrapper(sopts, redisStore)
	defer store.Stop()

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
		store.GetTracesForState(ReadyForDecision)
	}
}

func cleanupRedisStore(store BasicStorer) error {
	if r, ok := store.(*RedisBasicStore); ok {
		conn := r.client.Get()
		defer conn.Close()

		_, err := conn.Do("FLUSHALL")
		if err != nil {
			return err
		}
	}
	return nil
}

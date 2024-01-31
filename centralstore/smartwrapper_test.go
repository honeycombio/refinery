package centralstore

import (
	"fmt"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/stretchr/testify/assert"
)

func duration(s string) config.Duration {
	d, _ := time.ParseDuration(s)
	return config.Duration(d)
}

func standardOptions() (SmartWrapperOptions, LRSOptions) {
	sopts := SmartWrapperOptions{
		SpanChannelSize: 100,
		StateTicker:     duration("10ms"),
		SendDelay:       duration("250ms"),
		TraceTimeout:    duration("500ms"),
		DecisionTimeout: duration("100ms"),
	}
	lopts := LRSOptions{
		KeptSize:          100,
		DroppedSize:       100,
		SizeCheckInterval: duration("10s"),
	}
	return sopts, lopts
}

func TestSingleTraceOperation(t *testing.T) {
	sopts, lopts := standardOptions()
	store := NewSmartWrapper(sopts, NewLocalRemoteStore(lopts))
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
		assert.Equal(collect, span.TraceID, states[0].TraceID)
		assert.Equal(collect, Collecting, states[0].State)
	}, 1*time.Second, 10*time.Millisecond)

	// it should automatically time out to the Waiting state
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		states, err := store.GetStatusForTraces([]string{span.TraceID})
		assert.NoError(collect, err)
		assert.Equal(collect, 1, len(states))
		assert.Equal(collect, span.TraceID, states[0].TraceID)
		assert.Equal(collect, WaitingToDecide, states[0].State)
	}, 1*time.Second, 10*time.Millisecond)

	// and then to the Ready state
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		states, err := store.GetStatusForTraces([]string{span.TraceID})
		assert.NoError(collect, err)
		assert.Equal(collect, 1, len(states))
		assert.Equal(collect, span.TraceID, states[0].TraceID)
		assert.Equal(collect, ReadyForDecision, states[0].State)
	}, 1*time.Second, 10*time.Millisecond)
}

func TestBasicStoreOperation(t *testing.T) {
	sopts, lopts := standardOptions()
	store := NewSmartWrapper(sopts, NewLocalRemoteStore(lopts))
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

	// wait for it to reach the Ready state
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		states, err := store.GetStatusForTraces(traceids)
		assert.NoError(collect, err)
		assert.Equal(collect, 10, len(states))
		for _, state := range states {
			assert.Equal(collect, ReadyForDecision, state.State)
		}
	}, 3*time.Second, 10*time.Millisecond)

	// check that the spans are in the store
	for _, tid := range traceids {
		trace, err := store.GetTrace(tid)
		assert.NoError(t, err)
		assert.Equal(t, 10, len(trace.Spans))
		assert.NotNil(t, trace.Root)
	}
}

func BenchmarkStoreWriteSpan(b *testing.B) {
	sopts, lopts := standardOptions()
	store := NewSmartWrapper(sopts, NewLocalRemoteStore(lopts))

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
	sopts, lopts := standardOptions()
	store := NewSmartWrapper(sopts, NewLocalRemoteStore(lopts))

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
	sopts, lopts := standardOptions()
	store := NewSmartWrapper(sopts, NewLocalRemoteStore(lopts))

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
	sopts, lopts := standardOptions()
	sopts.SendDelay = duration("100ms")
	sopts.TraceTimeout = duration("100ms")
	store := NewSmartWrapper(sopts, NewLocalRemoteStore(lopts))

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

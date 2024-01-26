package centralstore

import (
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/stretchr/testify/assert"
)

func duration(s string) config.Duration {
	d, _ := time.ParseDuration(s)
	return config.Duration(d)
}

func createNewInMemStore() *InMemStore {
	opts := InMemStoreOptions{
		KeptSize:          100,
		DroppedSize:       100,
		SpanChannelSize:   100,
		SizeCheckInterval: duration("10s"),
		StateTicker:       duration("10ms"),
		SendDelay:         duration("250ms"),
		TraceTimeout:      duration("500ms"),
		DecisionTimeout:   duration("100ms"),
	}

	return NewInMemStore(opts)
}

func TestBasicStoreOperation(t *testing.T) {
	store := createNewInMemStore()
	defer store.Stop()

	span := &CentralSpan{
		TraceID:  "trace1",
		SpanID:   "span1",
		ParentID: "parent1", // we don't want this to be a root span
	}
	store.WriteSpan(span)

	// check that the span is in the store
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		store.mutex.Lock()
		defer store.mutex.Unlock()
		_, ok := store.traces[span.TraceID]
		assert.True(collect, ok)
	}, 1*time.Second, 10*time.Millisecond)

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

package route

import (
	"maps"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinylib/msgp/msgp"
	"github.com/vmihailenco/msgpack/v5"
)

func TestBatchedEventRoundTrip(t *testing.T) {
	timestamp := time.Now()
	mockCfg := &config.MockConfig{
		ParentIdFieldNames: []string{"trace.parent_id"},
	}

	// Create events slice
	events := []batchedEvent{
		{
			MsgPackTimestamp: &timestamp,
			cfg:              mockCfg,
			Data: types.NewPayload(map[string]any{
				"string_field":    "test_value",
				"trace.parent_id": "parent-123",
				"int_field":       int64(42),
				"float_field":     3.14159,
				"bool_field":      true,
				"nil_field":       nil,
			}, mockCfg),
		},
		{
			MsgPackTimestamp: &timestamp,
			SampleRate:       200,
			cfg:              mockCfg,
			Data: types.NewPayload(map[string]any{
				"another_string":  "hello world",
				"trace.parent_id": "parent-123",
				"negative_int":    int64(-123),
				"large_float":     1234.5678,
				"false_bool":      false,
				"nil_value":       nil,
			}, mockCfg),
		},
	}

	// Create batchedEvents struct and set events
	batchStruct := newBatchedEvents(mockCfg)
	batchStruct.events = events

	serialized, err := msgpack.Marshal(batchStruct.events) // Marshal the slice directly
	require.NoError(t, err)

	// Create new batchedEvents for unmarshaling
	deserialized := newBatchedEvents(mockCfg)
	_, err = deserialized.UnmarshalMsg(serialized)
	require.NoError(t, err)

	require.Len(t, deserialized.events, 2)
	deserializedEvents := deserialized.events
	for i := range deserializedEvents {
		assert.WithinDuration(t, timestamp, *deserializedEvents[i].MsgPackTimestamp, 0)
		assert.Equal(t, events[i].SampleRate, deserializedEvents[i].SampleRate)
		assert.Equal(t, maps.Collect(events[i].Data.All()), maps.Collect(deserializedEvents[i].Data.All()))
	}

	// Test overwriting with another unmarshal, with swapped events order.
	events[0], events[1] = events[1], events[0]
	batchStruct.events = events
	serialized, err = msgpack.Marshal(batchStruct.events)
	require.NoError(t, err)

	// Reset the deserialized batch
	deserialized = newBatchedEvents(mockCfg)
	_, err = deserialized.UnmarshalMsg(serialized)
	require.NoError(t, err)

	require.Len(t, deserialized.events, 2)
	deserializedEvents = deserialized.events
	for i := range deserializedEvents {
		assert.WithinDuration(t, timestamp, *deserializedEvents[i].MsgPackTimestamp, 0)
		assert.Equal(t, events[i].SampleRate, deserializedEvents[i].SampleRate)
		assert.Equal(t, maps.Collect(events[i].Data.All()), maps.Collect(deserializedEvents[i].Data.All()))
	}
}

func TestBatchedEventsUnmarshalMsgWithMetadata(t *testing.T) {
	mockCfg := &config.MockConfig{
		TraceIdFieldNames:  []string{"trace.trace_id"},
		ParentIdFieldNames: []string{"trace.parent_id"},
	}

	// Create multiple test events
	events := []struct {
		traceID    string
		parentID   string
		signalType string
		sampleRate int64
	}{
		{"trace-1", "", "span", 1},
		{"trace-2", "parent-2", "span", 5},
		{"trace-3", "", "log", 10},
	}

	// Marshal the batch
	var buf []byte
	buf = msgp.AppendArrayHeader(buf, uint32(len(events)))

	for _, event := range events {
		// Create event data
		eventData := map[string]interface{}{
			"trace.trace_id":   event.traceID,
			"trace.parent_id":  event.parentID,
			"meta.signal_type": event.signalType,
			"field":            "value",
		}

		// Marshal the event data
		var dataBuf []byte
		dataBuf = msgp.AppendMapHeader(dataBuf, uint32(len(eventData)))
		for k, v := range eventData {
			dataBuf = msgp.AppendString(dataBuf, k)
			if s, ok := v.(string); ok {
				dataBuf = msgp.AppendString(dataBuf, s)
			}
		}

		// Marshal the batched event
		buf = msgp.AppendMapHeader(buf, 2) // samplerate, data
		buf = msgp.AppendString(buf, "samplerate")
		buf = msgp.AppendInt64(buf, event.sampleRate)
		buf = msgp.AppendString(buf, "data")
		buf = append(buf, dataBuf...)
	}

	// Test the optimized unmarshal
	batch := newBatchedEvents(mockCfg)

	remaining, err := batch.UnmarshalMsg(buf)
	require.NoError(t, err)
	assert.Empty(t, remaining)

	// Verify all events were unmarshaled with metadata
	batchEvents := batch.events
	require.Len(t, batchEvents, 3)

	// Event 0: root span (no parent)
	assert.Equal(t, "trace-1", batchEvents[0].Data.MetaTraceID)
	assert.True(t, batchEvents[0].Data.MetaRefineryRoot.HasValue)
	assert.True(t, batchEvents[0].Data.MetaRefineryRoot.Value)

	// Event 1: non-root span (has parent)
	assert.Equal(t, "trace-2", batchEvents[1].Data.MetaTraceID)
	assert.True(t, batchEvents[1].Data.MetaRefineryRoot.HasValue)
	assert.False(t, batchEvents[1].Data.MetaRefineryRoot.Value)

	// Event 2: log (never root)
	assert.Equal(t, "trace-3", batchEvents[2].Data.MetaTraceID)
	assert.Equal(t, "log", batchEvents[2].Data.MetaSignalType)
	assert.False(t, batchEvents[2].Data.MetaRefineryRoot.HasValue)
	assert.False(t, batchEvents[2].Data.MetaRefineryRoot.Value)
}

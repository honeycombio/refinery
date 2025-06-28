package route

import (
	"maps"
	"testing"
	"time"

	"github.com/honeycombio/refinery/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinylib/msgp/msgp"
	"github.com/vmihailenco/msgpack/v5"
)

func TestBatchedEventRoundTrip(t *testing.T) {
	timestamp := time.Now()

	events := batchedEvents{
		{
			MsgPackTimestamp: &timestamp,
			Data: types.NewPayload(map[string]any{
				"string_field": "test_value",
				"int_field":    int64(42),
				"float_field":  3.14159,
				"bool_field":   true,
				"nil_field":    nil,
			}),
		},
		{
			MsgPackTimestamp: &timestamp,
			SampleRate:       200,
			Data: types.NewPayload(map[string]any{
				"another_string": "hello world",
				"negative_int":   int64(-123),
				"large_float":    1234.5678,
				"false_bool":     false,
				"nil_value":      nil,
			}),
		},
	}

	serialized, err := msgpack.Marshal(events)
	require.NoError(t, err)

	var deserialized batchedEvents
	_, err = deserialized.UnmarshalMsg(serialized)
	require.NoError(t, err)

	require.Len(t, deserialized, 2)
	for i := range deserialized {
		assert.WithinDuration(t, timestamp, *deserialized[i].MsgPackTimestamp, 0)
		assert.Equal(t, events[i].SampleRate, deserialized[i].SampleRate)
		assert.Equal(t, maps.Collect(events[i].Data.All()), maps.Collect(deserialized[i].Data.All()))
	}

	// Test overwriting with another unmarshal, with swapped events order.
	events[0], events[1] = events[1], events[0]
	serialized, err = msgpack.Marshal(events)
	require.NoError(t, err)

	deserialized = deserialized[:0]
	_, err = deserialized.UnmarshalMsg(serialized)
	require.NoError(t, err)
	require.Len(t, deserialized, 2)
	for i := range deserialized {
		assert.WithinDuration(t, timestamp, *deserialized[i].MsgPackTimestamp, 0)
		assert.Equal(t, events[i].SampleRate, deserialized[i].SampleRate)
		assert.Equal(t, maps.Collect(events[i].Data.All()), maps.Collect(deserialized[i].Data.All()))
	}
}

func TestBatchedEventsUnmarshalMsgWithMetadata(t *testing.T) {
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
	var batch batchedEvents
	traceIdFields := []string{"trace.trace_id"}
	parentIdFields := []string{"trace.parent_id"}

	remaining, err := batch.UnmarshalMsgWithMetadata(buf, traceIdFields, parentIdFields)
	require.NoError(t, err)
	assert.Empty(t, remaining)

	// Verify all events were unmarshaled with metadata
	require.Len(t, batch, 3)

	// Event 0: root span (no parent)
	assert.Equal(t, "trace-1", batch[0].Data.MetaTraceID)
	assert.True(t, batch[0].Data.MetaRefineryRoot.HasValue)
	assert.True(t, batch[0].Data.MetaRefineryRoot.Value)

	// Event 1: non-root span (has parent)
	assert.Equal(t, "trace-2", batch[1].Data.MetaTraceID)
	assert.True(t, batch[1].Data.MetaRefineryRoot.HasValue)
	assert.False(t, batch[1].Data.MetaRefineryRoot.Value)

	// Event 2: log (never root)
	assert.Equal(t, "trace-3", batch[2].Data.MetaTraceID)
	assert.Equal(t, "log", batch[2].Data.MetaSignalType)
	assert.True(t, batch[2].Data.MetaRefineryRoot.HasValue)
	assert.False(t, batch[2].Data.MetaRefineryRoot.Value)
}

package route

import (
	"maps"
	"testing"
	"time"

	"github.com/honeycombio/refinery/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
}

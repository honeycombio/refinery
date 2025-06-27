package types

import (
	"encoding/json"
	"fmt"
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestPayload(t *testing.T) {
	data := map[string]any{
		"key1":   "value1",
		"key2":   int64(42),
		"key3":   3.14,
		"key4":   true,
		"keyNil": nil,
		// Add some metadata fields to test
		"meta.trace_id":          "test-trace-123",
		"meta.refinery.root":     true,
		"meta.refinery.min_span": false,
		"meta.annotation_type":   "span_event",
	}

	var ph Payload
	doTest := func(t *testing.T) {
		assert.True(t, ph.Exists("key1"))
		assert.Equal(t, "value1", ph.Get("key1"))
		assert.True(t, ph.Exists("key2"))
		assert.Equal(t, int64(42), ph.Get("key2"))
		assert.True(t, ph.Exists("keyNil"))
		assert.Nil(t, ph.Get("keyNil"))
		assert.False(t, ph.Exists("nonexistent"))
		assert.Nil(t, ph.Get("nonexistent"))

		// Test metadata fields through Get()
		assert.True(t, ph.Exists("meta.trace_id"))
		assert.Equal(t, "test-trace-123", ph.Get("meta.trace_id"))

		assert.True(t, ph.Exists("meta.refinery.root"))
		assert.Equal(t, true, ph.Get("meta.refinery.root"))

		assert.True(t, ph.Exists("meta.refinery.min_span"))
		assert.Equal(t, false, ph.Get("meta.refinery.min_span"))

		assert.True(t, ph.Exists("meta.annotation_type"))
		assert.Equal(t, "span_event", ph.Get("meta.annotation_type"))

		// Test dedicated fields - should be populated from initial data or unmarshal
		assert.Equal(t, "test-trace-123", ph.MetaTraceID, "MetaTraceID should be populated")
		assert.True(t, ph.MetaRefineryRoot.HasValue, "MetaRefineryRoot should be set")
		assert.Equal(t, true, ph.MetaRefineryRoot.Value, "MetaRefineryRoot should be populated")
		assert.True(t, ph.MetaRefineryMinSpan.HasValue, "MetaRefineryMinSpan should be set")
		assert.Equal(t, false, ph.MetaRefineryMinSpan.Value, "MetaRefineryMinSpan should be populated")
		assert.Equal(t, "span_event", ph.MetaAnnotationType, "MetaAnnotationType should be populated")

		ph.Set("key5", "newvalue")
		assert.True(t, ph.Exists("key5"))
		assert.Equal(t, "newvalue", ph.Get("key5"))

		// Test setting metadata fields through Set()
		// Verify that Set() DOES update dedicated fields to keep them in sync

		ph.Set("meta.refinery.span_data_size", int64(1234))
		assert.Equal(t, int64(1234), ph.Get("meta.refinery.span_data_size"))
		// Verify dedicated field WAS updated by Set()
		assert.Equal(t, int64(1234), ph.MetaRefinerySpanDataSize, "Set() should update dedicated field")

		ph.Set("meta.refinery.incoming_user_agent", "test-agent/1.0")
		assert.Equal(t, "test-agent/1.0", ph.Get("meta.refinery.incoming_user_agent"))
		// Verify dedicated field WAS updated by Set()
		assert.Equal(t, "test-agent/1.0", ph.MetaRefineryIncomingUserAgent, "Set() should update dedicated field")

		// Overwrite an existing value
		ph.Set("key3", 4.13)
		assert.True(t, ph.Exists("key3"))
		assert.Equal(t, 4.13, ph.Get("key3"))

		expected := maps.Clone(data)
		expected["key3"] = 4.13
		expected["key5"] = "newvalue"
		expected["meta.refinery.span_data_size"] = int64(1234)
		expected["meta.refinery.incoming_user_agent"] = "test-agent/1.0"
		found := maps.Collect(ph.All())
		assert.Equal(t, expected, found)

		// test memoization
		ph.MemoizeFields("key1", "key2", "key3", "missingkey")
		assert.True(t, ph.Exists("key1"))
		assert.True(t, ph.Exists("key2"))
		assert.True(t, ph.Exists("key3"))
		assert.False(t, ph.Exists("missingkey"))

		asJSON, err := json.Marshal(ph)
		require.NoError(t, err)

		var fromJSON Payload
		err = json.Unmarshal(asJSON, &fromJSON)
		require.NoError(t, err)
		err = fromJSON.ExtractMetadata(nil, nil)
		require.NoError(t, err)

		// round-tripping through JSON turns our ints into floats
		expectedFromJSON := maps.Collect(ph.All())
		expectedFromJSON["key2"] = 42.0
		expectedFromJSON["meta.refinery.span_data_size"] = 1234.0
		assert.EqualValues(t, expectedFromJSON, maps.Collect(fromJSON.All()))
	}

	ph = NewPayload(data)
	err := ph.ExtractMetadata(nil, nil)
	require.NoError(t, err)
	t.Run("from_map", doTest)

	ph = Payload{}
	msgpData, err := msgpack.Marshal(data)
	require.NoError(t, err)
	err = msgpack.Unmarshal(msgpData, &ph)
	require.NoError(t, err)
	ph.ExtractMetadata(nil, nil)
	t.Run("from_msgpack", doTest)

	// Test payload with other stuff (another payload) following.
	ph = Payload{}
	extendedMsgpData := append(msgpData, msgpData...)
	remainder, err := ph.UnmarshalMsg(extendedMsgpData)
	require.NoError(t, err)
	assert.Equal(t, msgpData, remainder)
	ph.ExtractMetadata(nil, nil)
	t.Run("from_msgp", doTest)

	// Test our own marshaler
	msgpData, err = ph.MarshalMsg(nil)
	require.NoError(t, err)
	ph = Payload{}
	remainder, err = ph.UnmarshalMsg(msgpData)
	require.NoError(t, err)
	assert.Empty(t, remainder)
	ph.ExtractMetadata(nil, nil)
	t.Run("from_marshal", doTest)
}

func TestPayloadExtractMetadataWithFieldNames(t *testing.T) {
	t.Run("extract trace ID from custom field", func(t *testing.T) {
		data := map[string]any{
			"trace.trace_id": "custom-trace-123",
			"service.name":   "test-service",
		}
		ph := NewPayload(data)
		ph.ExtractMetadata([]string{"trace.trace_id", "traceId"}, nil)

		assert.Equal(t, "custom-trace-123", ph.MetaTraceID, "Should extract trace ID from custom field")
		assert.Equal(t, "custom-trace-123", ph.Get("trace.trace_id"))
	})

	t.Run("root span detection with parent ID", func(t *testing.T) {
		// No parent ID - should be root
		data := map[string]any{
			"trace.trace_id": "trace-123",
			"span.id":        "span-456",
		}
		ph := NewPayload(data)
		ph.ExtractMetadata([]string{"trace.trace_id"}, []string{"trace.parent_id", "parentId"})

		assert.Equal(t, "trace-123", ph.MetaTraceID)
		assert.True(t, ph.MetaRefineryRoot.HasValue, "Root flag should be set")
		assert.True(t, ph.MetaRefineryRoot.Value, "Should be root span when no parent ID")

		// With parent ID - should not be root
		data = map[string]any{
			"trace.trace_id":  "trace-789",
			"trace.parent_id": "parent-123",
			"span.id":         "span-789",
		}
		ph = NewPayload(data)
		ph.ExtractMetadata([]string{"trace.trace_id"}, []string{"trace.parent_id", "parentId"})

		assert.Equal(t, "trace-789", ph.MetaTraceID)
		assert.True(t, ph.MetaRefineryRoot.HasValue, "Root flag should be set")
		assert.False(t, ph.MetaRefineryRoot.Value, "Should not be root span when parent ID exists")
	})

	t.Run("log events are never root", func(t *testing.T) {
		data := map[string]any{
			"meta.signal_type": "log",
			"trace.trace_id":   "trace-123",
		}
		ph := NewPayload(data)
		ph.ExtractMetadata([]string{"trace.trace_id"}, []string{"trace.parent_id"})

		assert.Equal(t, "log", ph.MetaSignalType)
		assert.True(t, ph.MetaRefineryRoot.HasValue, "Root flag should be set")
		assert.False(t, ph.MetaRefineryRoot.Value, "Log events should never be root")
	})
}

func TestPayloadExtractMetadataError(t *testing.T) {
	t.Run("invalid msgpack data", func(t *testing.T) {
		// Create a payload with invalid msgpack data
		p := Payload{
			msgpMap: MsgpPayloadMap{
				rawData: []byte{0xFF, 0xFF, 0xFF}, // Invalid msgpack
			},
		}

		err := p.ExtractMetadata(nil, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create msgpack iterator")
	})
}

func TestPayloadGetSetMetadataSync(t *testing.T) {
	t.Run("Set updates metadata fields", func(t *testing.T) {
		ph := NewPayload(map[string]any{})

		// Test string fields
		ph.Set("meta.signal_type", "trace")
		assert.Equal(t, "trace", ph.MetaSignalType)
		assert.Equal(t, "trace", ph.Get("meta.signal_type"))

		ph.Set("meta.trace_id", "test-trace-456")
		assert.Equal(t, "test-trace-456", ph.MetaTraceID)
		assert.Equal(t, "test-trace-456", ph.Get("meta.trace_id"))

		// Test boolean fields
		ph.Set("meta.refinery.probe", true)
		assert.True(t, ph.MetaRefineryProbe.HasValue)
		assert.True(t, ph.MetaRefineryProbe.Value)
		assert.Equal(t, true, ph.Get("meta.refinery.probe"))

		ph.Set("meta.refinery.root", false)
		assert.True(t, ph.MetaRefineryRoot.HasValue)
		assert.False(t, ph.MetaRefineryRoot.Value)
		assert.Equal(t, false, ph.Get("meta.refinery.root"))

		// Test int64 fields
		ph.Set("meta.refinery.send_by", int64(12345))
		assert.Equal(t, int64(12345), ph.MetaRefinerySendBy)
		assert.Equal(t, int64(12345), ph.Get("meta.refinery.send_by"))

		ph.Set("meta.refinery.span_data_size", int64(67890))
		assert.Equal(t, int64(67890), ph.MetaRefinerySpanDataSize)
		assert.Equal(t, int64(67890), ph.Get("meta.refinery.span_data_size"))
	})

	t.Run("Get returns from dedicated fields", func(t *testing.T) {
		ph := NewPayload(map[string]any{})

		// Set fields directly
		ph.MetaSignalType = "log"
		ph.MetaTraceID = "direct-trace-123"
		ph.MetaRefineryProbe.Set(true)
		ph.MetaRefineryRoot.Set(false)
		ph.MetaRefinerySendBy = 54321

		// Get should return from dedicated fields
		assert.Equal(t, "log", ph.Get("meta.signal_type"))
		assert.Equal(t, "direct-trace-123", ph.Get("meta.trace_id"))
		assert.Equal(t, true, ph.Get("meta.refinery.probe"))
		assert.Equal(t, false, ph.Get("meta.refinery.root"))
		assert.Equal(t, int64(54321), ph.Get("meta.refinery.send_by"))
	})

	t.Run("Get returns nil for unset boolean fields", func(t *testing.T) {
		ph := NewPayload(map[string]any{})

		// Boolean fields without values should return nil
		assert.Nil(t, ph.Get("meta.refinery.probe"))
		assert.Nil(t, ph.Get("meta.refinery.root"))
		assert.Nil(t, ph.Get("meta.refinery.min_span"))
		assert.Nil(t, ph.Get("meta.refinery.expired_trace"))
	})
}

func BenchmarkPayload(b *testing.B) {
	// Create test data with many fields
	var keys []string

	data := make(map[string]any)
	for i := 0; i < 100; i++ {
		keys = append(keys, fmt.Sprintf("key%d", i))
		data[fmt.Sprintf("key%d", i)] = fmt.Sprintf("value%d", i)
		data[fmt.Sprintf("num%d", i)] = int64(i)
		data[fmt.Sprintf("float%d", i)] = float64(i) * 1.5
		data[fmt.Sprintf("bool%d", i)] = i%2 == 0
	}

	// Create msgp data using msgpack.Marshal
	msgpData, err := msgpack.Marshal(data)
	require.NoError(b, err)

	phMap := NewPayload(data)
	phMap.ExtractMetadata(nil, nil)
	var phMsgp Payload
	err = msgpack.Unmarshal(msgpData, &phMsgp)
	require.NoError(b, err)
	phMsgp.ExtractMetadata(nil, nil)

	b.Run("create_map", func(b *testing.B) {
		for b.Loop() {
			var m map[string]any
			_ = msgpack.Unmarshal(msgpData, &m)
			_ = NewPayload(m)
		}
	})

	b.Run("create_msgpack", func(b *testing.B) {
		for b.Loop() {
			var phMsgp Payload
			_ = phMsgp.UnmarshalMsgpack(msgpData)
		}
	})

	b.Run("create_msgp", func(b *testing.B) {
		for b.Loop() {
			var phMsgp Payload
			_, _ = phMsgp.UnmarshalMsg(msgpData)
		}
	})

	for _, num := range []int{1, 5, 10, 25} {
		b.Run(fmt.Sprintf("get_map/%d", num), func(b *testing.B) {
			modulo := len(keys) - num
			for n := range b.N {
				// Look at a variety of keys, since their (random) position in
				// the serialized data has a big impact on lookup time.
				offset := n % modulo
				for _, key := range keys[offset : offset+num] {
					_ = phMap.Get(key)
				}
			}
		})

		b.Run(fmt.Sprintf("get_msgp/%d", num), func(b *testing.B) {
			modulo := len(keys) - num
			for n := range b.N {
				offset := n % modulo
				for _, key := range keys[offset : offset+num] {
					_ = phMsgp.Get(key)
				}
			}
		})

		b.Run(fmt.Sprintf("get_memo/%d", num), func(b *testing.B) {
			modulo := len(keys) - num
			for n := range b.N {
				offset := n % modulo
				var phMsgpMemo Payload
				_ = phMsgpMemo.UnmarshalMsgpack(msgpData)
				phMsgpMemo.MemoizeFields(keys[offset : offset+num]...)
				for _, key := range keys[offset : offset+num] {
					_ = phMsgpMemo.Get(key)
				}
			}
		})
	}

	b.Run("iter_map", func(b *testing.B) {
		for b.Loop() {
			for k, v := range phMap.All() {
				_ = k
				_ = v
			}
		}
	})

	b.Run("iter_msgp", func(b *testing.B) {
		for b.Loop() {
			for k, v := range phMsgp.All() {
				_ = k
				_ = v
			}
		}
	})

	b.Run("marshal_msg", func(b *testing.B) {
		var buf []byte
		for b.Loop() {
			buf, _ = phMsgp.MarshalMsg(buf[:0])
		}
	})

	b.Run("get_unknown", func(b *testing.B) {
		_ = phMap.Get("get-unknown")
		// subsequent calls to unknown fields are fast, since they are memoized
		for b.Loop() {
			_ = phMsgp.Get("get-unknown")
		}
	})
	b.Run("exist_unknown", func(b *testing.B) {
		_ = phMap.Exists("exist-unknown")
		// subsequent calls to unknown fields are fast, since they are memoized
		for b.Loop() {
			_ = phMsgp.Exists("exist-unknown")
		}
	})
}

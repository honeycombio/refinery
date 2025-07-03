package types

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestMetadataFieldsHaveMetaPrefix(t *testing.T) {
	for key := range metadataFields {
		assert.True(t, strings.HasPrefix(key, "meta."))
	}
}

func TestPayload(t *testing.T) {
	data := map[string]any{
		"key1":   "value1",
		"key2":   int64(42),
		"key3":   3.14,
		"key4":   true,
		"keyNil": nil,
		// Add some metadata fields to test
		"trace.trace_id":         "test-trace-123",
		"meta.refinery.root":     true,
		"meta.refinery.min_span": false,
		"meta.annotation_type":   "span_event",

		// This is the wrong type, it should just be ignored and dropped from the data.
		// TODO enable on this is implemented.
		// "meta.refinery.send_by": "never",
	}
	mockCfg := &config.MockConfig{
		TraceIdFieldNames: []string{"trace.trace_id"},
	}

	ph := NewPayload(mockCfg, data)
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

		assert.Nil(t, ph.Get("meta.refinery.send_by"))
		assert.False(t, ph.Exists("meta.refinery.send_by"))

		// Test dedicated fields - should be populated from initial data or unmarshal
		assert.Equal(t, "test-trace-123", ph.MetaTraceID, "MetaTraceID should be populated")
		assert.True(t, ph.MetaRefineryRoot.HasValue, "MetaRefineryRoot should be set")
		assert.Equal(t, true, ph.MetaRefineryRoot.Value, "MetaRefineryRoot should be populated")
		assert.True(t, ph.MetaRefineryMinSpan.HasValue, "MetaRefineryMinSpan should be set")
		assert.Equal(t, false, ph.MetaRefineryMinSpan.Value, "MetaRefineryMinSpan should be populated")
		assert.Equal(t, "span_event", ph.MetaAnnotationType, "MetaAnnotationType should be populated")
		assert.Equal(t, int64(0), ph.MetaRefinerySendBy)

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

		fromJSON := NewPayload(mockCfg, nil)
		err = json.Unmarshal(asJSON, &fromJSON)
		require.NoError(t, err)

		// round-tripping through JSON turns our ints into floats
		expectedFromJSON := maps.Collect(ph.All())
		expectedFromJSON["key2"] = 42.0
		expectedFromJSON["meta.refinery.span_data_size"] = 1234.0
		assert.EqualValues(t, expectedFromJSON, maps.Collect(fromJSON.All()))
	}

	ph = NewPayload(mockCfg, data)
	ph.ExtractMetadata()
	t.Run("from_map", doTest)

	ph = NewPayload(mockCfg, nil)
	msgpData, err := msgpack.Marshal(data)
	require.NoError(t, err)
	err = msgpack.Unmarshal(msgpData, &ph)
	require.NoError(t, err)
	t.Run("from_msgpack", doTest)

	// Test payload with other stuff (another payload) following.
	ph = NewPayload(mockCfg, nil)
	extendedMsgpData := append(msgpData, msgpData...)
	remainder, err := ph.UnmarshalMsg(extendedMsgpData)
	require.NoError(t, err)
	assert.Equal(t, msgpData, remainder)
	t.Run("from_msgp", doTest)

	// Test our own marshaler
	msgpData, err = ph.MarshalMsg(nil)
	require.NoError(t, err)
	ph = NewPayload(mockCfg, nil)
	remainder, err = ph.UnmarshalMsg(msgpData)
	require.NoError(t, err)
	assert.Empty(t, remainder)
	t.Run("from_marshal", doTest)
}

func TestPayloadExtractMetadataWithFieldNames(t *testing.T) {
	t.Run("extract trace ID from custom field", func(t *testing.T) {
		data := map[string]any{
			"trace.trace_id": "custom-trace-123",
			"service.name":   "test-service",
		}
		ph := NewPayload(&config.MockConfig{
			TraceIdFieldNames: []string{"trace.trace_id"},
		}, data)
		ph.ExtractMetadata()

		assert.Equal(t, "custom-trace-123", ph.MetaTraceID, "Should extract trace ID from custom field")
		assert.Equal(t, "custom-trace-123", ph.Get("trace.trace_id"))
	})

	t.Run("root span detection with parent ID", func(t *testing.T) {
		// No parent ID - should be root
		data := map[string]any{
			"trace.trace_id": "trace-123",
			"span.id":        "span-456",
		}
		ph := NewPayload(&config.MockConfig{
			TraceIdFieldNames:  []string{"trace.trace_id"},
			ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		}, data)
		ph.ExtractMetadata()

		assert.Equal(t, "trace-123", ph.MetaTraceID)
		assert.True(t, ph.MetaRefineryRoot.HasValue, "Root flag should be set")
		assert.True(t, ph.MetaRefineryRoot.Value, "Should be root span when no parent ID")

		// With parent ID - should not be root
		data = map[string]any{
			"trace.trace_id":  "trace-789",
			"trace.parent_id": "parent-123",
			"span.id":         "span-789",
		}
		ph = NewPayload(&config.MockConfig{
			TraceIdFieldNames:  []string{"trace.trace_id"},
			ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		}, data)
		ph.ExtractMetadata()

		assert.Equal(t, "trace-789", ph.MetaTraceID)
		assert.True(t, ph.MetaRefineryRoot.HasValue, "Root flag should be set")
		assert.False(t, ph.MetaRefineryRoot.Value, "Should not be root span when parent ID exists")
	})

	t.Run("log events are never root", func(t *testing.T) {
		data := map[string]any{
			"meta.refinery.root": true,
			"meta.signal_type":   "log",
			"trace.trace_id":     "trace-123",
		}
		ph := NewPayload(&config.MockConfig{
			TraceIdFieldNames:  []string{"trace.trace_id"},
			ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		}, data)
		ph.ExtractMetadata()

		assert.Equal(t, "log", ph.MetaSignalType)
		assert.False(t, ph.MetaRefineryRoot.HasValue, "Root flag should be set")
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
			config: &config.MockConfig{
				TraceIdFieldNames:  []string{"trace.trace_id"},
				ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
			},
		}

		err := p.ExtractMetadata()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read msgpack map header")
	})
}

func TestPayloadUnmarshalMsg(t *testing.T) {
	t.Run("extracts metadata during unmarshal", func(t *testing.T) {
		// Create test data with all metadata fields
		data := map[string]any{
			// Regular fields
			"regular_field": "value1",
			"another_field": 42,

			// Core metadata fields
			"meta.signal_type":     "trace",
			"meta.annotation_type": "span_event",

			// Refinery boolean fields
			"meta.refinery.probe":         true,
			"meta.refinery.min_span":      true,
			"meta.refinery.expired_trace": false,
			"meta.refinery.shutdown_send": true,
			"meta.stressed":               true,

			// Refinery string fields
			"meta.refinery.incoming_user_agent": "test-agent/1.0",
			"meta.refinery.forwarded":           "192.168.1.1",
			"meta.refinery.local_hostname":      "test-host",
			"meta.refinery.reason":              "deterministic",
			"meta.refinery.send_reason":         "trace_timeout",
			"meta.refinery.sample_key":          "sample-key-123",

			// Refinery int64 fields
			"meta.refinery.send_by":              int64(1234567890),
			"meta.refinery.span_data_size":       int64(2048),
			"meta.span_event_count":              int64(5),
			"meta.span_link_count":               int64(3),
			"meta.span_count":                    int64(10),
			"meta.event_count":                   int64(15),
			"meta.refinery.original_sample_rate": int64(100),

			// Custom trace/parent fields
			"trace.trace_id": "custom-trace-456", // Custom trace ID field
			"span.parent_id": "",                 // Empty parent ID should make it root
		}

		msgpData, err := msgpack.Marshal(data)
		require.NoError(t, err)

		// Create payload and unmarshal with metadata extraction
		p := Payload{
			config: &config.MockConfig{
				TraceIdFieldNames:  []string{"wrong", "trace.trace_id"},
				ParentIdFieldNames: []string{"span.parent_id"},
			},
		}
		remainder, err := p.UnmarshalMsg(msgpData)
		require.NoError(t, err)
		assert.Empty(t, remainder)

		// Verify core metadata was extracted
		assert.Equal(t, "trace", p.MetaSignalType)
		assert.Equal(t, "custom-trace-456", p.MetaTraceID) // Should use custom field over meta.trace_id
		assert.Equal(t, "span_event", p.MetaAnnotationType)

		// Verify boolean metadata fields
		assert.True(t, p.MetaRefineryProbe.HasValue)
		assert.True(t, p.MetaRefineryProbe.Value)
		assert.True(t, p.MetaRefineryRoot.HasValue)
		assert.True(t, p.MetaRefineryRoot.Value) // Should be true because empty parent ID
		assert.True(t, p.MetaRefineryMinSpan.HasValue)
		assert.True(t, p.MetaRefineryMinSpan.Value)
		assert.True(t, p.MetaRefineryExpiredTrace.HasValue)
		assert.False(t, p.MetaRefineryExpiredTrace.Value)
		assert.True(t, p.MetaRefineryShutdownSend.HasValue)
		assert.True(t, p.MetaRefineryShutdownSend.Value)
		assert.True(t, p.MetaStressed.HasValue)
		assert.True(t, p.MetaStressed.Value)

		// Verify string metadata fields
		assert.Equal(t, "test-agent/1.0", p.MetaRefineryIncomingUserAgent)
		assert.Equal(t, "192.168.1.1", p.MetaRefineryForwarded)
		assert.Equal(t, "test-host", p.MetaRefineryLocalHostname)
		assert.Equal(t, "deterministic", p.MetaRefineryReason)
		assert.Equal(t, "trace_timeout", p.MetaRefinerySendReason)
		assert.Equal(t, "sample-key-123", p.MetaRefinerySampleKey)

		// Verify int64 metadata fields
		assert.Equal(t, int64(1234567890), p.MetaRefinerySendBy)
		assert.Equal(t, int64(2048), p.MetaRefinerySpanDataSize)
		assert.Equal(t, int64(5), p.MetaSpanEventCount)
		assert.Equal(t, int64(3), p.MetaSpanLinkCount)
		assert.Equal(t, int64(10), p.MetaSpanCount)
		assert.Equal(t, int64(15), p.MetaEventCount)
		assert.Equal(t, int64(100), p.MetaRefineryOriginalSampleRate)

		// Verify regular fields are still accessible
		assert.Equal(t, "value1", p.Get("regular_field"))
		assert.Equal(t, int64(42), p.Get("another_field")) // should preserve int type
	})

	t.Run("handles remainder correctly", func(t *testing.T) {
		data1 := map[string]any{"trace.trace_id": "trace1"}
		data2 := map[string]any{"trace.trace_id": "trace2"}

		msgpData1, _ := msgpack.Marshal(data1)
		msgpData2, _ := msgpack.Marshal(data2)
		combined := append(msgpData1, msgpData2...)

		p1 := Payload{
			config: &config.MockConfig{
				TraceIdFieldNames:  []string{"trace.trace_id"},
				ParentIdFieldNames: []string{"span.parent_id"},
			},
		}
		remainder, err := p1.UnmarshalMsg(combined)
		assert.Equal(t, "trace1", p1.MetaTraceID)
		assert.Equal(t, msgpData2, remainder)

		// Verify we can unmarshal the remainder
		p2 := Payload{
			config: &config.MockConfig{
				TraceIdFieldNames:  []string{"trace.trace_id"},
				ParentIdFieldNames: []string{"span.parent_id"},
			},
		}
		remainder2, err := p2.UnmarshalMsg(remainder)
		require.NoError(t, err)
		assert.Equal(t, "trace2", p2.MetaTraceID)
		assert.Empty(t, remainder2)
	})
}

func TestPayloadGetSetExistMetadataSync(t *testing.T) {
	t.Run("Set updates metadata fields", func(t *testing.T) {
		ph := NewPayload(&config.MockConfig{}, nil)

		// Test string fields
		assert.False(t, ph.Exists("meta.signal_type"))
		ph.Set("meta.signal_type", "trace")
		assert.Equal(t, "trace", ph.MetaSignalType)
		assert.Equal(t, "trace", ph.Get("meta.signal_type"))
		assert.True(t, ph.Exists("meta.signal_type"))

		assert.False(t, ph.Exists("meta.trace_id"))
		ph.Set("meta.trace_id", "test-trace-456")
		assert.Equal(t, "test-trace-456", ph.MetaTraceID)
		assert.Equal(t, "test-trace-456", ph.Get("meta.trace_id"))
		assert.True(t, ph.Exists("meta.trace_id"))

		// Test boolean fields
		assert.False(t, ph.Exists("meta.refinery.probe"))
		ph.Set("meta.refinery.probe", true)
		assert.True(t, ph.MetaRefineryProbe.HasValue)
		assert.True(t, ph.MetaRefineryProbe.Value)
		assert.Equal(t, true, ph.Get("meta.refinery.probe"))
		assert.True(t, ph.Exists("meta.refinery.probe"))

		assert.False(t, ph.Exists("meta.refinery.root"))
		ph.Set("meta.refinery.root", false)
		assert.True(t, ph.MetaRefineryRoot.HasValue)
		assert.False(t, ph.MetaRefineryRoot.Value)
		assert.Equal(t, false, ph.Get("meta.refinery.root"))
		assert.True(t, ph.Exists("meta.refinery.root"))

		// Test int64 fields
		assert.False(t, ph.Exists("meta.refinery.send_by"))
		ph.Set("meta.refinery.send_by", int64(12345))
		assert.Equal(t, int64(12345), ph.MetaRefinerySendBy)
		assert.Equal(t, int64(12345), ph.Get("meta.refinery.send_by"))
		assert.True(t, ph.Exists("meta.refinery.send_by"))

		assert.False(t, ph.Exists("meta.refinery.span_data_size"))
		ph.Set("meta.refinery.span_data_size", int64(67890))
		assert.Equal(t, int64(67890), ph.MetaRefinerySpanDataSize)
		assert.Equal(t, int64(67890), ph.Get("meta.refinery.span_data_size"))
		assert.True(t, ph.Exists("meta.refinery.span_data_size"))
	})

	t.Run("Get returns from dedicated fields", func(t *testing.T) {
		ph := NewPayload(&config.MockConfig{}, nil)

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

		// Exists should also work when fields are set directly
		assert.True(t, ph.Exists("meta.signal_type"))
		assert.True(t, ph.Exists("meta.trace_id"))
		assert.True(t, ph.Exists("meta.refinery.probe"))
		assert.True(t, ph.Exists("meta.refinery.root"))
		assert.True(t, ph.Exists("meta.refinery.send_by"))
	})

	t.Run("Get returns nil for unset boolean fields", func(t *testing.T) {
		ph := NewPayload(&config.MockConfig{}, nil)

		// Boolean fields without values should return nil and not exist
		assert.Nil(t, ph.Get("meta.refinery.probe"))
		assert.False(t, ph.Exists("meta.refinery.probe"))

		assert.Nil(t, ph.Get("meta.refinery.root"))
		assert.False(t, ph.Exists("meta.refinery.root"))

		assert.Nil(t, ph.Get("meta.refinery.min_span"))
		assert.False(t, ph.Exists("meta.refinery.min_span"))

		assert.Nil(t, ph.Get("meta.refinery.expired_trace"))
		assert.False(t, ph.Exists("meta.refinery.expired_trace"))
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

	phMap := NewPayload(&config.MockConfig{}, data)
	phMsgp := NewPayload(&config.MockConfig{}, nil)
	err = msgpack.Unmarshal(msgpData, &phMsgp)
	require.NoError(b, err)

	b.Run("create_map", func(b *testing.B) {
		for b.Loop() {
			var m map[string]any
			_ = msgpack.Unmarshal(msgpData, &m)
			_ = NewPayload(&config.MockConfig{}, m)
		}
	})

	b.Run("create_msgpack", func(b *testing.B) {
		for b.Loop() {
			phMsgp := NewPayload(&config.MockConfig{}, nil)
			_ = phMsgp.UnmarshalMsgpack(msgpData)
		}
	})

	b.Run("create_msgp", func(b *testing.B) {
		for b.Loop() {
			phMsgp := NewPayload(&config.MockConfig{}, nil)
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
				phMsgpMemo := NewPayload(&config.MockConfig{}, nil)
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

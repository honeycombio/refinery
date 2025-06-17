package types

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestMsgpPayloadMap_WorkingCases(t *testing.T) {
	inputData := map[string]any{
		"string_field":   "hello world",
		"int_field":      int8(42),
		"negative_int":   int64(-123),
		"float_field":    float32(4.125),
		"bool_true":      true,
		"bool_false":     false,
		"empty_string":   "",
		"zero_int":       int(0),
		"zero_float":     0.0,
		"large_int":      int64(9223372036854775807),  // max int64
		"small_int":      int64(-9223372036854775808), // min int64
		"unicode_string": "café ñoño 🚀",
		"nested_map":     map[string]any{"inner": "value"},
		"array_field":    []any{int64(1), int64(2), int64(3)},
		"nil_field":      nil,
	}

	// Serialize test data
	encoded, err := msgpack.Marshal(inputData)
	require.NoError(t, err)

	// Create MsgpPayloadMap
	payloadMap := NewMessagePackPayloadMap(encoded)

	// Test iteration
	iter, err := payloadMap.Iterate()
	require.NoError(t, err)

	gotData := make(map[string]any)
	for {
		key, typ, err := iter.NextKey()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		keyStr := string(key)
		switch typ {
		case FieldTypeString:
			val, err := iter.ValueString()
			require.NoError(t, err)
			assert.EqualValues(t, inputData[keyStr], val)
			gotData[keyStr] = val
		case FieldTypeInt64:
			val, err := iter.ValueInt64()
			require.NoError(t, err)
			assert.EqualValues(t, inputData[keyStr], val)
			gotData[keyStr] = val
		case FieldTypeFloat64:
			val, err := iter.ValueFloat64()
			require.NoError(t, err)
			assert.EqualValues(t, inputData[keyStr], val)
			gotData[keyStr] = val
		case FieldTypeBool:
			val, err := iter.ValueBool()
			require.NoError(t, err)
			assert.EqualValues(t, inputData[keyStr], val)
			gotData[keyStr] = val
		case FieldTypeOther:
			val, err := iter.ValueAny()
			require.NoError(t, err)
			assert.EqualValues(t, inputData[keyStr], val)
			gotData[keyStr] = val
		default:
			assert.Fail(t, "Unexpected field type", "Unexpected field type %v for key %s", typ, keyStr)
		}
	}
	assert.Len(t, gotData, len(inputData))

	// Test skipping all values
	iter, err = payloadMap.Iterate()
	require.NoError(t, err)

	skipCount := 0
	for {
		_, _, err := iter.NextKey()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		skipCount++
		// Don't call any Value methods - test that skipping works
	}
	assert.Equal(t, len(inputData), skipCount)

	// Test skipping some, but not all values
	iter, err = payloadMap.Iterate()
	require.NoError(t, err)

	clear(gotData)
	skipCount = 0
	for {
		key, typ, err := iter.NextKey()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		if typ == FieldTypeInt64 {
			val, err := iter.ValueInt64()
			require.NoError(t, err)
			assert.EqualValues(t, inputData[string(key)], val)
			gotData[string(key)] = val
		} else {
			skipCount++
		}
	}
	assert.Equal(t, len(inputData), skipCount+len(gotData))

	// Test empty map
	emptyData := map[string]any{}
	emptyEncoded, err := msgpack.Marshal(emptyData)
	require.NoError(t, err)

	emptyPayloadMap := NewMessagePackPayloadMap(emptyEncoded)
	emptyIter, err := emptyPayloadMap.Iterate()
	require.NoError(t, err)

	_, _, err = emptyIter.NextKey()
	assert.Equal(t, io.EOF, err)
}

func TestMsgpPayloadMap_ErrorCases(t *testing.T) {
	// Test invalid msgpack data
	invalidData := []byte{0xFF, 0xFF, 0xFF}
	invalidPayloadMap := NewMessagePackPayloadMap(invalidData)

	_, err := invalidPayloadMap.Iterate()
	assert.Error(t, err)

	// Test non-map msgpack data (array)
	arrayData := []any{1, 2, 3}
	arrayEncoded, err := msgpack.Marshal(arrayData)
	require.NoError(t, err)

	arrayPayloadMap := NewMessagePackPayloadMap(arrayEncoded)
	_, err = arrayPayloadMap.Iterate()
	assert.Error(t, err)

	// Test empty data - iterating is ok, but EOFs immediately.
	emptyPayloadMap := NewMessagePackPayloadMap(nil)
	iter, err := emptyPayloadMap.Iterate()
	assert.NoError(t, err)
	_, _, err = iter.NextKey()
	assert.Error(t, err)

	// Test truncated data, should fail on the second value.
	validData := map[string]any{"key": "value"}
	validEncoded, err := msgpack.Marshal(validData)
	require.NoError(t, err)

	truncatedData := validEncoded[:len(validEncoded)-2]
	truncatedPayloadMap := NewMessagePackPayloadMap(truncatedData)
	truncatedIter, err := truncatedPayloadMap.Iterate()
	assert.NoError(t, err)
	_, _, err = truncatedIter.NextKey()
	assert.NoError(t, err)
	_, _, err = truncatedIter.NextKey()
	assert.Error(t, err)

	// Test type mismatch errors by creating a map with specific types
	// and attempting to read them as wrong types
	inputData := map[string]any{
		"string_field": "hello",
		"int_field":    int64(42),
		"float_field":  3.14,
		"bool_field":   true,
	}

	encoded, err := msgpack.Marshal(inputData)
	require.NoError(t, err)

	payloadMap := NewMessagePackPayloadMap(encoded)
	iter, err = payloadMap.Iterate()
	require.NoError(t, err)

	for {
		_, typ, err := iter.NextKey()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		// Try to access with wrong types
		switch typ {
		case FieldTypeString:
			// Try to read as int
			_, err := iter.ValueInt64()
			assert.Error(t, err)
		case FieldTypeInt64:
			// Try to read as string
			_, err := iter.ValueString()
			assert.Error(t, err)
		case FieldTypeFloat64:
			// Try to read as bool
			_, err := iter.ValueBool()
			assert.Error(t, err)
		case FieldTypeBool:
			// Try to read as float
			_, err := iter.ValueFloat64()
			assert.Error(t, err)
		}
	}

	// Test calling Value methods without NextKey
	simpleData := map[string]any{"test": "value"}
	simpleEncoded, err := msgpack.Marshal(simpleData)
	require.NoError(t, err)

	freshPayloadMap := NewMessagePackPayloadMap(simpleEncoded)
	freshIter, err := freshPayloadMap.Iterate()
	require.NoError(t, err)

	_, err = freshIter.ValueString()
	assert.Error(t, err)
}

func BenchmarkMsgpPayloadMap(b *testing.B) {
	testData := map[string]any{
		"trace_id":        "abc123def456",
		"span_id":         "span789",
		"duration_ms":     int64(1500),
		"error_rate":      0.05,
		"success":         true,
		"service_name":    "my-service",
		"endpoint":        "/api/v1/users",
		"status_code":     int64(200),
		"bytes_sent":      int64(2048),
		"response_time":   125.5,
		"cache_hit":       false,
		"user_id":         int64(12345),
		"session_id":      "sess_abcdef123456",
		"region":          "us-west-2",
		"version":         "1.2.3",
		"build_id":        int64(987654321),
		"cpu_usage":       0.75,
		"memory_mb":       int64(512),
		"disk_io":         1024.0,
		"network_latency": 25.3,
	}

	encoded, err := msgpack.Marshal(testData)
	require.NoError(b, err)

	b.Run("iter_values", func(b *testing.B) {
		for b.Loop() {
			payloadMap := NewMessagePackPayloadMap(encoded)
			iter, _ := payloadMap.Iterate()

			fieldCount := 0
			for {
				_, typ, err := iter.NextKey()
				if err == io.EOF {
					break
				}

				// Access the value based on type
				switch typ {
				case FieldTypeString:
					_, _ = iter.ValueString()
				case FieldTypeInt64:
					_, _ = iter.ValueInt64()
				case FieldTypeFloat64:
					_, _ = iter.ValueFloat64()
				case FieldTypeBool:
					_, _ = iter.ValueBool()
				case FieldTypeOther:
					_, _ = iter.ValueAny()
				}
				fieldCount++
			}

			if fieldCount != len(testData) {
				b.Fatal("field count mismatch")
			}
		}
	})

	b.Run("iter_novalues", func(b *testing.B) {
		for b.Loop() {
			payloadMap := NewMessagePackPayloadMap(encoded)
			iter, _ := payloadMap.Iterate()

			fieldCount := 0
			for {
				_, _, err := iter.NextKey()
				if err == io.EOF {
					break
				}
				fieldCount++
				// Don't access values - just skip them
			}

			if fieldCount != len(testData) {
				b.Fatal("field count mismatch")
			}
		}
	})

	b.Run("legacy", func(b *testing.B) {
		for b.Loop() {
			var result map[string]any
			_ = msgpack.Unmarshal(encoded, &result)

			// Iterate through the deserialized map
			fieldCount := 0
			for key, value := range result {
				_ = key
				_ = value
				fieldCount++
			}

			if fieldCount != len(testData) {
				b.Fatal("field count mismatch")
			}
		}
	})
}

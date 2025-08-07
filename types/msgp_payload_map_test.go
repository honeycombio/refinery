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

	// Create iterator
	iter, err := newMsgpPayloadMapIter(encoded)
	require.NoError(t, err)

	gotData := make(map[string]any)
	for {
		key, typ, err := iter.nextKey()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		assert.NotEqual(t, FieldTypeUnknown, typ)

		keyStr := string(key)
		val, err := iter.valueAny()
		require.NoError(t, err)
		assert.EqualValues(t, inputData[keyStr], val)
		gotData[keyStr] = val
	}
	assert.Len(t, gotData, len(inputData))

	// Test skipping all values
	iter, err = newMsgpPayloadMapIter(encoded)
	require.NoError(t, err)

	skipCount := 0
	for {
		_, _, err := iter.nextKey()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		skipCount++
		// Don't call any Value methods - test that skipping works
	}
	assert.Equal(t, len(inputData), skipCount)

	// Test skipping some, but not all values
	iter, err = newMsgpPayloadMapIter(encoded)
	require.NoError(t, err)

	clear(gotData)
	skipCount = 0
	for {
		key, typ, err := iter.nextKey()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		if typ == FieldTypeInt64 {
			val, err := iter.valueAny()
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

	emptyIter, err := newMsgpPayloadMapIter(emptyEncoded)
	require.NoError(t, err)

	_, _, err = emptyIter.nextKey()
	assert.Equal(t, io.EOF, err)
}

func TestMsgpPayloadMap_ErrorCases(t *testing.T) {
	// Test invalid msgpack data
	invalidData := []byte{0xFF, 0xFF, 0xFF}
	_, err := newMsgpPayloadMapIter(invalidData)
	assert.Error(t, err)

	// Test non-map msgpack data (array)
	arrayData := []any{1, 2, 3}
	arrayEncoded, err := msgpack.Marshal(arrayData)
	require.NoError(t, err)

	_, err = newMsgpPayloadMapIter(arrayEncoded)
	assert.Error(t, err)

	// Test empty data - iterating is ok, but EOFs immediately.
	iter, err := newMsgpPayloadMapIter(nil)
	assert.NoError(t, err)
	_, _, err = iter.nextKey()
	assert.Error(t, err)

	// Test truncated data, should fail on the second value.
	validData := map[string]any{"key": "value"}
	validEncoded, err := msgpack.Marshal(validData)
	require.NoError(t, err)

	truncatedData := validEncoded[:len(validEncoded)-2]
	truncatedIter, err := newMsgpPayloadMapIter(truncatedData)
	assert.NoError(t, err)
	_, _, err = truncatedIter.nextKey()
	assert.NoError(t, err)
	_, _, err = truncatedIter.nextKey()
	assert.Error(t, err)

	// Test calling Value methods without NextKey
	simpleData := map[string]any{"test": "value"}
	simpleEncoded, err := msgpack.Marshal(simpleData)
	require.NoError(t, err)

	freshIter, err := newMsgpPayloadMapIter(simpleEncoded)
	require.NoError(t, err)

	_, err = freshIter.valueAny()
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
			iter, _ := newMsgpPayloadMapIter(encoded)

			fieldCount := 0
			for {
				_, _, err := iter.nextKey()
				if err == io.EOF {
					break
				}

				_, _ = iter.valueAny()
				fieldCount++
			}

			if fieldCount != len(testData) {
				b.Fatal("field count mismatch")
			}
		}
	})

	b.Run("iter_novalues", func(b *testing.B) {
		for b.Loop() {
			iter, _ := newMsgpPayloadMapIter(encoded)

			fieldCount := 0
			for {
				_, _, err := iter.nextKey()
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

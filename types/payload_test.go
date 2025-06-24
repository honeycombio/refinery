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

		ph.Set("key5", "newvalue")
		assert.True(t, ph.Exists("key5"))
		assert.Equal(t, "newvalue", ph.Get("key5"))

		// Overwrite an existing value
		ph.Set("key3", 4.13)
		assert.True(t, ph.Exists("key3"))
		assert.Equal(t, 4.13, ph.Get("key3"))

		expected := maps.Clone(data)
		expected["key3"] = 4.13
		expected["key5"] = "newvalue"
		found := maps.Collect(ph.All())
		assert.Equal(t, expected, found)

		asJSON, err := json.Marshal(ph)
		require.NoError(t, err)

		var fromJSON Payload
		err = json.Unmarshal(asJSON, &fromJSON)
		require.NoError(t, err)

		// round-tripping through JSON turns our ints into floats
		expectedFromJSON := maps.Collect(ph.All())
		expectedFromJSON["key2"] = 42.0
		assert.EqualValues(t, expectedFromJSON, maps.Collect(fromJSON.All()))
	}

	ph = NewPayload(data)
	t.Run("from_map", doTest)

	ph = Payload{}
	msgpData, err := msgpack.Marshal(data)
	require.NoError(t, err)
	err = msgpack.Unmarshal(msgpData, &ph)
	require.NoError(t, err)
	t.Run("from_msgpack", doTest)

	// Test payload with other stuff (another payload) following.
	ph = Payload{}
	extendedMsgpData := append(msgpData, msgpData...)
	remainder, err := ph.UnmarshalMsg(extendedMsgpData)
	require.NoError(t, err)
	assert.Equal(t, msgpData, remainder)
	t.Run("from_msgp", doTest)

	// Test our own marshaler
	msgpData, err = ph.MarshalMsg(nil)
	require.NoError(t, err)
	ph = Payload{}
	remainder, err = ph.UnmarshalMsg(msgpData)
	require.NoError(t, err)
	assert.Empty(t, remainder)
	t.Run("from_marshal", doTest)
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
	var phMsgp Payload
	err = msgpack.Unmarshal(msgpData, &phMsgp)
	require.NoError(b, err)

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

package types

import (
	"fmt"
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestPayloadHolder(t *testing.T) {
	data := map[string]any{
		"key1": "value1",
		"key2": int64(42),
		"key3": 3.14,
		"key4": true,
	}

	var ph PayloadHolder
	doTest := func(t *testing.T) {
		assert.Equal(t, "value1", ph.Get("key1"))
		assert.Equal(t, int64(42), ph.Get("key2"))
		assert.Nil(t, ph.Get("nonexistent"))

		ph.Set("key5", "newvalue")
		assert.Equal(t, "newvalue", ph.Get("key5"))

		found := make(map[string]any)
		for k, v := range ph.All() {
			found[k] = v
		}

		expected := maps.Clone(data)
		expected["key5"] = "newvalue"
		assert.Equal(t, expected, found)
	}

	ph = NewPayloadHolderFromMap(data)
	t.Run("from_map", doTest)

	msgpData, err := msgpack.Marshal(data)
	require.NoError(t, err)
	ph = NewPayloadHolderFromMsgP(msgpData)
	t.Run("from_msgp", doTest)
}

func BenchmarkPayloadHolder(b *testing.B) {
	// Create test data with many fields
	data := make(map[string]any)
	for i := 0; i < 100; i++ {
		data[fmt.Sprintf("key%d", i)] = fmt.Sprintf("value%d", i)
		data[fmt.Sprintf("num%d", i)] = int64(i)
		data[fmt.Sprintf("float%d", i)] = float64(i) * 1.5
		data[fmt.Sprintf("bool%d", i)] = i%2 == 0
	}

	// Create msgp data using msgpack.Marshal
	msgpData, err := msgpack.Marshal(data)
	require.NoError(b, err)

	phMap := NewPayloadHolderFromMap(data)
	phMsgp := NewPayloadHolderFromMsgP(msgpData)

	b.Run("single_map", func(b *testing.B) {
		b.ResetTimer()
		for b.Loop() {
			_ = phMap.Get("key50")
		}
	})

	b.Run("single_msgp", func(b *testing.B) {
		b.ResetTimer()
		for b.Loop() {
			_ = phMsgp.Get("key50")
		}
	})

	b.Run("single_memo", func(b *testing.B) {
		phMsgpMemo := NewPayloadHolderFromMsgP(msgpData)
		phMsgpMemo.MemoizeFields("key50")
		b.ResetTimer()
		for b.Loop() {
			_ = phMsgpMemo.Get("key50")
		}
	})

	b.Run("multi_map", func(b *testing.B) {
		keys := []string{"key10", "key20", "key30", "key40", "key50"}
		b.ResetTimer()
		for b.Loop() {
			for _, key := range keys {
				_ = phMap.Get(key)
			}
		}
	})

	b.Run("multi_msgp", func(b *testing.B) {
		keys := []string{"key10", "key20", "key30", "key40", "key50"}
		b.ResetTimer()
		for b.Loop() {
			for _, key := range keys {
				_ = phMsgp.Get(key)
			}
		}
	})

	b.Run("multi_memo", func(b *testing.B) {
		keys := []string{"key10", "key20", "key30", "key40", "key50"}
		phMsgpMemo := NewPayloadHolderFromMsgP(msgpData)
		phMsgpMemo.MemoizeFields(keys...)
		b.ResetTimer()
		for b.Loop() {
			for _, key := range keys {
				_ = phMsgpMemo.Get(key)
			}
		}
	})

	b.Run("iter_map", func(b *testing.B) {
		b.ResetTimer()
		for b.Loop() {
			for k, v := range phMap.All() {
				_ = k
				_ = v
			}
		}
	})

	b.Run("iter_msgp", func(b *testing.B) {
		b.ResetTimer()
		for b.Loop() {
			for k, v := range phMsgp.All() {
				_ = k
				_ = v
			}
		}
	})
}

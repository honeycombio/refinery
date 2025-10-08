package types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tinylib/msgp/msgp"
)

func TestJSONToMessagePack(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected map[string]any
	}{
		{
			name:  "mixed types",
			input: `{"str": "hello", "num": 123, "flag": true, "empty": null}`,
			expected: map[string]any{
				"str":   "hello",
				"num":   float64(123),
				"flag":  true,
				"empty": nil,
			},
		},
		{
			name:  "nested object",
			input: `{"nested": {"inner": "value", "count": 5}}`,
			expected: map[string]any{
				"nested": map[string]any{
					"inner": "value",
					"count": float64(5),
				},
			},
		},
		{
			name:  "array of strings",
			input: `{"items": ["a", "b", "c"]}`,
			expected: map[string]any{
				"items": []any{"a", "b", "c"},
			},
		},
		{
			name:  "array of numbers",
			input: `{"numbers": [1, 2, 3]}`,
			expected: map[string]any{
				"numbers": []any{float64(1), float64(2), float64(3)},
			},
		},
		{
			name:  "array of mixed types",
			input: `{"mixed": ["string", 42, true, null]}`,
			expected: map[string]any{
				"mixed": []any{"string", float64(42), true, nil},
			},
		},
		{
			name:     "empty object",
			input:    `{}`,
			expected: map[string]any{},
		},
		{
			name:  "empty array",
			input: `{"empty": []}`,
			expected: map[string]any{
				"empty": []any{},
			},
		},
		{
			name:  "complex nested structure",
			input: `{"data": {"users": [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}], "count": 2}}`,
			expected: map[string]any{
				"data": map[string]any{
					"users": []any{
						map[string]any{"name": "Alice", "age": float64(30)},
						map[string]any{"name": "Bob", "age": float64(25)},
					},
					"count": float64(2),
				},
			},
		},
		{
			name:  "unicode characters",
			input: `{"message": "Hello 🌍 World! αβγδε"}`,
			expected: map[string]any{
				"message": "Hello 🌍 World! αβγδε",
			},
		},
		{
			name:  "escaped characters",
			input: `{"message": "Line 1\nLine 2\tTabbed"}`,
			expected: map[string]any{
				"message": "Line 1\nLine 2\tTabbed",
			},
		},
		{
			name:  "quotes in string",
			input: `{"message": "She said \"Hello\""}`,
			expected: map[string]any{
				"message": "She said \"Hello\"",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test conversion
			msgpackData, err := JSONToMessagePack(nil, []byte(tt.input))
			assert.NoError(t, err)

			// Test unmarshaling
			var result any
			result, _, err = msgp.ReadIntfBytes(msgpackData)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)

			// Note: Our converter now matches json.Unmarshal behavior by converting all numbers to float64

			// Test idempotency - convert same input again
			msgpackData2, err := JSONToMessagePack(nil, []byte(tt.input))
			assert.NoError(t, err)
			assert.Equal(t, msgpackData, msgpackData2, "Conversion should be idempotent")
		})
	}
}

func TestJSONToMessagePackErrorCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "invalid JSON",
			input: `{"key": }`,
		},
		{
			name:  "empty input",
			input: ``,
		},
		{
			name:  "malformed JSON",
			input: `{"key": "value"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := JSONToMessagePack(nil, []byte(tt.input))
			assert.Error(t, err)
		})
	}
}

func BenchmarkJSONToMessagePack(b *testing.B) {
	testData := `{"trace_id": "abc123", "span_id": "xyz789", "timestamp": 1641024000, "duration": 150.5, "service": "web-api", "operation": "GET /users", "status": "ok", "tags": {"user_id": 12345, "region": "us-west-2", "version": "1.2.3"}, "events": [{"timestamp": 1641024001, "name": "cache_miss"}, {"time": "2023-01-01T12:00:00.123456789Z", "name": "db_query", "duration": 25.2}]}`

	b.Run("DirectConversion", func(b *testing.B) {
		var buf []byte
		for b.Loop() {
			buf, _ = JSONToMessagePack(buf[:0], []byte(testData))
		}
	})

	b.Run("StandardWay", func(b *testing.B) {
		var buf []byte
		for b.Loop() {
			var data map[string]any
			err := json.Unmarshal([]byte(testData), &data)
			if err != nil {
				b.Fatal(err)
			}
			buf, err = msgp.AppendIntf(buf[:0], data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

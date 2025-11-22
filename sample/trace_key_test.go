package sample

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyGeneration(t *testing.T) {
	tests := []struct {
		name           string
		fields         []string
		useTraceLength bool
		spans          []testSpan
		expectedKey    string
		expectedCount  int
	}{
		{
			name:           "all fields in single span",
			fields:         []string{"http.status_code", "request.path", "app.team.id", "important_field"},
			useTraceLength: true,
			spans: []testSpan{
				{
					data: map[string]interface{}{
						"http.status_code": 200,
						"request.path":     "/{slug}/home",
						"app.team.id":      float64(2),
						"important_field":  true,
					},
				},
			},
			expectedKey:   "2•,200•,true•,/{slug}/home•,1",
			expectedCount: 5,
		},
		{
			name:           "fields spread across spans",
			fields:         []string{"http.status_code", "request.path", "app.team.id", "important_field"},
			useTraceLength: true,
			spans: []testSpan{
				{data: map[string]interface{}{"http.status_code": 200}},
				{data: map[string]interface{}{"request.path": "/{slug}/home"}},
				{data: map[string]interface{}{"app.team.id": float64(2)}},
				{data: map[string]interface{}{"important_field": true}},
			},
			expectedKey:   "2•,200•,true•,/{slug}/home•,4",
			expectedCount: 5,
		},
		{
			name:           "multiple identical field values across spans",
			fields:         []string{"http.status_code"},
			useTraceLength: true,
			spans: []testSpan{
				{data: map[string]interface{}{"http.status_code": 200}},
				{data: map[string]interface{}{"http.status_code": 200}},
				{data: map[string]interface{}{"http.status_code": 404}},
				{data: map[string]interface{}{"http.status_code": 404}},
			},
			expectedKey:   "200•404•,4",
			expectedCount: 3,
		},
		{
			name:           "root prefixed fields",
			fields:         []string{"http.status_code", "root.service_name", "root.another_field"},
			useTraceLength: true,
			spans: []testSpan{
				{data: map[string]interface{}{"http.status_code": 404}},
				{data: map[string]interface{}{
					"http.status_code": 200,
					"service_name":     "another",
				}},

				{data: map[string]interface{}{
					"service_name": "test",
				}, isRoot: true},
			},
			expectedKey:   "200•404•,test,3",
			expectedCount: 4,
		},
		{
			name:   "no value for key fields found",
			fields: []string{"http.status_code", "request.path"},
			spans: []testSpan{
				{data: map[string]interface{}{"app.team.id": 2}},
				{data: map[string]interface{}{"important_field": true}, isRoot: true},
			},
			useTraceLength: true,
			expectedKey:    "2",
			expectedCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &config.MockConfig{}
			generator := newTraceKey(tt.fields, tt.useTraceLength)
			trace := createTestTrace(t, tt.spans, config)

			key, n := generator.build(trace)

			assert.Equal(t, tt.expectedKey, key, "key should match expected value")
			assert.Equal(t, tt.expectedCount, n, "count should match expected value")
		})
	}
}

func TestKeyLimits(t *testing.T) {
	fields := []string{"fieldA", "fieldB"}
	useTraceLength := true

	generator := newTraceKey(fields, useTraceLength)

	trace := &types.Trace{}

	config := &config.MockConfig{}

	// generate too many spans with different unique values
	for i := 0; i < 160; i++ {
		trace.AddSpan(&types.Span{
			Event: &types.Event{
				Data: types.NewPayload(config, map[string]interface{}{
					"fieldA": fmt.Sprintf("value%d", i),
					"fieldB": i,
				}),
			},
		})
	}

	trace.RootSpan = &types.Span{
		Event: &types.Event{
			Data: types.NewPayload(config, map[string]interface{}{
				"service_name": "test",
			}),
		},
	}

	_, n := generator.build(trace)
	// we should have maxKeyLength unique values
	assert.Equal(t, maxKeyLength, n)
}

func TestDistinctValue_AddAsString(t *testing.T) {
	tests := []struct {
		name string
		// Values to add in the format {fieldName, value}. Using an array here instead of a map because we need deterministic order for testing
		valuesToAdd [][]any
		// Expected unique counts for each field
		expectedCounts []int
		// Expected distinct values for each field
		expectedValues [][]string
	}{
		{
			name: "integer_and_string",
			valuesToAdd: [][]any{
				{"field1", 1},
				{"field1", "1"},
				{"field1", 1.0},
				{"field1", "café ñoño 🚀"},
			},
			expectedCounts: []int{2},
			expectedValues: [][]string{{"1", "café ñoño 🚀"}},
		},
		{
			name: "boolean_true",
			valuesToAdd: [][]any{
				{"field1", true},
				{"field1", "true"},
				{"field1", 1},
			},
			expectedCounts: []int{2},
			expectedValues: [][]string{{"true", "1"}},
		},
		{
			name: "boolean_false",
			valuesToAdd: [][]any{
				{"field1", false},
				{"field1", "false"},
				{"field1", 0},
			},
			expectedCounts: []int{2},
			expectedValues: [][]string{{"false", "0"}},
		},
		{
			name: "floating_point",
			valuesToAdd: [][]any{
				{"field1", 3.14},
				{"field1", "3.14"},
				{"field1", 3},
			},
			expectedCounts: []int{2},
			expectedValues: [][]string{{"3.14", "3"}},
		},
		{
			name: "zero_values",
			valuesToAdd: [][]any{
				{"field1", 0},
				{"field1", "0"},
				{"field1", 0.0},
				{"field1", nil},
			},
			expectedCounts: []int{2},
			expectedValues: [][]string{{"0", "<nil>"}},
		},
		{
			name: "negative_numbers",
			valuesToAdd: [][]any{
				{"field1", -42},
				{"field1", "-42"},
				{"field1", -42.0},
			},
			expectedCounts: []int{1},
			expectedValues: [][]string{{"-42"}},
		},
		{
			name: "nested_map",
			valuesToAdd: [][]any{
				{"field1", map[string]interface{}{"inner": "value"}},
				{"field1", map[string]interface{}{"inner": "value"}},
				{"field1", `{"inner":"value"}`}, // JSON string representation
			},
			expectedCounts: []int{2},
			expectedValues: [][]string{{"map[inner:value]", `{"inner":"value"}`}},
		},
		{
			name: "array_field",
			valuesToAdd: [][]any{
				{"field1", []any{1, 2, 3}},
				{"field1", []any{1, 2, 3}},
				{"field1", []any{4, 5, 6}}, // Different array
			},
			expectedCounts: []int{2}, // Two distinct arrays
			expectedValues: [][]string{{"[1 2 3]", "[4 5 6]"}},
		},
		{
			name: "multiple_fields",
			valuesToAdd: [][]any{
				{"field1", "value1"},
				{"field2", "value2"},
				{"field1", "value1"},
				{"field2", "value2"},
				{"field1", "value1"},
				{"field2", "value3"},
				{"field1", "value4"},
				{"field2", "value2"},
			},
			expectedCounts: []int{2, 2},
			expectedValues: [][]string{
				{"value1", "value4"},
				{"value2", "value3"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dv := &distinctValue{
				buf:    make([]byte, 0, 1024),
				values: []map[uint64]string{make(map[uint64]string)},
			}
			fieldList := []string{"field1", "field2"}
			dv.Reset(fieldList, 100)

			// Add values to the distinctValue instance
			for _, value := range tt.valuesToAdd {
				var fieldIdx int
				for i, v := range value {
					if i == 0 {
						fieldIdx = slices.Index(fieldList, value[0].(string))
						continue
					}
					dv.AddAsString(v, fieldIdx)
				}
			}

			// each field should have expected unique counts and values
			expectedTotalCount := 0
			for fieldIdx, expectedCount := range tt.expectedCounts {
				assert.Equal(t, expectedCount, len(dv.values[fieldIdx]), "Unexpected count for field index %d", fieldIdx)
				expectedTotalCount += expectedCount
			}

			assert.Equal(t, expectedTotalCount, dv.totalUniqueCount, "Unexpected unique count")

			// run the dinstinct values through fmt.Sprintf("%v") to ensure it matches the expected string representation
			// of the values
			for fieldIdx, expectedValues := range tt.expectedValues {
				values := dv.Values(fieldIdx)
				assert.ElementsMatch(t, expectedValues, values, "Unexpected distinct values")
				var expectedOutputs []string
				for _, v := range dv.values[fieldIdx] {
					expectedOutputs = append(expectedOutputs, fmt.Sprintf("%v", v))
				}
				assert.ElementsMatch(t, expectedOutputs, values, "field value doesn't match with fmt.Sprintf(\"%v\", fieldIdx)")
			}
		})
	}
}

func TestDistinctValue_MaxLimit(t *testing.T) {
	maxValues := 3
	dv := &distinctValue{
		buf: make([]byte, 0, 1024),
	}
	dv.Reset([]string{"field1"}, maxValues)

	// Add up to the limit
	for i := 0; i < maxValues-1; i++ {
		require.True(t, dv.AddAsString(i, 0), "Expected success adding value %d, got failure", i)
	}

	// This should fail as we've reached the limit
	require.False(t, dv.AddAsString("one-too-many", 0), "Expected failure when adding beyond max limit, got success")

	require.Equal(t, maxValues, dv.totalUniqueCount)
}

func TestDistinctValue_Values(t *testing.T) {
	dv := &distinctValue{
		buf: make([]byte, 0, 1024),
	}
	dv.Reset([]string{"field1", "field2", "empty-field"}, 10)

	// Add some mixed values
	dv.AddAsString("banana", 0)
	dv.AddAsString("apple", 0)
	dv.AddAsString("cherry", 0)
	dv.AddAsString(42, 1)
	dv.AddAsString(true, 1)

	t.Run("Values sorted", func(t *testing.T) {
		values := dv.Values(0)
		expected := []string{"apple", "banana", "cherry"}
		require.EqualValues(t, expected, values)
	})

	t.Run("Type conversion", func(t *testing.T) {
		values := dv.Values(1)
		require.Len(t, values, 2)
		require.Equal(t, []string{"42", "true"}, values)
	})

	t.Run("Invalid index", func(t *testing.T) {
		// Test with negative index
		values := dv.Values(-1)
		require.Nil(t, values)

		// Test with out of bounds index
		values = dv.Values(5)
		require.Nil(t, values)
	})

	t.Run("Empty field", func(t *testing.T) {
		// Field that has no values
		values := dv.Values(2)
		require.Nil(t, values)
	})
}

func BenchmarkTraceKeyBuild(b *testing.B) {
	scenarios := []struct {
		name           string
		fields         []string
		useTraceLength bool
		spanCount      int
		uniqueValues   int
		rootFields     bool
	}{
		{
			name:           "small_trace_fields",
			fields:         []string{"http.status_code", "request.path"},
			useTraceLength: true,
			spanCount:      100,
			uniqueValues:   10,
			rootFields:     false,
		},
		{
			name:           "large_trace_fields",
			fields:         []string{"http.status_code", "request.path"},
			useTraceLength: true,
			spanCount:      1_000_000,
			uniqueValues:   10,
			rootFields:     false,
		},
		{
			name:           "small_trace_many_fields",
			fields:         []string{"http.status_code", "request.path", "service.name", "http.method", "user.id", "app.team"},
			useTraceLength: true,
			spanCount:      100,
			uniqueValues:   10,
			rootFields:     false,
		},
		{
			name:           "with_root_fields",
			fields:         []string{"http.status_code", "request.path", "root.service_name", "root.team_id"},
			useTraceLength: true,
			spanCount:      100,
			uniqueValues:   10,
			rootFields:     true,
		},
		{
			name:           "high_cardinality",
			fields:         []string{"user.id", "session.id", "trace.id"},
			useTraceLength: true,
			spanCount:      500,
			uniqueValues:   200, // Will hit maxKeyLength
			rootFields:     false,
		},
		{
			name:           "empty_fields",
			fields:         []string{"empty-field", "http.status_code", "request.path"},
			useTraceLength: true,
			spanCount:      100,
			uniqueValues:   10,
			rootFields:     false,
		},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			generator := newTraceKey(scenario.fields, scenario.useTraceLength)

			trace := &types.Trace{}
			config := &config.MockConfig{}

			for i := 0; i < scenario.spanCount; i++ {
				spanData := make(map[string]interface{})

				for _, field := range scenario.fields {
					if !strings.HasPrefix(field, "root.") {
						// Use modulo to create repeating values
						valueIdx := i % scenario.uniqueValues
						switch field {
						case "http.status_code":
							codes := []int{200, 201, 400, 404, 500}
							spanData[field] = codes[valueIdx%len(codes)]
						case "request.path":
							spanData[field] = fmt.Sprintf("/api/v1/resource/%d", valueIdx)
						case "service.name":
							services := []string{"auth-service", "user-service", "billing-service", "api-gateway", "data-processor"}
							spanData[field] = services[valueIdx%len(services)]
						case "http.method":
							methods := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
							spanData[field] = methods[valueIdx%len(methods)]
						case "user.id", "session.id", "trace.id":
							// High cardinality fields
							spanData[field] = fmt.Sprintf("id-%d", valueIdx)
						case "empty-field":
							// Intentionally leave this field empty to simulate missing data
							spanData[field] = nil
						default:
							spanData[field] = fmt.Sprintf("value-%d", valueIdx)
						}
					}
				}

				span := &types.Span{
					Event: &types.Event{
						Data: types.NewPayload(config, spanData),
					},
				}

				// Set as root span if it's the first one and we need root fields
				if i == 0 && scenario.rootFields {
					span.IsRoot = true
					trace.RootSpan = span

					// Add root-specific fields
					for _, field := range scenario.fields {
						if strings.HasPrefix(field, "root.") {
							actualField := field[len("root."):]
							span.Data.Set(actualField, fmt.Sprintf("root-%s-value", actualField))
						}
					}
				}

				trace.AddSpan(span)
			}

			b.ResetTimer()

			for b.Loop() {
				generator.build(trace)
			}
		})
	}
}

type testSpan struct {
	data   map[string]interface{}
	isRoot bool
}

// Helper function to create test traces with specified data
func createTestTrace(t *testing.T, spans []testSpan, config *config.MockConfig) *types.Trace {
	trace := &types.Trace{}

	for _, s := range spans {
		span := &types.Span{
			Event: &types.Event{
				Data: types.NewPayload(config, s.data),
			},
		}
		if s.isRoot {
			trace.RootSpan = span
		}
		trace.AddSpan(span)
	}

	return trace
}

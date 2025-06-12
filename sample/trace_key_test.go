package sample

import (
	"fmt"
	"strings"
	"testing"

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
			generator := newTraceKey(tt.fields, tt.useTraceLength)
			trace := createTestTrace(t, tt.spans)

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

	// generate too many spans with different unique values
	for i := 0; i < 160; i++ {
		trace.AddSpan(&types.Span{
			Event: types.Event{
				Data: map[string]interface{}{
					"fieldA": fmt.Sprintf("value%d", i),
					"fieldB": i,
				},
			},
		})
	}

	trace.RootSpan = &types.Span{
		Event: types.Event{
			Data: map[string]interface{}{
				"service_name": "test",
			},
		},
	}

	_, n := generator.build(trace)
	// we should have maxKeyLength unique values
	assert.Equal(t, maxKeyLength, n)
}

type testSpan struct {
	data   map[string]interface{}
	isRoot bool
}

// Helper function to create test traces with specified data
func createTestTrace(t *testing.T, spans []testSpan) *types.Trace {
	trace := &types.Trace{}

	for _, s := range spans {
		span := &types.Span{
			Event: types.Event{
				Data: s.data,
			},
		}
		if s.isRoot {
			trace.RootSpan = span
		}
		trace.AddSpan(span)
	}

	return trace
}

func TestDistinctValue_AddAsString(t *testing.T) {
	fields := []string{"field1", "field2"}
	dv := &distinctValue{
		buf: make([]byte, 0, 1024),
	}
	dv.init(fields, 10)

	tests := []struct {
		name        string
		value       any
		fieldIdx    int
		wantSuccess bool
		wantCount   int
	}{
		{
			name:        "Add string",
			value:       "test-value",
			fieldIdx:    0,
			wantSuccess: true,
			wantCount:   1,
		},
		{
			name:        "Add integer",
			value:       42,
			fieldIdx:    0,
			wantSuccess: true,
			wantCount:   2,
		},
		{
			name:        "Add float",
			value:       3.14,
			fieldIdx:    0,
			wantSuccess: true,
			wantCount:   3,
		},
		{
			name:        "Add boolean",
			value:       true,
			fieldIdx:    0,
			wantSuccess: true,
			wantCount:   4,
		},
		{
			name:        "Add to second field",
			value:       "second-field",
			fieldIdx:    1,
			wantSuccess: true,
			wantCount:   5,
		},
		{
			name:        "Add duplicate string",
			value:       "test-value",
			fieldIdx:    0,
			wantSuccess: false, // Already exists
			wantCount:   5,     // Unchanged
		},
		{
			name:        "Add nil",
			value:       nil,
			fieldIdx:    0,
			wantSuccess: false,
			wantCount:   5, // Unchanged
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dv.AddAsString(tt.value, tt.fieldIdx)
			require.Equal(t, tt.wantSuccess, got)
			require.Equal(t, tt.wantCount, dv.totalUniqueCount)
		})
	}
}

func TestDistinctValue_MaxLimit(t *testing.T) {
	maxValues := 3
	dv := &distinctValue{
		buf: make([]byte, 0, 1024),
	}
	dv.init([]string{"field1"}, maxValues)

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
	dv.init([]string{"field1", "field2", "empty-field"}, 10)

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
		require.Contains(t, values, "42")
		require.Contains(t, values, "true")
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
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			generator := newTraceKey(scenario.fields, scenario.useTraceLength)

			trace := &types.Trace{}

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
						default:
							spanData[field] = fmt.Sprintf("value-%d", valueIdx)
						}
					}
				}

				span := &types.Span{
					Event: types.Event{
						Data: spanData,
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
							span.Data[actualField] = fmt.Sprintf("root-%s-value", actualField)
						}
					}
				}

				trace.AddSpan(span)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				key, _ := generator.build(trace)
				// Force evaluation to prevent the compiler from optimizing away the call
				if len(key) == 0 {
					b.Fatal("Empty key returned")
				}
			}
		})
	}
}

func TestDistinctValue_ValueTypeConsistency(t *testing.T) {
	tests := []struct {
		name           string
		equalValues    []interface{} // These should all be considered identical
		differentValue interface{}   // This should be considered a new unique value
		expectedStr    string        // The expected string representation in Values()
	}{
		{
			name:           "integer_and_string_2",
			equalValues:    []interface{}{2, "2", 2.0},
			differentValue: "2a",
			expectedStr:    "2",
		},
		{
			name:           "boolean_true",
			equalValues:    []interface{}{true, "true"},
			differentValue: 1,
			expectedStr:    "true",
		},
		{
			name:           "boolean_false",
			equalValues:    []interface{}{false, "false"},
			differentValue: 0,
			expectedStr:    "false",
		},
		{
			name:           "floating_point",
			equalValues:    []interface{}{3.14, "3.14"},
			differentValue: 3,
			expectedStr:    "3.14",
		},
		{
			name:           "zero_values",
			equalValues:    []interface{}{0, "0", 0.0},
			differentValue: false,
			expectedStr:    "0",
		},
		{
			name:           "negative_numbers",
			equalValues:    []interface{}{-42, "-42", -42.0},
			differentValue: "-42.0",
			expectedStr:    "-42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dv := &distinctValue{
				buf:              make([]byte, 0, 1024),
				fields:           []string{"testField"},
				values:           []map[uint64]string{make(map[uint64]string)},
				maxDistinctValue: 100,
			}

			firstAdded := dv.AddAsString(tt.equalValues[0], 0)
			require.True(t, firstAdded, "First value should be added successfully")
			require.Equal(t, 1, dv.totalUniqueCount)

			// All equal values should be considered duplicates
			for i := 1; i < len(tt.equalValues); i++ {
				added := dv.AddAsString(tt.equalValues[i], 0)
				require.False(t, added,
					"Value %v should be considered a duplicate of %v",
					tt.equalValues[i], tt.equalValues[0])
				require.Equal(t, 1, dv.totalUniqueCount,
					"Count should remain 1 after adding duplicate %v", tt.equalValues[i])
			}

			differentAdded := dv.AddAsString(tt.differentValue, 0)
			require.True(t, differentAdded,
				"Value %v should NOT be considered a duplicate", tt.differentValue)
			require.Equal(t, 2, dv.totalUniqueCount,
				"Count should be 2 after adding different value %v", tt.differentValue)

			values := dv.Values(0)
			require.Len(t, values, 2)
			require.Contains(t, values, tt.expectedStr,
				"Values should contain expected string representation %q", tt.expectedStr)
		})
	}
}

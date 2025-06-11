package sample

import (
	"fmt"
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
	dv := newDistinctValue(fields, 10)

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
	dv := newDistinctValue([]string{"field"}, maxValues)

	// Add up to the limit
	for i := 0; i < maxValues-1; i++ {
		require.True(t, dv.AddAsString(i, 0), "Expected success adding value %d, got failure", i)
	}

	// This should fail as we've reached the limit
	require.False(t, dv.AddAsString("one-too-many", 0), "Expected failure when adding beyond max limit, got success")

	require.Equal(t, maxValues, dv.totalUniqueCount)
}

func TestDistinctValue_Values(t *testing.T) {
	dv := newDistinctValue([]string{"field1", "field2"}, 10)

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
		dv := newDistinctValue([]string{"empty"}, 10)
		values := dv.Values(0)
		require.Nil(t, values)
	})
}

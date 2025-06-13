package sample

import (
	"fmt"
	"testing"

	"github.com/honeycombio/refinery/types"
	"github.com/stretchr/testify/assert"
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
				Data: types.NewPayload(map[string]interface{}{
					"fieldA": fmt.Sprintf("value%d", i),
					"fieldB": i,
				}),
			},
		})
	}

	trace.RootSpan = &types.Span{
		Event: types.Event{
			Data: types.NewPayload(map[string]interface{}{
				"service_name": "test",
			}),
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
				Data: types.NewPayload(s.data),
			},
		}
		if s.isRoot {
			trace.RootSpan = span
		}
		trace.AddSpan(span)
	}

	return trace
}

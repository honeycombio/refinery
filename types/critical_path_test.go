package types

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSpanID(t *testing.T) {
	testCases := []struct {
		name  string
		span  *Span
		idSet bool
	}{
		{
			name: "explicit span_id",
			span: &Span{
				Event: Event{
					Data: map[string]interface{}{
						"span_id": "1234567890",
					},
				},
			},
			idSet: true,
		},
		{
			name: "empty span_id",
			span: &Span{
				Event: Event{
					Data: map[string]interface{}{
						"span_id": "",
					},
				},
			},
			idSet: false,
		},
		{
			name: "non-string span_id",
			span: &Span{
				Event: Event{
					Data: map[string]interface{}{
						"span_id": 12345,
					},
				},
			},
			idSet: false,
		},
		{
			name: "no span_id",
			span: &Span{
				Event: Event{
					Data: map[string]interface{}{
						"other": "value",
					},
				},
			},
			idSet: false,
		},
		{
			name: "nil data",
			span: &Span{
				Event: Event{
					Data: nil,
				},
			},
			idSet: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			id := tc.span.SpanID()
			if tc.idSet {
				assert.Equal(t, tc.span.Data["span_id"], id)
			} else {
				assert.NotEmpty(t, id, "Should generate a fallback ID")
			}
		})
	}
}

func TestCalculateCriticalPath(t *testing.T) {
	// Test different trace structures to verify critical path detection
	testCases := []struct {
		name                    string
		setupTrace              func() *Trace
		expectedCriticalCount   int
		expectedCriticalSpanIDs []string
	}{
		{
			name: "empty trace",
			setupTrace: func() *Trace {
				return &Trace{}
			},
			expectedCriticalCount:   0,
			expectedCriticalSpanIDs: []string{},
		},
		{
			name: "root only",
			setupTrace: func() *Trace {
				trace := &Trace{}
				root := &Span{
					TraceID: "trace1",
					IsRoot:  true,
					Event: Event{
						Data: map[string]interface{}{
							"span_id": "root",
						},
					},
				}
				trace.RootSpan = root
				trace.AddSpan(root)
				return trace
			},
			expectedCriticalCount:   1,
			expectedCriticalSpanIDs: []string{"root"},
		},
		{
			name: "simple parent-child",
			setupTrace: func() *Trace {
				trace := &Trace{}
				
				// Create root span
				root := &Span{
					TraceID: "trace1",
					IsRoot:  true,
					Event: Event{
						Data: map[string]interface{}{
							"span_id": "root",
						},
					},
				}
				trace.RootSpan = root
				trace.AddSpan(root)
				
				// Create child span with 10ms duration
				child1 := &Span{
					TraceID:  "trace1",
					ParentID: "root",
					Event: Event{
						Data: map[string]interface{}{
							"span_id":      "child1",
							"duration_ms":  10.0,
						},
					},
				}
				trace.AddSpan(child1)
				
				// Create child span with 20ms duration
				child2 := &Span{
					TraceID:  "trace1",
					ParentID: "root",
					Event: Event{
						Data: map[string]interface{}{
							"span_id":      "child2",
							"duration_ms":  20.0,
						},
					},
				}
				trace.AddSpan(child2)
				
				return trace
			},
			expectedCriticalCount:   2,
			expectedCriticalSpanIDs: []string{"root", "child2"},
		},
		{
			name: "complex multi-level",
			setupTrace: func() *Trace {
				trace := &Trace{}
				
				// Create root span
				root := &Span{
					TraceID: "trace1",
					IsRoot:  true,
					Event: Event{
						Data: map[string]interface{}{
							"span_id": "root",
						},
					},
				}
				trace.RootSpan = root
				trace.AddSpan(root)
				
				// Level 1 children
				child1 := &Span{
					TraceID:  "trace1",
					ParentID: "root",
					Event: Event{
						Data: map[string]interface{}{
							"span_id":      "child1",
							"duration_ms":  30.0,
						},
					},
				}
				trace.AddSpan(child1)
				
				child2 := &Span{
					TraceID:  "trace1",
					ParentID: "root",
					Event: Event{
						Data: map[string]interface{}{
							"span_id":      "child2",
							"duration_ms":  20.0,
						},
					},
				}
				trace.AddSpan(child2)
				
				// Level 2 grandchildren
				grandchild1 := &Span{
					TraceID:  "trace1",
					ParentID: "child1",
					Event: Event{
						Data: map[string]interface{}{
							"span_id":      "grandchild1",
							"duration_ms":  5.0,
						},
					},
				}
				trace.AddSpan(grandchild1)
				
				grandchild2 := &Span{
					TraceID:  "trace1",
					ParentID: "child1",
					Event: Event{
						Data: map[string]interface{}{
							"span_id":      "grandchild2", 
							"duration_ms":  25.0,
						},
					},
				}
				trace.AddSpan(grandchild2)
				
				// Add decision span (should be ignored in critical path calculation)
				decisionSpan := &Span{
					TraceID:  "trace1",
					ParentID: "root",
					Event: Event{
						Data: map[string]interface{}{
							"span_id":                "decision1",
							"meta.refinery.min_span": true,
							"duration_ms":            100.0, // even though this is longest duration, it should be ignored
						},
					},
				}
				trace.AddSpan(decisionSpan)
				
				return trace
			},
			expectedCriticalCount:   3,
			expectedCriticalSpanIDs: []string{"root", "child1", "grandchild2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			trace := tc.setupTrace()
			
			// Calculate critical path
			result := trace.CalculateCriticalPath()
			
			// If we expect critical path spans, verify the calculation was successful
			if tc.expectedCriticalCount > 0 {
				assert.True(t, result, "Critical path calculation should succeed")
			}
			
			// Count spans on critical path
			criticalSpans := 0
			criticalSpanIDs := []string{}
			for _, span := range trace.GetSpans() {
				if span.IsOnCriticalPath {
					criticalSpans++
					if spanID, ok := span.Event.Data["span_id"].(string); ok {
						criticalSpanIDs = append(criticalSpanIDs, spanID)
					}
				}
			}
			
			assert.Equal(t, tc.expectedCriticalCount, criticalSpans, "Number of spans on critical path should match expected")
			
			// Verify expected span IDs are on critical path
			for _, expectedID := range tc.expectedCriticalSpanIDs {
				found := false
				for _, spanID := range criticalSpanIDs {
					if spanID == expectedID {
						found = true
						break
					}
				}
				assert.True(t, found, "Span ID %s should be on critical path", expectedID)
			}
		})
	}
}

func TestExtractDecisionContextWithCriticalPath(t *testing.T) {
	// Test that critical path info is included in the extracted decision context
	span := &Span{
		TraceID:          "trace1",
		IsOnCriticalPath: true,
		Event: Event{
			Data: map[string]interface{}{},
		},
	}
	
	decisionCtx := span.ExtractDecisionContext()
	require.NotNil(t, decisionCtx)
	
	critical, ok := decisionCtx.Data["meta.refinery.is_critical_path"]
	require.True(t, ok, "Critical path flag should be included in decision context")
	assert.True(t, critical.(bool), "Critical path flag should be true")
	
	// Test span not on critical path
	span.IsOnCriticalPath = false
	decisionCtx = span.ExtractDecisionContext()
	_, ok = decisionCtx.Data["meta.refinery.is_critical_path"]
	assert.False(t, ok, "Critical path flag should not be included for non-critical spans")
}

// Benchmark the SpanID function
func BenchmarkSpanID(b *testing.B) {
	span := &Span{
		Event: Event{
			Data: map[string]interface{}{
				"span_id": "test-span-id",
			},
		},
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		span.SpanID()
	}
}

// Benchmark critical path calculation for different trace sizes
func BenchmarkCalculateCriticalPath(b *testing.B) {
	benchmarks := []struct {
		name      string
		numSpans  int
		numLevels int
	}{
		{"Small_5spans", 5, 2},
		{"Medium_20spans", 20, 3},
		{"Large_100spans", 100, 4},
		{"XLarge_500spans", 500, 5},
	}
	
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			trace := buildTestTrace(bm.numSpans, bm.numLevels)
			
			b.ResetTimer()
			b.ReportAllocs()
			
			for i := 0; i < b.N; i++ {
				// Reset critical path markers before each run
				for _, span := range trace.spans {
					span.IsOnCriticalPath = false
				}
				trace.CalculateCriticalPath()
			}
		})
	}
}

// Helper function to build a test trace with the specified number of spans
func buildTestTrace(numSpans, levels int) *Trace {
	trace := &Trace{}
	
	// Create root span
	root := &Span{
		TraceID: "trace-bench",
		IsRoot:  true,
		Event: Event{
			Data: map[string]interface{}{
				"span_id": "root",
			},
		},
	}
	trace.RootSpan = root
	trace.spans = make([]*Span, 0, numSpans)
	trace.AddSpan(root)
	
	spansPerLevel := numSpans / levels
	if spansPerLevel < 1 {
		spansPerLevel = 1
	}
	
	// Build the trace with a tree structure
	buildLevel(trace, root, 1, levels, spansPerLevel, numSpans)
	
	return trace
}

// Recursively build trace levels
func buildLevel(trace *Trace, parent *Span, currentLevel, maxLevels, spansPerLevel, remainingSpans int) int {
	if currentLevel >= maxLevels || remainingSpans <= 1 {
		return remainingSpans
	}
	
	// Determine how many spans to create at this level
	spansToCreate := spansPerLevel
	if spansToCreate > remainingSpans-1 {
		spansToCreate = remainingSpans - 1
	}
	
	parentID := ""
	if id, ok := parent.Data["span_id"].(string); ok {
		parentID = id
	}
	
	// Create spans for this level
	for i := 0; i < spansToCreate; i++ {
		spanID := fmt.Sprintf("span-%d-%d", currentLevel, i)
		childSpan := &Span{
			TraceID:  "trace-bench",
			ParentID: parentID,
			Event: Event{
				Data: map[string]interface{}{
					"span_id":     spanID,
					"duration_ms": float64(10 + i),  // Vary durations
				},
			},
		}
		trace.AddSpan(childSpan)
		
		// For some spans, continue to the next level
		if i == 0 && currentLevel < maxLevels-1 {
			remainingSpans = buildLevel(trace, childSpan, currentLevel+1, maxLevels, spansPerLevel, remainingSpans-1)
		}
	}
	
	return remainingSpans - spansToCreate
}
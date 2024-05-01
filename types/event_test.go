package types

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSpan_GetDataSize(t *testing.T) {
	tests := []struct {
		name       string
		numInts    int
		numStrings int
		want       int
	}{
		{"all ints small", 10, 0, 80},
		{"all ints large", 100, 0, 800},
		{"all strings small", 0, 10, 45},
		{"all strings large", 0, 100, 4950},
		{"mixed small", 10, 10, 125},
		{"mixed large", 100, 100, 5750},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sp := &Span{
				TraceID: tt.name,
				Event: Event{
					Data: make(map[string]any),
				},
			}
			for i := 0; i < tt.numInts; i++ {
				sp.Data[tt.name+"int"+strconv.Itoa(i)] = i
			}
			for i := 0; i < tt.numStrings; i++ {
				sp.Data[tt.name+"str"+strconv.Itoa(i)] = strings.Repeat("x", i)
			}
			if got := sp.GetDataSize(); got != tt.want {
				t.Errorf("Span.CalculateSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSpan_AnnotationType(t *testing.T) {
	tests := []struct {
		name string
		data map[string]any
		want SpanAnnotationType
	}{
		{"unknown", map[string]any{}, SpanAnnotationTypeUnknown},
		{"span_event", map[string]any{"meta.annotation_type": "span_event"}, SpanAnnotationTypeSpanEvent},
		{"link", map[string]any{"meta.annotation_type": "link"}, SpanAnnotationTypeLink},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sp := &Span{
				Event: Event{
					Data: tt.data,
				},
			}
			if got := sp.AnnotationType(); got != tt.want {
				t.Errorf("Span.AnnotationType() = %v, want %v", got, tt.want)
			}
		})
	}
}

// These benchmarks were just to verify that the size calculation is acceptable
// even on big spans. The P99 for normal (20-field) spans shows that it will take ~1
// microsecond (on an m1 laptop) but a 1000-field span (extremely rare!) will take
// ~10 microseconds. Since these happen once per span, when adding it to a trace,
// we don't expect this to be a performance issue.
func BenchmarkSpan_CalculateSizeSmall(b *testing.B) {
	sp := &Span{
		Event: Event{
			Data: make(map[string]any),
		},
	}
	for i := 0; i < 10; i++ {
		sp.Data["int"+strconv.Itoa(i)] = i
	}
	for i := 0; i < 10; i++ {
		sp.Data["str"+strconv.Itoa(i)] = strings.Repeat("x", i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sp.GetDataSize()
	}
}

func BenchmarkSpan_CalculateSizeLarge(b *testing.B) {
	sp := &Span{
		Event: Event{
			Data: make(map[string]any),
		},
	}
	for i := 0; i < 500; i++ {
		sp.Data["int"+strconv.Itoa(i)] = i
	}
	for i := 0; i < 500; i++ {
		sp.Data["str"+strconv.Itoa(i)] = strings.Repeat("x", i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sp.GetDataSize()
	}
}

func TestCalculateRelativeSpanStartDurations(t *testing.T) {
	rootSpan := &Span{
		Event: Event{
			Timestamp: time.Now(),
			Data:      map[string]any{},
		},
		SpanID: "rootSpan",
	}

	trace := Trace{
		RootSpan: rootSpan,
		spans:    map[string]*Span{},
	}
	trace.spans["rootSpan"] = rootSpan

	childSpan1 := &Span{
		Event: Event{
			Timestamp: rootSpan.Timestamp.Add(10 * time.Millisecond),
			Data:      map[string]any{},
		},
		SpanID:   "childSpan1",
		ParentID: "rootSpan",
	}
	childSpan2 := &Span{
		Event: Event{
			Timestamp: rootSpan.Timestamp.Add(25 * time.Millisecond),
			Data:      map[string]any{},
		},
		SpanID:   "childSpan2",
		ParentID: "childSpan1",
	}

	trace.AddSpan(childSpan1)
	trace.AddSpan(childSpan2)

	trace.CalculateRelativeSpanStartDurations()

	assert.Equal(t, int64(10), childSpan1.Data["relative_start_time_ms"].(int64), "Relative start time for childSpan1 should be 10ms")
	assert.Equal(t, int64(10), childSpan1.Data["relative_start_time_parent_ms"].(int64), "Relative start time from parent for childSpan1 should be 10ms")

	assert.Equal(t, int64(25), childSpan2.Data["relative_start_time_ms"].(int64), "Relative start time for childSpan2 should be 20ms")
	assert.Equal(t, int64(15), childSpan2.Data["relative_start_time_parent_ms"].(int64), "Relative start time from parent for childSpan2 should be 10ms")
}

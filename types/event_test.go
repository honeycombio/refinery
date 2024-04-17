package types

import (
	"strconv"
	"strings"
	"testing"
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
		want SpanType
	}{
		{"unknown", map[string]any{}, SpanTypeNormal},
		{"span_event", map[string]any{"meta.annotation_type": "span_event"}, SpanTypeEvent},
		{"link", map[string]any{"meta.annotation_type": "link"}, SpanTypeLink},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sp := &Span{
				Event: Event{
					Data: tt.data,
				},
			}
			if got := sp.Type(); got != tt.want {
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

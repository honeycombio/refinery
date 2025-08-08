package types

import (
	"strconv"
	"strings"
	"testing"

	"github.com/honeycombio/refinery/config"
)

func TestSpan_GetDataSize(t *testing.T) {
	tests := []struct {
		name       string
		numInts    int
		numStrings int
		want       int
	}{
		{"all ints small", 10, 0, 260},
		{"all ints large", 100, 0, 2690},
		{"all strings small", 0, 10, 255},
		{"all strings large", 0, 100, 7140},
		{"mixed small", 10, 10, 425},
		{"mixed large", 100, 100, 8930},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make(map[string]any)
			for i := 0; i < tt.numInts; i++ {
				data[tt.name+"int"+strconv.Itoa(i)] = i
			}
			for i := 0; i < tt.numStrings; i++ {
				data[tt.name+"str"+strconv.Itoa(i)] = strings.Repeat("x", i)
			}
			payload := NewPayload(&config.MockConfig{}, data)
			sp := &Span{
				TraceID: tt.name,
				Event: Event{
					Data: payload,
				},
			}
			if got := sp.GetDataSize(); got != tt.want {
				t.Errorf("Span.CalculateSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSpan_GetDataSizeSlice(t *testing.T) {
	tests := []struct {
		name string
		num  int
		want int
	}{
		{"empty", 0, 4},
		{"small", 10, 84},
		{"large", 100, 804},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sliceData := make([]any, tt.num)
			for i := range tt.num {
				sliceData[i] = i
			}
			payload := NewPayload(&config.MockConfig{}, map[string]any{"data": sliceData})
			sp := &Span{
				Event: Event{
					Data: payload,
				},
			}
			if got := sp.GetDataSize(); got != tt.want {
				t.Errorf("Span.CalculateSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSpan_GetDataSizeMap(t *testing.T) {
	tests := []struct {
		name string
		num  int
		want int
	}{
		{"empty", 0, 4},
		{"small", 10, 94},
		{"large", 100, 994},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapData := make(map[string]any)
			for i := range tt.num {
				mapData[strconv.Itoa(i)] = i
			}
			payload := NewPayload(&config.MockConfig{}, map[string]any{"data": mapData})
			sp := &Span{
				Event: Event{
					Data: payload,
				},
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
			payload := NewPayload(&config.MockConfig{}, tt.data)
			payload.ExtractMetadata()
			sp := &Span{
				Event: Event{
					Data: payload,
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
	data := make(map[string]any)
	for i := 0; i < 10; i++ {
		data["int"+strconv.Itoa(i)] = i
	}
	for i := 0; i < 10; i++ {
		data["str"+strconv.Itoa(i)] = strings.Repeat("x", i)
	}
	payload := NewPayload(&config.MockConfig{}, data)
	sp := &Span{
		Event: Event{
			Data: payload,
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sp.GetDataSize()
	}
}

func BenchmarkSpan_CalculateSizeLarge(b *testing.B) {
	data := make(map[string]any)
	for i := 0; i < 500; i++ {
		data["int"+strconv.Itoa(i)] = i
	}
	for i := 0; i < 500; i++ {
		data["str"+strconv.Itoa(i)] = strings.Repeat("x", i)
	}
	payload := NewPayload(&config.MockConfig{}, data)
	sp := &Span{
		Event: Event{
			Data: payload,
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sp.GetDataSize()
	}
}

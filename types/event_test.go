package types

import (
	"maps"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/stretchr/testify/assert"
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

func TestSpan_ExtractDecisionContext(t *testing.T) {
	cfg := &config.MockConfig{
		TraceIdFieldNames: []string{"trace.trace_id"},
	}
	payload := NewPayload(cfg, map[string]any{
		"test":                 "test",
		"meta.annotation_type": "span_event",
	})
	payload.ExtractMetadata()
	ev := Event{
		APIHost:     "test.api.com",
		APIKey:      "test-api-key",
		Dataset:     "test-dataset",
		Environment: "test-environment",
		SampleRate:  5,
		Timestamp:   time.Now(),
		Data:        payload,
	}
	sp := &Span{
		Event:       ev,
		TraceID:     "test-trace-id",
		ArrivalTime: time.Now(),
		IsRoot:      true,
	}

	got := sp.ExtractDecisionContext(cfg)
	assert.Equal(t, ev.APIHost, got.APIHost)
	assert.Equal(t, ev.APIKey, got.APIKey)
	assert.Equal(t, ev.Dataset, got.Dataset)
	assert.Equal(t, ev.Environment, got.Environment)
	assert.Equal(t, ev.SampleRate, got.SampleRate)
	assert.Equal(t, ev.Timestamp, got.Timestamp)
	expectedData := map[string]any{
		"trace.trace_id":               sp.TraceID,
		"meta.refinery.root":           true,
		"meta.refinery.min_span":       true,
		"meta.annotation_type":         SpanAnnotationTypeSpanEvent.String(),
		"meta.refinery.span_data_size": int64(38),
	}
	actualData := maps.Collect(got.Data.All())
	assert.Equal(t, expectedData, actualData)
}

func TestSpan_IsDecisionSpan(t *testing.T) {
	tests := []struct {
		name string
		data map[string]any
		want bool
	}{
		{"nil meta", nil, false},
		{"no meta", map[string]any{}, false},
		{"no meta.refinery.min_span", map[string]any{"meta.annotation_type": "span_event"}, false},
		{"invalid min_span", map[string]any{"meta.annotation_type": "span_event", "meta.refinery.mi_span": true}, false},
		{"is decision span", map[string]any{"meta.annotation_type": "span_event", "meta.refinery.min_span": true}, true},
		{"is not decision span", map[string]any{"meta.annotation_type": "span_event", "meta.refinery.min_span": false}, false},
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
			got := sp.IsDecisionSpan()
			assert.Equal(t, tt.want, got)
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
	for i := range 10 {
		data["int"+strconv.Itoa(i)] = i
	}
	for i := range 10 {
		data["str"+strconv.Itoa(i)] = strings.Repeat("x", i)
	}
	payload := NewPayload(&config.MockConfig{}, data)
	sp := &Span{
		Event: Event{
			Data: payload,
		},
	}

	for b.Loop() {
		sp.GetDataSize()
	}
}

func BenchmarkSpan_CalculateSizeLarge(b *testing.B) {
	data := make(map[string]any)
	for i := range 500 {
		data["int"+strconv.Itoa(i)] = i
	}
	for i := range 500 {
		data["str"+strconv.Itoa(i)] = strings.Repeat("x", i)
	}
	payload := NewPayload(&config.MockConfig{}, data)
	sp := &Span{
		Event: Event{
			Data: payload,
		},
	}

	for b.Loop() {
		sp.GetDataSize()
	}
}

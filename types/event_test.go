package types

import (
	"maps"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/honeycombio/husky/otlp"
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
	payload := NewPayload(cfg, map[string]interface{}{
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
	expectedData := map[string]interface{}{
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

func TestIsClassicKey(t *testing.T) {
	testCases := []struct {
		name     string
		key      string
		expected bool
	}{
		// 32-character classic API keys (hex digits only)
		{name: "valid 32-char classic key - all lowercase", key: "a1b2c3d4e5f67890abcdef1234567890", expected: true},
		{name: "valid 32-char classic key - all numbers", key: "12345678901234567890123456789012", expected: true},
		{name: "valid 32-char classic key - all lowercase letters", key: "abcdefabcdefabcdefabcdefabcdefab", expected: true},
		{name: "valid 32-char classic key - mixed hex", key: "0123456789abcdef0123456789abcdef", expected: true},
		{name: "invalid 32-char key - uppercase letters", key: "A1B2C3D4E5F67890ABCDEF1234567890", expected: false},
		{name: "invalid 32-char key - contains g", key: "a1b2c3d4e5f67890abcdefg234567890", expected: false},
		{name: "invalid 32-char key - contains special chars", key: "a1b2c3d4e5f67890abcdef123456789!", expected: false},
		{name: "invalid 32-char key - contains space", key: "a1b2c3d4e5f67890abcdef12345 7890", expected: false},

		// 64-character classic ingest keys (pattern: ^hc[a-z]ic_[0-9a-z]*$)
		{name: "valid 64-char ingest key - hcaic", key: "hcaic_1234567890123456789012345678901234567890123456789012345678", expected: true},
		{name: "valid 64-char ingest key - hcbic", key: "hcbic_abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234", expected: true},
		{name: "valid 64-char ingest key - hczic", key: "hczic_0123456789abcdef0123456789abcdef0123456789abcdef0123456789", expected: true},
		{name: "valid 64-char ingest key - mixed", key: "hcxic_1234567890123456789012345678901234567890123456789012345678", expected: true},
		{name: "invalid 64-char ingest key - wrong prefix", key: "hc1ic_1234567890123456789012345678901234567890123456789012345678", expected: false},
		{name: "invalid 64-char ingest key - uppercase in prefix", key: "hcAic_1234567890123456789012345678901234567890123456789012345678", expected: false},
		{name: "invalid 64-char ingest key - missing underscore", key: "hcaic1234567890123456789012345678901234567890123456789012345678", expected: false},
		{name: "invalid 64-char ingest key - uppercase in suffix", key: "hcaic_1234567890123456789012345678901234567890123456789012345A78", expected: false},
		{name: "invalid 64-char ingest key - special char in suffix", key: "hcaic_123456789012345678901234567890123456789012345678901234567!", expected: false},

		// Edge cases for length
		{name: "empty key", key: "", expected: false},
		{name: "too short - 31 chars", key: "a1b2c3d4e5f67890abcdef123456789", expected: false},
		{name: "too long - 33 chars", key: "a1b2c3d4e5f67890abcdef12345678901", expected: false},
		{name: "too short - 63 chars", key: "hcaic_123456789012345678901234567890123456789012345678901234567", expected: false},
		{name: "too long - 65 chars", key: "hcaic_12345678901234567890123456789012345678901234567890123456789", expected: false},

		// Non-classic keys (E&S keys should return false)
		{name: "E&S key", key: "abc123DEF456ghi789jklm", expected: false},
		{name: "E&S ingest key", key: "hcxik_1234567890123456789012345678901234567890123456789012345678", expected: false},

		// Invalid patterns
		{name: "random string", key: "this-is-not-a-key", expected: false},
		{name: "numbers only but wrong length", key: "123456789012", expected: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsLegacyAPIKey(tc.key)
			assert.Equal(t, tc.expected, result, "Expected IsClassicApiKey(%q) to return %v", tc.key, tc.expected)
		})
	}
}

func BenchmarkIsLegacyAPIKey(b *testing.B) {
	tests := []struct {
		name string
		key  string
	}{
		{"Valid classic key", "a1b2c3d4e5f67890abcdef1234567890"},
		{"Invalid classic key", "abcdef0123456789abcdef01234567zz"},
		{"Valid ingest key", "hcaic_1234567890123456789012345678901234567890123456789012345678"},
		{"Invalid ingest key", "hcaic_1234567890123456789012345678901234567890123456789012345678"},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = IsLegacyAPIKey(tt.key)
			}
		})

		b.Run(tt.name+"/husky", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = otlp.IsClassicApiKey(tt.key)
			}
		})
	}
}

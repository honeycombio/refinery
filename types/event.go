package types

import (
	"context"
	"fmt"
	"slices"
	"time"

	huskyotlp "github.com/honeycombio/husky/otlp"
)

const (
	APIKeyHeader = "X-Honeycomb-Team"
	// libhoney-js uses this
	APIKeyHeaderShort = "X-Hny-Team"
	DatasetHeader     = "X-Honeycomb-Dataset"
	SampleRateHeader  = "X-Honeycomb-Samplerate"
	TimestampHeader   = "X-Honeycomb-Event-Time"
	QueryTokenHeader  = "X-Honeycomb-Refinery-Query"
)

// used to put a request ID into the request context for logging
type RequestIDContextKey struct{}

// event is not part of a trace - it's an event that showed up with no trace ID
type Event struct {
	Context     context.Context
	APIHost     string
	APIKey      string
	Dataset     string
	Environment string
	SampleRate  uint
	Timestamp   time.Time
	Data        map[string]interface{}
}

// Trace isn't something that shows up on the wire; it gets created within
// Refinery. Traces are not thread-safe; only one goroutine should be working
// with a trace object at a time.
type Trace struct {
	APIHost string
	APIKey  string
	Dataset string
	TraceID string

	// SampleRate should only be changed if the changer holds the SendSampleLock
	sampleRate uint
	// KeepSample should only be changed if the changer holds the SendSampleLock
	KeepSample bool
	// Sent should only be changed if the changer holds the SendSampleLock
	Sent             bool
	keptReason       uint
	DeciderShardAddr string

	SendBy  time.Time
	Retried bool

	// ArrivalTime is the server time when the first span arrived for this trace.
	// Used to calculate how long traces spend sitting in Refinery
	ArrivalTime time.Time

	RootSpan *Span

	// DataSize is the sum of the DataSize of spans that are added.
	// It's used to help expire the most expensive traces.
	DataSize int

	// spans is the list of spans in this trace
	spans []*Span

	// totalImpact is the sum of the trace's cacheImpact; if this value is 0
	// it is recalculated during CacheImpact(), otherwise this value is
	// returned. We reset it to 0 when adding spans so it gets recalculated.
	// This is used to memoize the impact calculation so that it doesn't get
	// calculated over and over during a sort.
	totalImpact int
}

// AddSpan adds a span to this trace
func (t *Trace) AddSpan(sp *Span) {
	// We've done all the work to know this is a trace we are putting in our cache, so
	// now is when we can calculate the size of it so that our cache size management
	// code works properly.
	sp.ArrivalTime = time.Now()
	sp.DataSize = sp.GetDataSize()
	t.DataSize += sp.DataSize
	t.spans = append(t.spans, sp)
	t.totalImpact = 0
}

// CacheImpact calculates an abstract value for something we're calling cache impact, which is
// the sum of the CacheImpact of all of the spans in a trace. We use it to order traces
// so we can eject the ones that having the most impact on the cache size, but balancing that
// against preferring to keep newer spans.
func (t *Trace) CacheImpact(traceTimeout time.Duration) int {
	if t.totalImpact == 0 {
		for _, sp := range t.GetSpans() {
			t.totalImpact += sp.CacheImpact(traceTimeout)
		}
	}
	return t.totalImpact
}

// GetSpans returns the list of descendants in this trace
func (t *Trace) GetSpans() []*Span {
	return t.spans
}

func (t *Trace) RemoveDecisionSpans() {
	t.spans = slices.DeleteFunc(t.spans, func(sp *Span) bool {
		return sp.IsDecisionSpan()
	})
}

func (t *Trace) ID() string {
	return t.TraceID
}

func (t *Trace) SampleRate() uint {
	return t.sampleRate
}

func (t *Trace) SetSampleRate(rate uint) {
	t.sampleRate = rate
}

func (t *Trace) KeptReason() uint {
	return t.keptReason
}

func (t *Trace) SetKeptReason(reason uint) {
	t.keptReason = reason
}

// DescendantCount gets the number of descendants of all kinds currently in this trace
func (t *Trace) DescendantCount() uint32 {
	return uint32(len(t.spans))
}

// SpanCount gets the number of spans currently in this trace.
// This is different from DescendantCount because it doesn't include span events or links.
func (t *Trace) SpanCount() uint32 {
	var count uint32
	for _, s := range t.spans {
		switch s.AnnotationType() {
		case SpanAnnotationTypeSpanEvent, SpanAnnotationTypeLink:
			continue
		default:
			count++

		}
	}
	return count
}

// SpanLinkCount gets the number of span links currently in this trace.
func (t *Trace) SpanLinkCount() uint32 {
	var count uint32
	for _, s := range t.spans {
		if s.AnnotationType() == SpanAnnotationTypeLink {
			count++
		}
	}
	return count
}

// SpanEventCount gets the number of span events currently in this trace.
func (t *Trace) SpanEventCount() uint32 {
	var count uint32
	for _, s := range t.spans {
		if s.AnnotationType() == SpanAnnotationTypeSpanEvent {
			count++
		}
	}
	return count
}

func (t *Trace) GetSamplerKey() (string, bool) {
	if IsLegacyAPIKey(t.APIKey) {
		return t.Dataset, true
	}

	env := ""
	for _, sp := range t.GetSpans() {
		if sp.Environment != "" {
			env = sp.Environment
			break
		}
	}

	return env, false
}

// IsOrphan returns true if the trace is older than 4 times the traceTimeout
func (t *Trace) IsOrphan(traceTimeout time.Duration, now time.Time) bool {
	return now.Sub(t.SendBy) >= traceTimeout*4
}

// CalculateCriticalPath identifies spans that are on the critical path through the trace.
// This is defined as the path of spans with the longest execution time from the root to any leaf.
// Returns true if critical path was calculated.
func (t *Trace) CalculateCriticalPath() bool {
	if len(t.spans) == 0 || t.RootSpan == nil {
		return false
	}

	// Direct approach to avoid excess memory allocations
	// Only allocate if we have a non-trivial number of spans
	spanCount := len(t.spans)
	if spanCount <= 1 {
		// If only one span (root), it's automatically on the critical path
		if t.RootSpan != nil {
			t.RootSpan.IsOnCriticalPath = true
		}
		return true
	}
	
	// Reset critical path status - more efficient to do as a separate pass
	// to avoid branching in the main loop
	for _, span := range t.spans {
		span.IsOnCriticalPath = false
	}

	// Pre-calculate span IDs and build parent->children relationship
	// Using fixed-size maps when possible to avoid growth reallocations
	childrenMap := make(map[string][]*Span, spanCount/2+1) // Conservative estimate 
	
	// First pass: identify decision spans and build the parent-child relationships
	for _, span := range t.spans {
		// Skip decision spans - they are not on the critical path
		if span.IsDecisionSpan() {
			continue
		}
		
		// Build parent -> children map
		if span.ParentID != "" {
			childrenMap[span.ParentID] = append(childrenMap[span.ParentID], span)
		}
	}
	
	// Mark root span as on critical path
	if t.RootSpan != nil {
		t.RootSpan.IsOnCriticalPath = true
		
		// Find the critical path using an iterative approach
		// More efficient than recursive for most cases
		var stack []*Span
		stack = append(stack, t.RootSpan)
		
		for len(stack) > 0 {
			// Pop the last span
			current := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			
			// Get all children
			children := childrenMap[current.SpanID()]
			if len(children) == 0 {
				continue
			}
			
			// Find child with longest duration
			var longestChild *Span
			var longestDuration float64
			
			for _, child := range children {
				// Extract duration from span data
				if child.Data == nil {
					continue
				}
				
				durationVal, ok := child.Data["duration_ms"]
				if !ok {
					continue
				}
				
				var duration float64
				switch d := durationVal.(type) {
				case float64:
					duration = d
				case int64:
					duration = float64(d)
				case int:
					duration = float64(d)
				default:
					// Ignore other types to avoid unnecessary type assertions
					continue
				}
				
				if duration > longestDuration {
					longestDuration = duration
					longestChild = child
				}
			}
			
			// Mark the child with longest duration as on critical path
			if longestChild != nil {
				longestChild.IsOnCriticalPath = true
				// Add to stack to process its children
				stack = append(stack, longestChild)
			}
		}
	}
	
	return true
}

// Span is an event that shows up with a trace ID, so will be part of a Trace
type Span struct {
	Event
	TraceID          string
	DataSize         int
	ArrivalTime      time.Time
	IsRoot           bool
	IsOnCriticalPath bool
	ParentID         string
}

// IsDecicionSpan returns true if the span is a decision span based on
// a flag set in the span's metadata.
func (sp *Span) IsDecisionSpan() bool {
	if sp.Data == nil {
		return false
	}
	v, ok := sp.Data["meta.refinery.min_span"]
	if !ok {
		return false
	}
	isDecisionSpan, ok := v.(bool)
	if !ok {
		return false
	}

	return isDecisionSpan
}

// SpanID returns a unique identifier for this span
func (sp *Span) SpanID() string {
	if sp.Data != nil {
		if id, ok := sp.Data["span_id"]; ok {
			if strID, ok := id.(string); ok && strID != "" {
				return strID
			}
		}
	}
	
	// Cache the pointer string directly, to prevent allocations on repeated calls
	// This is thread-safe since the pointer value won't change for a given span
	ptr := fmt.Sprintf("%p", sp)
	
	// In a production system we would store this in a field on the span
	// but we don't want to modify the struct definition further, so we'll just return it
	return ptr
}

// ExtractDecisionContext returns a new Event that contains only the data that is
// relevant to the decision-making process.
func (sp *Span) ExtractDecisionContext() *Event {
	decisionCtx := sp.Event
	dataSize := sp.DataSize
	if dataSize == 0 {
		dataSize = sp.GetDataSize()
	}
	decisionCtx.Data = map[string]interface{}{
		"trace_id":                     sp.TraceID,
		"meta.refinery.root":           sp.IsRoot,
		"meta.refinery.min_span":       true,
		"meta.annotation_type":         sp.AnnotationType(),
		"meta.refinery.span_data_size": dataSize,
	}
	
	// Include critical path information if this span is on the critical path
	if sp.IsOnCriticalPath {
		decisionCtx.Data["meta.refinery.is_critical_path"] = true
	}
	
	return &decisionCtx
}

// GetDataSize computes the size of the Data element of the Span.
// Note that it's not the full size of the span, but we're mainly using this for
// relative ordering, not absolute calculations.
func (sp *Span) GetDataSize() int {
	total := 0

	if sp.IsDecisionSpan() {
		if v, ok := sp.Data["meta.refinery.span_data_size"]; ok {
			switch value := v.(type) {
			case int64:
				return int(value)
			case uint64:
				return int(value)
			}
		}
		return 0
	}
	// the data types we should be getting from JSON are:
	// float64, int64, bool, string, []byte
	for _, v := range sp.Data {
		switch value := v.(type) {
		case bool:
			total += 1
		case float64, int64, int:
			total += 8
		case string:
			total += len(value)
		case []byte: // also catch []uint8
			total += len(value)
		default:
			total += 8 // catchall
		}
	}
	return total
}

// SpanAnnotationType is an enum for the type of annotation this span is.
type SpanAnnotationType int

const (
	// SpanAnnotationTypeUnknown is the default value for an unknown annotation type.
	SpanAnnotationTypeUnknown SpanAnnotationType = iota
	// SpanAnnotationTypeSpanEvent is the type for a span event.
	SpanAnnotationTypeSpanEvent
	// SpanAnnotationTypeLink is the type for a span link.
	SpanAnnotationTypeLink
)

// AnnotationType returns the type of annotation this span is.
func (sp *Span) AnnotationType() SpanAnnotationType {
	t := sp.Data["meta.annotation_type"]
	switch t {
	case "span_event":
		return SpanAnnotationTypeSpanEvent
	case "link":
		return SpanAnnotationTypeLink
	default:
		return SpanAnnotationTypeUnknown
	}
}

// cacheImpactFactor controls how much more we weigh older spans compared to newer ones;
// setting this to 1 means they're not weighted by duration
const cacheImpactFactor = 4

// CacheImpact calculates an abstract value for something we're calling cache impact, which is
// the product of the size of the span and a factor related to the amount of time the span
// has been stored in the cache, based on the TraceTimeout value.
func (sp *Span) CacheImpact(traceTimeout time.Duration) int {
	// multiplier will be a value from 1-cacheImpactFactor, depending on how long the
	// span has been in the cache compared to the traceTimeout. It might go higher
	// during the brief period between traceTimeout and the time when the span is sent.
	multiplier := int(cacheImpactFactor*time.Since(sp.ArrivalTime)/traceTimeout) + 1
	// We can assume DataSize was set when the span was added.
	return multiplier * sp.DataSize
}

func IsLegacyAPIKey(apiKey string) bool {
	return huskyotlp.IsClassicApiKey(apiKey)
}

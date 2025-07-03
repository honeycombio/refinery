package types

import (
	"context"
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
	Data        Payload

	// EnqueuedUnixMicro is the time when the event was enqueued for transmission
	// used for usec-resolution metrics.
	EnqueuedUnixMicro int64

	dataSize int
}

// GetDataSize computes the size of the Data element of the Event.
func (e *Event) GetDataSize() int {
	if e.dataSize == 0 {
		e.dataSize = e.Data.GetDataSize()
	}
	return e.dataSize
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

	spanCount      uint32
	spanEventCount uint32
	spanLinkCount  uint32
}

// AddSpan adds a span to this trace
func (t *Trace) AddSpan(sp *Span) {
	// We've done all the work to know this is a trace we are putting in our cache, so
	// now is when we can calculate the size of it so that our cache size management
	// code works properly.
	sp.ArrivalTime = time.Now()
	t.DataSize += sp.GetDataSize()
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

func (t *Trace) calculateSpanCounts() {
	var (
		spanCount      uint32
		spanEventCount uint32
		spanLinkCount  uint32
	)
	for _, s := range t.spans {
		switch s.AnnotationType() {
		case SpanAnnotationTypeSpanEvent:
			// SpanEventCount gets the number of span events currently in this trace.
			spanEventCount++
		case SpanAnnotationTypeLink:
			// SpanLinkCount gets the number of span links currently in this trace.
			spanLinkCount++
		default:
			// SpanCount gets the number of spans currently in this trace.
			// This is different from DescendantCount because it doesn't include span events or links.
			spanCount++
		}
	}

	t.spanCount = spanCount
	t.spanEventCount = spanEventCount
	t.spanLinkCount = spanLinkCount
}

// DescendantCount gets the number of descendants of all kinds currently in this trace
func (t *Trace) DescendantCount() uint32 {
	return uint32(len(t.spans))
}

func (t *Trace) SpanCount() uint32 {
	// if we haven't calculated the span counts yet, do it now
	// we do this so that we don't have to calculate the counts every time
	if t.spanLinkCount == 0 && t.spanCount == 0 && t.spanEventCount == 0 {
		t.calculateSpanCounts()
	}
	return t.spanCount
}

func (t *Trace) SpanLinkCount() uint32 {
	// if we haven't calculated the span counts yet, do it now
	// we do this so that we don't have to calculate the counts every time
	if t.spanLinkCount == 0 && t.spanCount == 0 && t.spanEventCount == 0 {
		t.calculateSpanCounts()
	}
	return t.spanLinkCount
}

func (t *Trace) SpanEventCount() uint32 {
	// if we haven't calculated the span counts yet, do it now
	// we do this so that we don't have to calculate the counts every time
	if t.spanLinkCount == 0 && t.spanCount == 0 && t.spanEventCount == 0 {
		t.calculateSpanCounts()
	}
	return t.spanEventCount
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

// Span is an event that shows up with a trace ID, so will be part of a Trace
// This is not thread-safe; only one goroutine should be working with a span object at a time.
type Span struct {
	Event
	TraceID        string
	ArrivalTime    time.Time
	IsRoot         bool
	annotationType SpanAnnotationType
}

// IsDecicionSpan returns true if the span is a decision span based on
// a flag set in the span's metadata.
func (sp *Span) IsDecisionSpan() bool {
	return sp.Data.MetaRefineryMinSpan.Value
}

// ExtractDecisionContext returns a new Event that contains only the data that is
// relevant to the decision-making process.
func (sp *Span) ExtractDecisionContext() *Event {
	decisionCtx := sp.Event
	dataSize := sp.Event.GetDataSize()

	// Create a new empty payload and set metadata fields directly
	decisionCtx.Data = NewPayload(map[string]interface{}{})
	decisionCtx.Data.MetaTraceID = sp.TraceID
	decisionCtx.Data.MetaRefineryRoot.Set(sp.IsRoot)
	decisionCtx.Data.MetaRefineryMinSpan.Set(true)
	decisionCtx.Data.MetaAnnotationType = sp.AnnotationType().String()
	decisionCtx.Data.MetaRefinerySpanDataSize = int64(dataSize)

	if sp.Data.MetaRefinerySendBy > 0 {
		decisionCtx.Data.MetaRefinerySendBy = sp.Data.MetaRefinerySendBy
	}

	return &decisionCtx
}

func (sp *Span) SetSendBy(sendBy time.Time) {
	sp.Data.MetaRefinerySendBy = sendBy.Unix()
}

func (sp *Span) GetSendBy() (time.Time, bool) {
	if sp.Data.MetaRefinerySendBy == 0 {
		return time.Time{}, false
	}
	return time.Unix(sp.Data.MetaRefinerySendBy, 0), true
}

// GetDataSize computes the size of the Data element of the Span.
// Note that it's not the full size of the span, but we're mainly using this for
// relative ordering, not absolute calculations.
func (sp *Span) GetDataSize() int {
	if sp.IsDecisionSpan() {
		return int(sp.Data.MetaRefinerySpanDataSize)
	}

	return sp.Event.GetDataSize()
}

// SpanAnnotationType is an enum for the type of annotation this span is.
type SpanAnnotationType int

const (
	// SpanAnnotationTypeUnSet is the default value for an unset annotation type.
	SpanAnnotationTypeUnSet SpanAnnotationType = iota
	// SpanAnnotationTypeUnknown is the value for an unknown annotation type.
	SpanAnnotationTypeUnknown
	// SpanAnnotationTypeSpanEvent is the type for a span event.
	SpanAnnotationTypeSpanEvent
	// SpanAnnotationTypeLink is the type for a span link.
	SpanAnnotationTypeLink
)

func (sat SpanAnnotationType) String() string {
	switch sat {
	case SpanAnnotationTypeUnSet:
		return ""
	case SpanAnnotationTypeUnknown:
		return "unknown"
	case SpanAnnotationTypeSpanEvent:
		return "span_event"
	case SpanAnnotationTypeLink:
		return "link"
	default:
		return ""
	}
}

// GetSpanAnnotationType returns the type of annotation this span is.
func (sp *Span) AnnotationType() SpanAnnotationType {
	if sp.annotationType != SpanAnnotationTypeUnSet {
		return sp.annotationType
	}

	switch sp.Data.MetaAnnotationType {
	case "span_event":
		sp.annotationType = SpanAnnotationTypeSpanEvent
	case "link":
		sp.annotationType = SpanAnnotationTypeLink
	default:
		sp.annotationType = SpanAnnotationTypeUnknown
	}

	return sp.annotationType
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
	return multiplier * sp.GetDataSize()
}

func IsLegacyAPIKey(apiKey string) bool {
	return huskyotlp.IsClassicApiKey(apiKey)
}

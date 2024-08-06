package cache

import (
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/types"
)

type TraceSentRecord interface {
	// Kept returns whether the trace was kept (sampled and sent to honeycomb) or dropped.
	Kept() bool
	// Rate() returns the sample rate for the trace
	Rate() uint
	// DescendantCount returns the count of items associated with the trace, including all types of children like span links and span events.
	DescendantCount() uint
	// SpanEventCount returns the count of span events in the trace.
	SpanEventCount() uint
	// SpanLinkCount returns the count of span links in the trace.
	SpanLinkCount() uint
	// SpanCount returns the count of child spans in the trace.
	SpanCount() uint
	// Count records additional spans in the totals
	Count(*types.Span)
}

type TraceSentCache interface {
	// Record preserves the record of a trace being sent or not.
	Record(trace KeptTrace, keep bool, reason string)
	// CheckTrace if a trace is in the cache; if found, it returns the appropriate TraceSentRecord and true, else nil and false.
	// It does not modify the count information.
	CheckTrace(traceID string) (TraceSentRecord, string, bool)
	// CheckSpan tests if a trace corresponding to the span is in the cache; if found, it returns the appropriate TraceSentRecord and true,
	// else nil and false.
	CheckSpan(span *types.Span) (TraceSentRecord, string, bool)
	// Stop halts the cache in preparation for shutdown
	Stop()
	// Resize adjusts the size of the cache according to the Config passed in
	Resize(cfg config.SampleCacheConfig) error
}

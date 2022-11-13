package cache

import (
	"github.com/honeycombio/refinery/types"
)

type TraceSentRecord interface {
	// Kept returns whether the trace was kept (sampled and sent to honeycomb) or dropped.
	Kept() bool
	// Rate() returns the sample rate for the trace
	Rate() uint
	// Descendants returns the count of items associated with the trace, including all types of children like span links and span events.
	Descendants() uint
	Add(*types.Span)
}

type TraceSentCache interface {
	// Record preserves the record of a trace being sent or not.
	Record(trace *types.Trace, keep bool)
	// Check tests if a trace corresponding to the span is in the cache; if found, it returns the appropriate TraceSentRecord and true,
	// else nil and false.
	Check(span *types.Span) (TraceSentRecord, bool)
}

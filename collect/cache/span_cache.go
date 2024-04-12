package cache

import (
	"github.com/honeycombio/refinery/types"
	"github.com/jonboulle/clockwork"
)

type SpanCache interface {
	// Adds a span to the cache. If a trace with the same traceID already exists,
	// the span will be added to that trace. If no trace with the traceID exists,
	// a new trace will be created. Only returns an error if there is a problem
	// adding the span to the cache.
	Set(span *types.Span) error
	// Retrieves a trace from the cache by traceID. If no trace with the traceID
	// exists, returns nil.
	// Note: the returned trace is a pointer to the trace in the cache. It should
	// be treated as a read-only trace. This is because the cache may modify the
	// trace after it is returned. If you need to modify the trace, make a copy.
	Get(traceID string) *types.Trace
	// Returns the desired fraction of oldest trace IDs in the cache.
	// (e.g. if fract is 0.1, returns the oldest 10% of trace IDs)
	GetOldest(fract float64) []string
	// Returns up to n trace IDs from the cache; successive calls will return
	// different trace IDs until all have been returned, then it will start over.
	GetTraceIDs(n int) []string
	// Removes a trace from the cache by traceID. If no trace with the traceID exists,
	// does nothing.
	Remove(traceID string)
	// Returns the number of traces in the cache.
	Len() int
	// Clock returns the clock used by the cache.
	GetClock() clockwork.Clock
}

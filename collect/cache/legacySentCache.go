package cache

import (
	lru "github.com/hashicorp/golang-lru"
	"github.com/honeycombio/refinery/types"
)

// legacySentRecord is Refinery's original traceSent cache. It keeps the same records
// for both kept and dropped traces and the size of the sent cache is set based on the size
// of the live trace cache.

// legacySentRecord is an internal record we leave behind when sending a trace to remember
// our decision for the future, so any delinquent spans that show up later can
// be dropped or passed along.
type legacySentRecord struct {
	keep      bool // true if the trace was kept, false if it was dropped
	rate      uint // sample rate used when sending the trace
	spanCount uint // number of spans in the trace (we decorate the root span with this)
}

func (t *legacySentRecord) Kept() bool {
	return t.keep
}

func (t *legacySentRecord) Rate() uint {
	return t.rate
}

func (t *legacySentRecord) Descendants() uint {
	return uint(t.spanCount)
}

func (t *legacySentRecord) Add(*types.Span) {
	t.spanCount++
}

// Make sure it implements TraceSentRecord
var _ TraceSentRecord = (*legacySentRecord)(nil)

type legacySentCache struct {
	sentTraceCache *lru.Cache
}

// Make sure it implements TraceSentCache
var _ TraceSentCache = (*legacySentCache)(nil)

func NewDefaultSentCache(capacity int) (TraceSentCache, error) {
	stc, err := lru.New(capacity)
	if err != nil {
		return nil, err
	}
	return &legacySentCache{sentTraceCache: stc}, nil
}

func (c *legacySentCache) Record(trace *types.Trace, keep bool) {
	// record this decision in the sent record LRU for future spans
	sentRecord := legacySentRecord{
		keep:      keep,
		rate:      trace.SampleRate,
		spanCount: trace.SpanCount(),
	}
	c.sentTraceCache.Add(trace.TraceID, &sentRecord)
}

func (c *legacySentCache) Check(span *types.Span) (TraceSentRecord, bool) {
	if sentRecord, found := c.sentTraceCache.Get(span.TraceID); found {
		if sr, ok := sentRecord.(*legacySentRecord); ok {
			sr.Add(span)
			return sr, true
		}
	}
	return nil, false
}

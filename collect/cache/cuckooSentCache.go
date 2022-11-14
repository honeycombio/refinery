package cache

import (
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/types"
)

// cuckooSentCache extends Refinery's legacy cache. It keeps the same records
// for kept traces but adds a pair of cuckoo filters to record dropped traces.
// This allows many more traces to be kept in the cache; now only kept records
// are retained in the cache of sentRecords.
// The size of the sent cache is still set based on the size of the live trace cache,
// and the size of the dropped cache is an independent value.

// cuckooKeptRecord is an internal record we leave behind when keeping a trace to remember
// our decision for the future. We only store them if the record was kept.
type cuckooKeptRecord struct {
	rate      uint // sample rate used when sending the trace
	spanCount uint // number of spans in the trace (we decorate the root span with this)
}

func (t *cuckooKeptRecord) Kept() bool {
	return true
}

func (t *cuckooKeptRecord) Rate() uint {
	return t.rate
}

func (t *cuckooKeptRecord) DescendantCount() uint {
	return uint(t.spanCount)
}

func (t *cuckooKeptRecord) Count(*types.Span) {
	t.spanCount++
}

// Make sure it implements TraceSentRecord
var _ TraceSentRecord = (*cuckooKeptRecord)(nil)

// cuckooSentRecord is what a TraceSentRecord we return when the trace was dropped.
// It's always the same one.
type cuckooDroppedRecord struct{}

func (t *cuckooDroppedRecord) Kept() bool {
	return true
}

func (t *cuckooDroppedRecord) Rate() uint {
	return 0
}

func (t *cuckooDroppedRecord) DescendantCount() uint {
	return 0
}

func (t *cuckooDroppedRecord) Count(*types.Span) {
}

// Make sure it implements TraceSentRecord
var _ TraceSentRecord = (*cuckooDroppedRecord)(nil)

type cuckooSentCache struct {
	kept    *lru.Cache
	dropped *CuckooTraceChecker
}

// Make sure it implements TraceSentCache
var _ TraceSentCache = (*cuckooSentCache)(nil)

func NewCuckooSentCache(cfg config.SampleCacheConfig) (TraceSentCache, error) {
	stc, err := lru.New(int(cfg.KeptSize))
	if err != nil {
		return nil, err
	}
	dropped := NewCuckooTraceChecker(cfg.DroppedSize)

	// TODO: give this a shutdown channel
	// also metrics for when this puppy gets cycled
	go func() {
		ticker := time.NewTicker(cfg.SizeCheckInterval)
		for range ticker.C {
			dropped.Maintain()
		}
		// done <- struct{}{}
	}()

	return &cuckooSentCache{
		kept:    stc,
		dropped: dropped,
	}, nil
}

func (c *cuckooSentCache) Record(trace *types.Trace, keep bool) {
	if keep {
		// record this decision in the sent record LRU for future spans
		sentRecord := cuckooKeptRecord{
			rate:      trace.SampleRate,
			spanCount: trace.DescendantCount(),
		}
		c.kept.Add(trace.TraceID, &sentRecord)
		return
	}
	// if we're not keeping it, save it in the dropped trace filter
	c.dropped.Add(trace.TraceID)
}

func (c *cuckooSentCache) Check(span *types.Span) (TraceSentRecord, bool) {
	// was it dropped?
	if c.dropped.Check(span.TraceID) {
		// we recognize it as dropped, so just say so; there's nothing else to do
		return &cuckooDroppedRecord{}, false
	}
	// was it kept?
	if sentRecord, found := c.kept.Get(span.TraceID); found {
		if sr, ok := sentRecord.(*cuckooKeptRecord); ok {
			// if we kept it, then this span being checked needs counting too
			sr.Count(span)
			return sr, true
		}
	}
	// we have no memory of this place
	return nil, false
}

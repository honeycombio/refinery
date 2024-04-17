package cache

import (
	"sync"
	"time"

	"github.com/facebookgo/startstop"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

// cuckooSentCache extends Refinery's legacy cache. It keeps the same records
// for kept traces but adds a pair of cuckoo filters to record dropped traces.
// This allows many more traces to be kept in the cache; now only kept records
// are retained in the cache of sentRecords.
// The size of the sent cache is still set based on the size of the live trace cache,
// and the size of the dropped cache is an independent value.

// keptTraceCacheEntry is an internal record we leave behind when keeping a trace to remember
// our decision for the future. We only store them if the record was kept.
type keptTraceCacheEntry struct {
	rate           uint32 // sample rate used when sending the trace
	eventCount     uint32 // number of descendants in the trace (we decorate the root span with this)
	spanEventCount uint32 // number of span events in the trace
	spanLinkCount  uint32 // number of span links in the trace
	spanCount      uint32 // number of spans in the trace
	reason         uint32 // which rule was used to decide to keep the trace
}

// KeptTrace is an interface for a trace that was kept.
// It contains all the information we need to remember about the trace.
type KeptTrace interface {
	ID() string
	SampleRate() uint
	DescendantCount() uint32
	SpanEventCount() uint32
	SpanLinkCount() uint32
	SpanCount() uint32
	SetSentReason(uint)
	SentReason() uint
}

func NewKeptTraceCacheEntry(t KeptTrace) *keptTraceCacheEntry {
	if t == nil {
		return &keptTraceCacheEntry{}
	}

	return &keptTraceCacheEntry{
		rate:           uint32(t.SampleRate()),
		eventCount:     t.DescendantCount(),
		spanEventCount: t.SpanEventCount(),
		spanLinkCount:  t.SpanLinkCount(),
		spanCount:      t.SpanCount(),
		reason:         uint32(t.SentReason()),
	}
}

func (t *keptTraceCacheEntry) Kept() bool {
	return true
}

func (t *keptTraceCacheEntry) Rate() uint {
	return uint(t.rate)
}

// DescendantCount returns the count of items associated with the trace, including all types of children like span links and span events.
func (t *keptTraceCacheEntry) DescendantCount() uint {
	return uint(t.eventCount)
}

// SpanEventCount returns the count of span events in the trace.
func (t *keptTraceCacheEntry) SpanEventCount() uint {
	return uint(t.spanEventCount)
}

// SpanLinkCount returns the count of span links in the trace.
func (t *keptTraceCacheEntry) SpanLinkCount() uint {
	return uint(t.spanLinkCount)
}

// SpanCount returns the count of spans in the trace.
func (t *keptTraceCacheEntry) SpanCount() uint {
	return uint(t.spanCount)
}

// Count records additional spans in the cache record.
func (t *keptTraceCacheEntry) Count(s *types.Span) {
	t.eventCount++
	switch s.Type() {
	case types.SpanTypeEvent:
		t.spanEventCount++
	case types.SpanTypeLink:
		t.spanLinkCount++
	default:
		t.spanCount++
	}
}

// Make sure it implements TraceSentRecord
var _ TraceSentRecord = (*keptTraceCacheEntry)(nil)

// cuckooDroppedRecord is what we return when the trace was dropped.
// It's always the same one.
type cuckooDroppedRecord struct{}

func (t *cuckooDroppedRecord) Kept() bool {
	return false
}

func (t *cuckooDroppedRecord) Rate() uint {
	return 0
}

func (t *cuckooDroppedRecord) DescendantCount() uint {
	return 0
}

func (t *cuckooDroppedRecord) SpanEventCount() uint {
	return 0
}

func (t *cuckooDroppedRecord) SpanLinkCount() uint {
	return 0
}

func (t *cuckooDroppedRecord) SpanCount() uint {
	return 0
}

func (t *cuckooDroppedRecord) Count(*types.Span) {
}

func (t *cuckooDroppedRecord) Reason() uint {
	return 0
}

// Make sure it implements TraceSentRecord
var _ TraceSentRecord = (*cuckooDroppedRecord)(nil)

type CuckooSentCache struct {
	Cfg     config.Config   `inject:""`
	Met     metrics.Metrics `inject:"genericMetrics"`
	kept    *lru.Cache[string, *keptTraceCacheEntry]
	dropped *CuckooTraceChecker

	// The done channel is used to decide when to terminate the monitor
	// goroutine. When resizing the cache, we write to the channel, but
	// when terminating the system, call Stop() to close the channel.
	// Either one causes the goroutine to shut down, and in resizing
	// we then start a new monitor.
	done chan struct{}

	// This mutex is for managing kept traces
	keptMut     sync.Mutex
	sentReasons *SentReasonsCache
}

// Make sure it implements TraceSentCache
var _ TraceSentCache = (*CuckooSentCache)(nil)

// ensure it implements startstop.Stopper
var _ startstop.Stopper = (*CuckooSentCache)(nil)

// ensure it implements startstop.Starter
var _ startstop.Starter = (*CuckooSentCache)(nil)

func (c *CuckooSentCache) Start() error {
	cfg := c.Cfg.GetSampleCacheConfig()
	kept, err := lru.New[string, *keptTraceCacheEntry](int(cfg.KeptSize))
	if err != nil {
		return err
	}
	c.kept = kept
	c.dropped = NewCuckooTraceChecker(cfg.DroppedSize, c.Met)
	c.sentReasons = NewSentReasonsCache(c.Met)
	c.done = make(chan struct{})

	go c.monitor()
	return nil
}

// goroutine to monitor the cache and cycle the size check periodically
func (c *CuckooSentCache) monitor() {
	ticker := time.NewTicker(time.Duration(c.Cfg.GetSampleCacheConfig().SizeCheckInterval))
	for {
		select {
		case <-ticker.C:
			c.dropped.Maintain()
		case <-c.done:
			ticker.Stop()
			return
		}
	}
}

// Stop halts the monitor goroutine
func (c *CuckooSentCache) Stop() error {
	close(c.done)
	return nil
}

func (c *CuckooSentCache) Record(trace KeptTrace, keep bool, reason string) {
	if keep {
		// record this decision in the sent record LRU for future spans
		trace.SetSentReason(c.sentReasons.Set(reason))
		sentRecord := NewKeptTraceCacheEntry(trace)

		c.keptMut.Lock()
		defer c.keptMut.Unlock()
		c.kept.Add(trace.ID(), sentRecord)

		return
	}
	// if we're not keeping it, save it in the dropped trace filter
	c.dropped.Add(trace.ID())
}

// fast check to see if we've already dropped this trace
func (c *CuckooSentCache) Dropped(traceID string) bool {
	return c.dropped.Check(traceID)
}

func (c *CuckooSentCache) Check(span *types.Span) (TraceSentRecord, string, bool) {
	// was it dropped?
	if c.dropped.Check(span.TraceID) {
		// we recognize it as dropped, so just say so; there's nothing else to do
		return &cuckooDroppedRecord{}, "", false
	}
	// was it kept?
	c.keptMut.Lock()
	defer c.keptMut.Unlock()
	if sentRecord, found := c.kept.Get(span.TraceID); found {
		// if we kept it, then this span being checked needs counting too
		sentRecord.Count(span)
		reason, _ := c.sentReasons.Get(uint(sentRecord.reason))
		return sentRecord, reason, true
	}
	// we have no memory of this place
	return nil, "", false
}

// Test checks if a trace was kept or dropped, and returns the reason if it was kept.
// The bool return value is true if the trace was found in the cache.
func (c *CuckooSentCache) Test(traceID string) (TraceSentRecord, string, bool) {
	// was it dropped?
	if c.dropped.Check(traceID) {
		// we recognize it as dropped, so just say so; there's nothing else to do
		return &cuckooDroppedRecord{}, "", true
	}
	// was it kept?
	c.keptMut.Lock()
	defer c.keptMut.Unlock()
	if sentRecord, found := c.kept.Get(traceID); found {
		reason, _ := c.sentReasons.Get(uint(sentRecord.reason))
		return sentRecord, reason, true
	}
	// we have no memory of this place
	return nil, "", false
}

func (c *CuckooSentCache) Resize(cfg config.SampleCacheConfig) error {
	stc, err := lru.New[string, *keptTraceCacheEntry](int(cfg.KeptSize))
	if err != nil {
		return err
	}

	// grab all the items in the current cache; if it's larger than
	// what will fit in the new one, discard the oldest ones
	// (we don't have to do anything with the ones we discard, this is
	// the trace decisions cache).
	c.keptMut.Lock()
	defer c.keptMut.Unlock()
	keys := c.kept.Keys()
	if len(keys) > int(cfg.KeptSize) {
		keys = keys[len(keys)-int(cfg.KeptSize):]
	}
	// copy all the keys to the new cache in order
	for _, k := range keys {
		if v, found := c.kept.Get(k); found {
			stc.Add(k, v)
		}
	}
	c.kept = stc

	// also set up the drop cache size to change eventually
	c.dropped.SetNextCapacity(cfg.DroppedSize)

	// shut down the old monitor and create a new one
	c.done <- struct{}{}
	go c.monitor()
	return nil
}

func (c *CuckooSentCache) GetMetrics() (map[string]interface{}, error) {
	cfg := c.Cfg.GetSampleCacheConfig()
	metrics := map[string]interface{}{
		"sent_cache_kept":             c.kept.Len(),
		"sent_cache_kept_capacity":    cfg.KeptSize,
		"sent_cache_dropped":          c.dropped.current.Count(),
		"sent_cache_dropped_load":     c.dropped.current.LoadFactor(),
		"sent_cache_dropped_capacity": cfg.DroppedSize,
	}
	return metrics, nil
}

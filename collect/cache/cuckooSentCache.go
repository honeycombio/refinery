package cache

import (
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
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

// cuckooSentRecord is what we return when the trace was dropped.
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

func (t *cuckooDroppedRecord) Count(*types.Span) {
}

// Make sure it implements TraceSentRecord
var _ TraceSentRecord = (*cuckooDroppedRecord)(nil)

type cuckooSentCache struct {
	kept    *lru.Cache
	dropped *CuckooTraceChecker
	cfg     config.SampleCacheConfig

	// The done channel is used to decide when to terminate the monitor
	// goroutine. When resizing the cache, we write to the channel, but
	// when terminating the system, call Stop() to close the channel.
	// Either one causes the goroutine to shut down, and in resizing
	// we then start a new monitor.
	done chan struct{}

	// This mutex is for managing kept traces
	keptMut sync.Mutex
}

// Make sure it implements TraceSentCache
var _ TraceSentCache = (*cuckooSentCache)(nil)

func NewCuckooSentCache(cfg config.SampleCacheConfig, met metrics.Metrics) (TraceSentCache, error) {
	stc, err := lru.New(int(cfg.KeptSize))
	if err != nil {
		return nil, err
	}
	dropped := NewCuckooTraceChecker(cfg.DroppedSize, met)

	cache := &cuckooSentCache{
		kept:    stc,
		dropped: dropped,
		cfg:     cfg,
		done:    make(chan struct{}),
	}
	go cache.monitor()
	return cache, nil
}

// goroutine to monitor the cache and cycle the size check periodically
func (c *cuckooSentCache) monitor() {
	ticker := time.NewTicker(c.cfg.SizeCheckInterval)
	for {
		select {
		case <-ticker.C:
			c.dropped.Maintain()
		case <-c.done:
			return
		}
	}
}

// Stop halts the monitor goroutine
func (c *cuckooSentCache) Stop() {
	close(c.done)
}

func (c *cuckooSentCache) Record(trace *types.Trace, keep bool) {
	if keep {
		// record this decision in the sent record LRU for future spans
		sentRecord := cuckooKeptRecord{
			rate:      trace.SampleRate,
			spanCount: trace.DescendantCount(),
		}
		c.keptMut.Lock()
		defer c.keptMut.Unlock()
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
	c.keptMut.Lock()
	defer c.keptMut.Unlock()
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

func (c *cuckooSentCache) Resize(cfg config.SampleCacheConfig) error {
	stc, err := lru.New(int(cfg.KeptSize))
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

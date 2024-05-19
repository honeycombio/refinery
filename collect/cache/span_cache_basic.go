package cache

import (
	"sort"
	"sync"
	"time"

	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
	"github.com/jonboulle/clockwork"
)

// this is a naive implementation that uses a map
// and doesn't try to be clever about memory usage or allocations
type SpanCache_basic struct {
	Cfg     config.Config   `inject:""`
	Clock   clockwork.Clock `inject:""`
	Metrics metrics.Metrics `inject:"genericMetrics"`
	cache   map[string]*types.Trace
	mut     sync.RWMutex

	// current and nextix are used only by the GetTraceIDs method and are not protected
	// by the mutex; they are only accessed by the goroutine that calls GetTraceIDs.
	current []string
	nextix  int
}

// ensure that spanCache implements SpanCache
var _ SpanCache = &SpanCache_basic{}

// ensure that spanCache implements startstop.Starter
var _ startstop.Starter = &SpanCache_basic{}

func (sc *SpanCache_basic) Start() error {
	sc.Metrics.Register("spancache_spans", "updown")
	sc.Metrics.Register("spancache_traces", "updown")
	sc.cache = make(map[string]*types.Trace)
	return nil
}

func (sc *SpanCache_basic) GetClock() clockwork.Clock {
	return sc.Clock
}

func (sc *SpanCache_basic) Set(sp *types.Span) error {
	sc.mut.Lock()
	defer sc.mut.Unlock()
	traceID := sp.TraceID
	trace, ok := sc.cache[traceID]
	if !ok {
		trace = &types.Trace{
			APIHost:     sp.APIHost,
			APIKey:      sp.APIKey,
			Dataset:     sp.Dataset,
			TraceID:     traceID,
			ArrivalTime: sc.Clock.Now(),
		}
		sc.cache[traceID] = trace
		sc.Metrics.Up("spancache_traces")
	}
	if sp.IsRoot {
		trace.RootSpan = sp
	}
	trace.AddSpan(sp)
	sc.Metrics.Up("spancache_spans")
	return nil
}

func (sc *SpanCache_basic) Get(traceID string) *types.Trace {
	sc.mut.RLock()
	defer sc.mut.RUnlock()
	trace, ok := sc.cache[traceID]
	if !ok {
		return nil
	}
	return trace
}

func (sc *SpanCache_basic) GetOldest(fract float64) []string {
	if fract <= 0 {
		return nil
	}

	type tidWithImpact struct {
		id     string
		impact int
	}
	sc.mut.RLock()
	count := len(sc.cache)
	sc.mut.RUnlock()
	n := int(float64(count) * fract)
	ids := make([]tidWithImpact, 0, n)

	timeout := sc.Cfg.GetTraceTimeout()
	if timeout == 0 {
		timeout = 60 * time.Second
	}

	sc.mut.RLock()
	for traceID := range sc.cache {
		ids = append(ids, tidWithImpact{
			id:     traceID,
			impact: sc.cache[traceID].CacheImpact(timeout),
		})
	}
	sc.mut.RUnlock()
	// Sort traces by CacheImpact, heaviest first
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].impact > ids[j].impact
	})

	if len(ids) < n {
		n = len(ids)
	}
	ret := make([]string, n)
	for i := 0; i < n; i++ {
		ret[i] = ids[i].id
	}
	return ret
}

// This gets a batch of up to n traceIDs from the cache; it's used to get a
// batch of traceIDs to process in parallel. It snapshots the active map (into
// current) and returns a slice of traceIDs that were current at the time of the
// call. It will return successive slices of traceIDs until it has returned all
// of them, then it will start over from a fresh snapshot. GetTraceIDs is not
// concurrency-safe; it is intended to be called from a single goroutine.
func (sc *SpanCache_basic) GetTraceIDs(n int) []string {
	// this is the only function that looks at current or nextix so it
	// doesn't need to lock those fields
	if sc.current == nil || sc.nextix >= len(sc.current) {
		sc.mut.RLock()
		sc.current = make([]string, 0, len(sc.cache))
		for traceID := range sc.cache {
			sc.current = append(sc.current, traceID)
		}
		sc.mut.RUnlock()
		sc.nextix = 0
	}
	if sc.nextix+n > len(sc.current) {
		n = len(sc.current) - sc.nextix
	}
	defer func() {
		// update the next index position to be the
		// last element of the returned slice
		sc.nextix += n
	}()

	return sc.current[sc.nextix : sc.nextix+n]
}

// This gets all the traceIDs that are older than 2*(TraceTimeout+SendDelay).
// These are traces that should have been decided but we didn't see a published
// decision. This is used to help clean up the cache and in situations where the
// cache has data but we didn't get the published decision (probably because we
// got a late span just after booting up).
func (sc *SpanCache_basic) GetOldTraceIDs() []string {
	cutoffDuration := 2 * (sc.Cfg.GetTraceTimeout() + sc.Cfg.GetSendDelay())
	cutoffTime := sc.Clock.Now().Add(-cutoffDuration)
	ids := make([]string, 0)

	sc.mut.RLock()
	defer sc.mut.RUnlock()

	for traceID := range sc.cache {
		if sc.cache[traceID].ArrivalTime.Before(cutoffTime) {
			ids = append(ids, traceID)
		}
	}
	return ids
}

func (sc *SpanCache_basic) Remove(traceID string) {
	sc.mut.Lock()
	defer sc.mut.Unlock()
	trace, ok := sc.cache[traceID]
	if !ok {
		return
	}
	sc.Metrics.Down("spancache_traces")
	sc.Metrics.Count("spancache_spans", -int64(trace.DescendantCount()))
	delete(sc.cache, traceID)
}

func (sc *SpanCache_basic) Len() int {
	sc.mut.RLock()
	defer sc.mut.RUnlock()
	return len(sc.cache)
}

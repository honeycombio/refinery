package cache

import (
	"sort"
	"sync"
	"time"

	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/types"
	"github.com/jonboulle/clockwork"
)

// this is a naive implementation that uses a map
// and doesn't try to be clever about memory usage or allocations
type spanCache_basic struct {
	Cfg   config.Config   `inject:""`
	Clock clockwork.Clock `inject:""`
	cache map[string]*types.Trace
	mut   sync.RWMutex

	// current and nextix are used only by the GetTraceIDs method and are not protected
	// by the mutex; they are only accessed by the goroutine that calls GetTraceIDs.
	current []string
	nextix  int
}

// ensure that spanCache implements SpanCache
var _ SpanCache = &spanCache_basic{}

// ensure that spanCache implements startstop.Starter
var _ startstop.Starter = &spanCache_basic{}

func (sc *spanCache_basic) Start() error {
	sc.cache = make(map[string]*types.Trace)
	return nil
}

func (sc *spanCache_basic) GetClock() clockwork.Clock {
	return sc.Clock
}

func (sc *spanCache_basic) Set(sp *types.Span) error {
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
	}
	trace.AddSpan(sp)
	return nil
}

func (sc *spanCache_basic) Get(traceID string) *types.Trace {
	sc.mut.RLock()
	defer sc.mut.RUnlock()
	trace, ok := sc.cache[traceID]
	if !ok {
		return nil
	}
	return trace
}

func (sc *spanCache_basic) GetOldest(fract float64) []string {
	type tidWithImpact struct {
		id     string
		impact int
	}
	n := int(float64(len(sc.cache)) * fract)
	ids := make([]tidWithImpact, 0, len(sc.cache))

	timeout, err := sc.Cfg.GetTraceTimeout()
	if err != nil {
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
// batch of traceIDs to process in parallel. It snapshots the active map and
// returns a slice of traceIDs that were current at the time of the call. It
// will return successive slices of traceIDs until it has returned all of them,
// then it will start over from a fresh snapshot.
// GetTraceIDs is not concurrency-safe; it is intended to be called from a
// single goroutine.
func (sc *spanCache_basic) GetTraceIDs(n int) []string {
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
	return sc.current[sc.nextix : sc.nextix+n]
}

func (sc *spanCache_basic) Remove(traceID string) {
	sc.mut.Lock()
	defer sc.mut.Unlock()
	delete(sc.cache, traceID)
}

func (sc *spanCache_basic) Len() int {
	sc.mut.RLock()
	defer sc.mut.RUnlock()
	return len(sc.cache)
}

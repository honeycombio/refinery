package cache

import (
	"sort"
	"sync"

	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/types"
	"github.com/jonboulle/clockwork"
)

// this is a naive implementation that uses a map
// and doesn't try to be clever about memory usage or allocations
type spanCache_basic struct {
	Clock   clockwork.Clock `inject:""`
	cache   map[string]*types.Trace
	current []string
	nextix  int
	mut     sync.RWMutex
}

// ensure that spanCache implements SpanCache
var _ SpanCache = &spanCache_basic{}

// ensure that spanCache implements startstop.Starter
var _ startstop.Starter = &spanCache_basic{}

// ensure that spanCache implements startstop.Stopper
var _ startstop.Stopper = &spanCache_basic{}

func (sc *spanCache_basic) Start() error {
	sc.cache = make(map[string]*types.Trace)
	return nil
}

func (sc *spanCache_basic) Stop() error {
	sc.cache = nil
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
	n := int(float64(len(sc.cache)) * fract)
	ids := make([]string, 0, len(sc.cache))

	sc.mut.RLock()
	for traceID := range sc.cache {
		ids = append(ids, traceID)
	}
	sort.Slice(ids, func(i, j int) bool {
		t1 := sc.cache[ids[i]]
		t2 := sc.cache[ids[j]]
		return t1.ArrivalTime.Before(t2.ArrivalTime)
	})
	sc.mut.RUnlock()

	if len(ids) < n {
		n = len(ids)
	}
	// truncate the slice to the desired length AND capacity without reallocating
	return ids[:n:n]
}

// This gets a batch of up to n traceIDs from the cache; it's used to get a
// batch of traceIDs to process in parallel. It snapshots the active map and
// returns a slice of traceIDs that were current at the time of the call. It
// will return successive slices of traceIDs until it has returned all of them,
// then it will start over from a fresh snapshot.
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
	return len(sc.cache)
}

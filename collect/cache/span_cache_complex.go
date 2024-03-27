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

// this cache is designed to be performant by pooling the allocated objects and
// reusing them.
// List of spans have capacity
type spanCache_complex struct {
	Cfg       config.Config   `inject:""`
	Clock     clockwork.Clock `inject:""`
	active    map[string]int
	freeSlots []int
	cache     []*types.Trace
	mut       sync.RWMutex

	// current and nextix are used only by the GetTraceIDs method and are not protected
	// by the mutex; they are only accessed by the goroutine that calls GetTraceIDs.
	current []string
	nextix  int
}

// ensure that spanCache implements SpanCache
var _ SpanCache = &spanCache_complex{}

// ensure that spanCache implements startstop.Starter
var _ startstop.Starter = &spanCache_complex{}

func (sc *spanCache_complex) Start() error {
	cfg, err := sc.Cfg.GetCollectionConfig()
	if err != nil {
		return err
	}
	sc.active = make(map[string]int, cfg.CacheCapacity)
	sc.freeSlots = make([]int, 0, cfg.CacheCapacity)
	sc.cache = make([]*types.Trace, 0, cfg.CacheCapacity)
	return nil
}

func (sc *spanCache_complex) GetClock() clockwork.Clock {
	return sc.Clock
}

func (sc *spanCache_complex) Set(sp *types.Span) error {
	var trace *types.Trace
	traceID := sp.TraceID
	sc.mut.Lock()
	defer sc.mut.Unlock()
	index, ok := sc.active[traceID]
	if ok {
		trace = sc.cache[index]
	} else {
		// we don't have a trace for this span yet so we need to set one up
		// see if we have any free slots
		if len(sc.freeSlots) > 0 {
			// we have a free slot, so we can reuse it
			// pop the last element off the freeSlots slice
			index = sc.freeSlots[len(sc.freeSlots)-1]
			sc.freeSlots = sc.freeSlots[:len(sc.freeSlots)-1]
			trace = sc.cache[index]
		} else {
			// we don't have any free slots, so we need to allocate a new slot
			// on the end of the cache
			index = len(sc.cache)
			trace = &types.Trace{}
			sc.cache = append(sc.cache, trace)
		}
		sc.active[traceID] = index
	}
	trace.APIHost = sp.APIHost
	trace.APIKey = sp.APIKey
	trace.Dataset = sp.Dataset
	trace.TraceID = traceID
	trace.ArrivalTime = sc.Clock.Now()
	trace.AddSpan(sp)
	return nil
}

func (sc *spanCache_complex) Get(traceID string) *types.Trace {
	sc.mut.RLock()
	defer sc.mut.RUnlock()
	index, ok := sc.active[traceID]
	if !ok {
		return nil
	}
	return sc.cache[index]
}

// Returns the oldest fraction of traces in the cache. This is used to decide
// which traces to drop when the cache is full. It's moderately expensive
// because it has to sort the traces by arrival time, but I couldn't find a
// faster way to do it.
func (sc *spanCache_complex) GetOldest(fract float64) []string {
	type tidWithImpact struct {
		id     string
		impact int
	}
	n := int(float64(len(sc.active)) * fract)
	ids := make([]tidWithImpact, 0, len(sc.active))

	timeout, err := sc.Cfg.GetTraceTimeout()
	if err != nil {
		timeout = 60 * time.Second
	}

	sc.mut.RLock()
	for _, ix := range sc.active {
		ids = append(ids, tidWithImpact{
			id:     sc.cache[ix].TraceID,
			impact: sc.cache[ix].CacheImpact(timeout),
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
func (sc *spanCache_complex) GetTraceIDs(n int) []string {
	// this is the only function that looks at current or nextix so it
	// doesn't need to lock those fields
	if sc.current == nil || sc.nextix >= len(sc.current) {
		sc.mut.RLock()
		sc.current = make([]string, 0, len(sc.active))
		for traceID := range sc.active {
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

func (sc *spanCache_complex) Remove(traceID string) {
	sc.mut.Lock()
	defer sc.mut.Unlock()
	index, ok := sc.active[traceID]
	if !ok {
		return
	}
	// we don't have to touch the cache itself, just the active map and the freeSlots
	delete(sc.active, traceID)
	sc.freeSlots = append(sc.freeSlots, index)
}

func (sc *spanCache_complex) Len() int {
	sc.mut.RLock()
	defer sc.mut.RUnlock()
	return len(sc.active)
}

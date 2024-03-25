package cache

import (
	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/types"
	"github.com/jonboulle/clockwork"
)

type SpanCache interface {
	Set(span *types.Span) error
	Get(traceID string) (*types.Trace, error)
	GetOldest(n int) []*types.Trace
	Remove(traceID string) error
	Len() int
}

// we're going to start with a naive implementation that uses a map
// and doesn't try to be clever about memory usage or allocations
type spanCache struct {
	Clock clockwork.Clock `inject:""`
	cache map[string]*types.Trace
}

// ensure that spanCache implements SpanCache
var _ SpanCache = &spanCache{}

// ensure that spanCache implements startstop.Starter
var _ startstop.Starter = &spanCache{}

// ensure that spanCache implements startstop.Stopper
var _ startstop.Stopper = &spanCache{}

func (sc *spanCache) Start() error {
	sc.cache = make(map[string]*types.Trace)
	return nil
}

func (sc *spanCache) Stop() error {
	sc.cache = nil
	return nil
}

func (sc *spanCache) Set(sp *types.Span) error {
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

func (sc *spanCache) Get(traceID string) (*types.Trace, error) {
	trace, ok := sc.cache[traceID]
	if !ok {
		return nil, nil
	}
	return trace, nil
}

func (sc *spanCache) GetOldest(n int) []*types.Trace {
	// this is a naive implementation that just returns the first n traces
	// in the cache. in a real implementation, we would want to sort the
	// traces by arrival time and return the oldest n.
	// It's useful for benchmarks because it's probably the fastest way to
	// get n traces.
	traces := make([]*types.Trace, 0, n)
	for _, trace := range sc.cache {
		traces = append(traces, trace)
		if len(traces) == n {
			break
		}
	}
	return traces
}

func (sc *spanCache) Remove(traceID string) error {
	delete(sc.cache, traceID)
	return nil
}

func (sc *spanCache) Len() int {
	return len(sc.cache)
}

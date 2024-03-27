package cache

import (
	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/types"
	"github.com/jonboulle/clockwork"
)

type SpanCache interface {
	Set(span *types.Span) error
	Get(traceID string) (*types.TraceV2, error)
	GetOldest(n int) []*types.TraceV2
	Remove(traceID string) error
	Resize(cacheSize int) SpanCache
	Len() int
}

// we're going to start with a naive implementation that uses a map
// and doesn't try to be clever about memory usage or allocations
type DefaultSpanCache struct {
	Clock clockwork.Clock `inject:""`
	cache map[string]*types.TraceV2
}

// ensure that spanCache implements SpanCache
var _ SpanCache = &DefaultSpanCache{}

// ensure that spanCache implements startstop.Starter
var _ startstop.Starter = &DefaultSpanCache{}

// ensure that spanCache implements startstop.Stopper
var _ startstop.Stopper = &DefaultSpanCache{}

func (sc *DefaultSpanCache) Start() error {
	sc.cache = make(map[string]*types.TraceV2)
	return nil
}

func (sc *DefaultSpanCache) Stop() error {
	sc.cache = nil
	return nil
}

func (sc *DefaultSpanCache) Set(sp *types.Span) error {
	traceID := sp.TraceID
	trace, ok := sc.cache[traceID]
	if !ok {
		trace = &types.TraceV2{
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

func (sc *DefaultSpanCache) Get(traceID string) (*types.TraceV2, error) {
	trace, ok := sc.cache[traceID]
	if !ok {
		return nil, nil
	}
	return trace, nil
}

func (sc *DefaultSpanCache) GetOldest(n int) []*types.TraceV2 {
	// this is a naive implementation that just returns the first n traces
	// in the cache. in a real implementation, we would want to sort the
	// traces by arrival time and return the oldest n.
	// It's useful for benchmarks because it's probably the fastest way to
	// get n traces.
	traces := make([]*types.TraceV2, 0, n)
	for _, trace := range sc.cache {
		traces = append(traces, trace)
		if len(traces) == n {
			break
		}
	}
	return traces
}

func (sc *DefaultSpanCache) Remove(traceID string) error {
	delete(sc.cache, traceID)
	return nil
}

func (sc *DefaultSpanCache) Resize(cacheSize int) SpanCache {
	// TODO: implement resizing
	return sc
}

func (sc *DefaultSpanCache) Len() int {
	return len(sc.cache)
}

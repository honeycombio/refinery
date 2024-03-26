package cache

import (
	"fmt"
	"slices"
	"time"

	"github.com/honeycombio/refinery/types"
)

// mentally map[TraceID]*types.Trace
// priority queue by total cache impact
type CacheV2 interface {
	Set(span *types.Span) *types.TraceV2
	Get(traceID string) *types.TraceV2
	Pop(n int) []*types.TraceV2
	Resize(cacheSize int) CacheV2
}

type ToyCacheV2 struct {
	entries map[string]*types.TraceV2
	cache   []*types.TraceV2
}

func NewToyCacheV2() CacheV2 {
	return &ToyCacheV2{
		entries: make(map[string]*types.TraceV2, 0),
		cache:   make([]*types.TraceV2, 0),
	}
}

func (c *ToyCacheV2) Set(span *types.Span) *types.TraceV2 {
	trace := c.Get(span.TraceID)
	if trace == nil {
		trace = &types.TraceV2{
			TraceID:     span.TraceID,
			ArrivalTime: time.Now(),
			APIHost:     span.APIHost,
			APIKey:      span.APIKey,
			Dataset:     span.Dataset,
		}
		c.cache = append(c.cache, trace)
		c.entries[span.TraceID] = trace
	}
	trace.AddSpan(span)
	return trace
}

func (c *ToyCacheV2) Get(traceID string) *types.TraceV2 {
	return c.entries[traceID]
}

func (c *ToyCacheV2) Pop(n int) []*types.TraceV2 {
	if n > len(c.cache) {
		n = len(c.cache)
	}
	oldest := c.cache[:n]
	fmt.Println("oldest", oldest, n)
	c.cache = slices.Delete(c.cache, 0, n)
	for _, trace := range oldest {
		delete(c.entries, trace.TraceID)
	}
	return oldest
}

func (c *ToyCacheV2) Len() int {
	return len(c.cache)
}

func (c *ToyCacheV2) GetAll() []*types.TraceV2 {
	return c.cache
}

func (c *ToyCacheV2) Resize(cacheSize int) CacheV2 {
	// TODO: implement resizing
	return c
}

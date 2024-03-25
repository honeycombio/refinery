package collect

import (
	"context"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/honeycombio/refinery/centralstore"
	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/generics"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/types"
)

type CentralCollector struct {
	Store   centralstore.SmartStorer `inject:""`
	Config  config.Config            `inject:""`
	Logger  logger.Logger            `inject:""`
	Metrics metrics.Metrics          `inject:"genericMetrics"`

	mutex sync.RWMutex
	cache cache.Cache
	// TODO: this can be a better name
	datasetSamplers map[string]sample.Sampler
	keyFields       generics.Set[string]

	incoming chan *types.Span
	reload   chan struct{}
}

// ensure that we implement the Collector interface
var _ Collector = (*CentralCollector)(nil)

func (c *CentralCollector) Start() error {
	// call reload config and then get the updated unique fields
	collectorCfg, err := c.Config.GetCollectionConfig()
	if err != nil {
		return err
	}

	c.cache = cache.NewInMemCache(collectorCfg.CacheCapacity, c.Metrics, c.Logger)

	// listen for config reloads
	c.Config.RegisterReloadCallback(c.sendReloadSignal)

	c.incoming = make(chan *types.Span, collectorCfg.GetIncomingQueueSize())
	c.reload = make(chan struct{}, 1)
	c.datasetSamplers = make(map[string]sample.Sampler)

	// spin up one collector because this is a single threaded collector
	go c.collect()

	return nil
}

// implement the Collector interface
func (c *CentralCollector) AddSpan(span *types.Span) error {
	// extract all key fields from the span
	// construct a central store span
	// call WriteSpan on the central store

	return nil
}

func (c *CentralCollector) AddSpanFromPeer(span *types.Span) error {
	return nil
}

func (c *CentralCollector) Stressed() bool {
	return false
}

func (c *CentralCollector) GetStressedSampleRate(traceID string) (rate uint, keep bool, reason string) {
	return 0, false, ""
}

func (c *CentralCollector) ProcessSpanImmediately(sp *types.Span, keep bool, sampleRate uint, reason string) {
}

func (c *CentralCollector) collect() {
	tickerDuration := c.Config.GetSendTickerValue()
	ticker := time.NewTicker(tickerDuration)
	defer ticker.Stop()

	// mutex is normally held by this goroutine at all times.
	// It is unlocked once per ticker cycle for tests.
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for {
		select {
		case <-ticker.C:
			c.sendTracesInCache(time.Now())
			c.checkAlloc()

			// Briefly unlock the cache, to allow test access.
			c.mutex.Unlock()
			runtime.Gosched()
			c.mutex.Lock()
		case sp := <-c.incoming:
			c.processSpan(sp)
		case <-c.reload:
			c.reloadConfigs()
		}
	}

}

func (c *CentralCollector) processSpan(sp *types.Span) {
	ctx := context.Background()
	// add the span to the cache
	trace := c.cache.Get(sp.TraceID)
	if trace == nil {
		timeout, err := c.Config.GetTraceTimeout()
		if err != nil {
			timeout = 60 * time.Second
		}

		now := time.Now()
		trace = &types.Trace{
			APIHost:     sp.APIHost,
			APIKey:      sp.APIKey,
			Dataset:     sp.Dataset,
			TraceID:     sp.TraceID,
			ArrivalTime: now,
			SendBy:      now.Add(timeout),
		}
		trace.SetSampleRate(sp.SampleRate) // if it had a sample rate, we want to keep it
		// push this into the cache and if we eject an unsent trace, send it ASAP
		ejectedTrace := c.cache.Set(trace)
		if ejectedTrace != nil {
			// TODO: maybe this is where we need to immediately consult the central store
			// for a decision for this trace
			// immediately transition to ready for decision
			// c.send(ejectedTrace, TraceSendEjectedFull)
		}
	}

	// great! trace is live. add the span.
	trace.AddSpan(sp)

	// if this is a root span, send the trace
	parentID, ok := c.GetParentID(sp)
	if !ok {
		trace.RootSpan = sp
	}

	err := c.Store.WriteSpan(ctx, &centralstore.CentralSpan{
		TraceID:  sp.TraceID,
		SpanID:   sp.ID,
		ParentID: parentID,
	})
	if err != nil {
		c.Logger.Error().WithField("error", err).Logf("Failed to write span to central store")
	}

}

func (c *CentralCollector) sendTracesInCache(now time.Time) {
	traces := c.cache.TakeExpiredTraces(now)
	for _, t := range traces {
		if t.RootSpan != nil {
			c.send(t, TraceSendGotRoot)
		} else {
			c.send(t, TraceSendExpired)
		}
	}
}

func (c *CentralCollector) checkAlloc() {
	inMemConfig, err := c.Config.GetCollectionConfig()
	maxAlloc := inMemConfig.GetMaxAlloc()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	if err != nil || maxAlloc == 0 || mem.Alloc < uint64(maxAlloc) {
		return
	}

	// Figure out what fraction of the total cache we should remove. We'd like it to be
	// enough to get us below the max capacity, but not TOO much below.
	// Because our impact numbers are only the data size, reducing by enough to reach
	// max alloc will actually do more than that.
	totalToRemove := mem.Alloc - uint64(maxAlloc)

	// The size of the cache exceeds the user's intended allocation, so we're going to
	// remove the traces from the cache that have had the most impact on allocation.
	// To do this, we sort the traces by their CacheImpact value and then remove traces
	// until the total size is less than the amount to which we want to shrink.
	existingCache, ok := c.cache.(*cache.DefaultInMemCache)
	if !ok {
		c.Logger.Error().WithField("alloc", mem.Alloc).Logf(
			"total allocation exceeds limit, but unable to control cache",
		)
		return
	}
	allTraces := existingCache.GetAll()
	timeout, err := c.Config.GetTraceTimeout()
	if err != nil {
		timeout = 60 * time.Second
	} // Sort traces by CacheImpact, heaviest first
	sort.Slice(allTraces, func(i, j int) bool {
		return allTraces[i].CacheImpact(timeout) > allTraces[j].CacheImpact(timeout)
	})

	// Now start removing the biggest traces, by summing up DataSize for
	// successive traces until we've crossed the totalToRemove threshold
	// or just run out of traces to delete.

	cap := existingCache.GetCacheSize()

	totalDataSizeSent := 0
	tracesSent := generics.NewSet[string]()
	// Send the traces we can't keep.
	for _, trace := range allTraces {
		tracesSent.Add(trace.TraceID)
		totalDataSizeSent += trace.DataSize
		c.send(trace, TraceSendEjectedMemsize)
		if totalDataSizeSent > int(totalToRemove) {
			break
		}
	}
	existingCache.RemoveTraces(tracesSent)

	// Treat any MaxAlloc overage as an error so we know it's happening
	c.Logger.Error().
		WithField("cache_size", cap).
		WithField("alloc", mem.Alloc).
		WithField("num_traces_sent", len(tracesSent)).
		WithField("datasize_sent", totalDataSizeSent).
		WithField("new_trace_count", existingCache.GetCacheSize()).
		Logf("evicting large traces early due to memory overage")

	// Manually GC here - without this we can easily end up evicting more than we
	// need to, since total alloc won't be updated until after a GC pass.
	runtime.GC()
}

// sendReloadSignal will trigger the collector reloading its config, eventually.
func (c *CentralCollector) sendReloadSignal() {
	// non-blocking insert of the signal here so we don't leak goroutines
	select {
	case c.reload <- struct{}{}:
		c.Logger.Debug().Logf("sending collect reload signal")
	default:
		c.Logger.Debug().Logf("collect already waiting to reload; skipping additional signal")
	}
}

func (c *CentralCollector) reloadConfigs() {
	c.Logger.Debug().Logf("reloading in-mem collect config")
	imcConfig, err := c.Config.GetCollectionConfig()
	if err != nil {
		c.Logger.Error().WithField("error", err).Logf("Failed to reload InMemCollector section when reloading configs")
	}

	if existingCache, ok := c.cache.(*cache.DefaultInMemCache); ok {
		if imcConfig.CacheCapacity != existingCache.GetCacheSize() {
			c.Logger.Debug().WithField("cache_size.previous", existingCache.GetCacheSize()).WithField("cache_size.new", imcConfig.CacheCapacity).Logf("refreshing the cache because it changed size")
			inMemCache := cache.NewInMemCache(imcConfig.CacheCapacity, c.Metrics, c.Logger)
			// pull the old cache contents into the new cache
			for j, trace := range existingCache.GetAll() {
				if j >= imcConfig.CacheCapacity {
					c.send(trace, TraceSendEjectedFull)
					continue
				}
				inMemCache.Set(trace)
			}
			c.cache = inMemCache
		} else {
			c.Logger.Debug().Logf("skipping reloading the in-memory cache on config reload because it hasn't changed capacity")
		}

		// reload the cache size in smart store?
		// c.SampleTraceCache.Resize(i.Config.GetSampleCacheConfig())
	} else {
		c.Logger.Error().WithField("cache", c.cache.(*cache.DefaultInMemCache)).Logf("skipping reloading the cache on config reload because it's not an in-memory cache")
	}

	//c.StressRelief.UpdateFromConfig(i.Config.GetStressReliefConfig())

	// clear out any samplers that we have previously created
	// so that the new configuration will be propagated
	c.datasetSamplers = make(map[string]sample.Sampler)
	// TODO add resizing the LRU sent trace cache on config reload
}

func (c *CentralCollector) send(sp *types.Trace, reason string) {
	// TODO: implement getting a decision from the central store
}

func (c *CentralCollector) GetParentID(sp *types.Span) (string, bool) {
	for _, parentIdFieldName := range c.Config.GetParentIdFieldNames() {
		parentId := sp.Data[parentIdFieldName]
		if v, ok := parentId.(string); ok {
			return v, true
		}
	}

	return "", false
}

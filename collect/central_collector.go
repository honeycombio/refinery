package collect

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/honeycombio/refinery/centralstore"
	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const cacheEjectBatchSize = 100

type CentralCollector struct {
	Store          centralstore.SmartStorer `inject:""`
	Config         config.Config            `inject:""`
	Clock          clockwork.Clock          `inject:""`
	Transmission   transmit.Transmission    `inject:"upstreamTransmission"`
	Logger         logger.Logger            `inject:""`
	Metrics        metrics.Metrics          `inject:"genericMetrics"`
	SamplerFactory *sample.SamplerFactory   `inject:""`

	// For test use only
	BlockOnAddSpan bool

	mutex sync.RWMutex
	cache cache.SpanCache
	// TODO: this can be a better name
	datasetSamplers map[string]sample.Sampler

	incoming chan *types.Span
	reload   chan struct{}

	eg *errgroup.Group
}

// ensure that we implement the Collector interface
var _ Collector = (*CentralCollector)(nil)

func (c *CentralCollector) Start() error {
	// call reload config and then get the updated unique fields
	collectorCfg, err := c.Config.GetCollectionConfig()
	if err != nil {
		return err
	}

	// TODO: use the actual new cache implementation
	spanCache := &cache.DefaultSpanCache{
		Clock: c.Clock,
	}
	spanCache.Start()
	c.cache = spanCache

	// listen for config reloads
	c.Config.RegisterReloadCallback(c.sendReloadSignal)

	c.incoming = make(chan *types.Span, collectorCfg.GetIncomingQueueSize())
	c.reload = make(chan struct{}, 1)
	c.datasetSamplers = make(map[string]sample.Sampler)

	// spin up one collector because this is a single threaded collector
	c.eg = &errgroup.Group{}
	c.eg.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic in central collector: %v\n%s", r, debug.Stack())
			}
		}()

		c.collect()
		return nil
	})

	return nil
}

func (c *CentralCollector) Stop() error {
	close(c.incoming)
	close(c.reload)
	return c.eg.Wait()
}

// implement the Collector interface
func (c *CentralCollector) AddSpan(span *types.Span) error {
	return c.add(span, c.incoming)
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

func (c *CentralCollector) add(sp *types.Span, ch chan<- *types.Span) error {
	if c.BlockOnAddSpan {
		ch <- sp
		c.Metrics.Increment("span_received")
		c.Metrics.Up("spans_waiting")
		return nil
	}

	select {
	case ch <- sp:
		c.Metrics.Increment("span_received")
		c.Metrics.Up("spans_waiting")
		return nil
	default:
		return ErrWouldBlock
	}
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
			c.sendTracesInCache()
			c.checkAlloc()

			// Briefly unlock the cache, to allow test access.
			c.mutex.Unlock()
			runtime.Gosched()
			c.mutex.Lock()
		case sp, ok := <-c.incoming:
			if !ok {
				return
			}
			c.processSpan(sp)
		case <-c.reload:
			c.reloadConfigs()
		}
	}

}

func (c *CentralCollector) processSpan(sp *types.Span) {
	// add the span to the cache
	trace, err := c.cache.Get(sp.TraceID)
	if err != nil {
		c.Logger.Error().Logf("failed to get trace: %s from cache ", sp.TraceID)
		return
	}
	if trace == nil {
		err = c.cache.Set(sp)
		if err != nil {
			c.Logger.Error().Logf("failed to set trace: %s from cache ", sp.TraceID)
			return
		}
	}

	trace, err = c.cache.Get(sp.TraceID)
	if err != nil {
		c.Logger.Error().Logf("failed to get trace: %s from cache ", sp.TraceID)
		return
	}

	// construct a central store span
	cs := &centralstore.CentralSpan{
		TraceID: sp.TraceID,
		SpanID:  sp.ID,
	}
	parentID, ok := c.GetParentID(sp)
	if !ok {
		cs.IsRoot = true
	}
	cs.ParentID = parentID

	samplerKey, isLegacyKey := trace.GetSamplerKey()
	logFields := logrus.Fields{
		"trace_id": trace.TraceID,
	}
	if isLegacyKey {
		logFields["dataset"] = samplerKey
	} else {
		logFields["environment"] = samplerKey
	}
	sampler, found := c.datasetSamplers[samplerKey]
	if !found {
		sampler = c.SamplerFactory.GetSamplerImplementationForKey(samplerKey, isLegacyKey)
		c.datasetSamplers[samplerKey] = sampler
	}
	// extract all key fields from the span
	keyFields := sampler.GetKeyFields()
	for _, keyField := range keyFields {
		if val, ok := sp.Data[keyField]; ok {
			cs.KeyFields[keyField] = val
		}
	}

	// send the span to the central store
	ctx := context.Background()
	err = c.Store.WriteSpan(ctx, cs)
	if err != nil {
		c.Logger.Error().WithFields(logFields).Logf("error writing span to central store: %s", err)
	}

}

func (c *CentralCollector) sendTracesInCache() {
	ctx := context.Background()
	traces := c.cache.GetOldest(cacheEjectBatchSize)
	for _, t := range traces {
		c.Store.WriteSpan(ctx, &centralstore.CentralSpan{
			TraceID: t.TraceID,
		})
		c.cache.Remove(t.TraceID)
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

	// TODO: implement cache eviction here

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

	if imcConfig.CacheCapacity != c.cache.Len() {
		c.Logger.Debug().WithField("cache_size.previous", c.cache.Len()).WithField("cache_size.new", imcConfig.CacheCapacity).Logf("refreshing the cache because it changed size")
		c.cache = c.cache.Resize(imcConfig.CacheCapacity)
	} else {
		c.Logger.Debug().Logf("skipping reloading the in-memory cache on config reload because it hasn't changed capacity")
	}

	// reload the cache size in smart store?
	// c.SampleTraceCache.Resize(i.Config.GetSampleCacheConfig())

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

// Convenience method for tests.
func (c *CentralCollector) getFromCache(traceID string) *types.TraceV2 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	trace, err := c.cache.Get(traceID)
	if err != nil {
		return nil
	}

	return trace
}

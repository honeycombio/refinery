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

const (
	cacheEjectBatchSize = 100
)

type CentralCollector struct {
	Store          centralstore.SmartStorer `inject:""`
	Config         config.Config            `inject:""`
	Clock          clockwork.Clock          `inject:""`
	Transmission   transmit.Transmission    `inject:"upstreamTransmission"`
	Logger         logger.Logger            `inject:""`
	Metrics        metrics.Metrics          `inject:"genericMetrics"`
	SamplerFactory *sample.SamplerFactory   `inject:""`
	SpanCache      cache.SpanCache          `inject:""`

	mut                   sync.RWMutex
	samplersByDestination map[string]sample.Sampler

	incoming chan *types.Span
	reload   chan struct{}

	eg *errgroup.Group
}

func (c *CentralCollector) Start() error {
	// call reload config and then get the updated unique fields
	collectorCfg := c.Config.GetCollectionConfig()

	// listen for config reloads
	c.Config.RegisterReloadCallback(c.sendReloadSignal)

	c.incoming = make(chan *types.Span, collectorCfg.GetIncomingQueueSize())
	c.reload = make(chan struct{}, 1)
	c.samplersByDestination = make(map[string]sample.Sampler)

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

func (c *CentralCollector) add(sp *types.Span, ch chan<- *types.Span) error {
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

	for {
		select {
		case <-ticker.C:
			c.sendTracesForDecision()
			c.checkAlloc()

		case sp, ok := <-c.incoming:
			if !ok {
				return
			}
			c.processSpan(sp)
		case <-c.reload:
			// reload config
		}
	}

}

func (c *CentralCollector) processSpan(sp *types.Span) {
	// add the span to the cache
	trace := c.SpanCache.Get(sp.TraceID)
	if trace == nil {
		err := c.SpanCache.Set(sp)
		if err != nil {
			c.Logger.Error().Logf("error adding span with trace ID %s to cache: %s", sp.TraceID, err)
			return
		}
	}

	trace = c.SpanCache.Get(sp.TraceID)

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

	c.mut.RLock()
	sampler, found := c.samplersByDestination[samplerKey]
	c.mut.RUnlock()
	if !found {
		sampler = c.SamplerFactory.GetSamplerImplementationForKey(samplerKey, isLegacyKey)
		c.mut.Lock()
		c.samplersByDestination[samplerKey] = sampler
		c.mut.Unlock()
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
	err := c.Store.WriteSpan(ctx, cs)
	if err != nil {
		c.Logger.Error().WithFields(logFields).Logf("error writing span to central store: %s", err)
	}

}

func (c *CentralCollector) sendTracesForDecision() {
	ctx := context.Background()
	traces := c.SpanCache.GetOldest(cacheEjectBatchSize)
	for _, t := range traces {
		err := c.Store.WriteSpan(ctx, &centralstore.CentralSpan{
			TraceID: t,
		})
		if err != nil {
			c.Logger.Error().Logf("error trigger decision making process for trace %s: %s", t, err)
		}
	}
}

func (c *CentralCollector) checkAlloc() {
	inMemConfig := c.Config.GetCollectionConfig()
	maxAlloc := inMemConfig.GetMaxAlloc()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	if maxAlloc == 0 || mem.Alloc < uint64(maxAlloc) {
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

func (c *CentralCollector) GetParentID(sp *types.Span) (string, bool) {
	for _, parentIdFieldName := range c.Config.GetParentIdFieldNames() {
		parentId := sp.Data[parentIdFieldName]
		if v, ok := parentId.(string); ok {
			return v, true
		}
	}

	return "", false
}

func (c *CentralCollector) Stressed() bool {
	return false
}

func (c *CentralCollector) GetStressedSampleRate(traceID string) (rate uint, keep bool, reason string) {
	return 0, false, ""
}

func (c *CentralCollector) ProcessSpanImmediately(sp *types.Span, keep bool, sampleRate uint, reason string) {
}

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
	// TODO: these should be configurable
	cacheEjectBatchSize          = 100
	processTracesBatchSize       = 100
	processTracesBackoffInterval = 200 * time.Microsecond
	deciderBackoffInterval       = 100 * time.Microsecond
	deciderBatchSize             = 100
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

	done chan struct{}
	eg   *errgroup.Group

	hostname string

	// test hooks
	BlockOnDecider bool
}

func (c *CentralCollector) Start() error {
	// call reload config and then get the updated unique fields
	collectorCfg := c.Config.GetCollectionConfig()

	c.done = make(chan struct{})

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

	c.eg.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic in central collector: %v\n%s", r, debug.Stack())
			}
		}()

		return c.processTraces()
	})

	c.eg.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic in central collector: %v\n%s", r, debug.Stack())
			}
		}()

		return c.decide()
	})

	return nil
}

func (c *CentralCollector) Stop() error {
	close(c.done)
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
	ticker := c.Clock.NewTicker(tickerDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.Chan():
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

func (c *CentralCollector) processTraces() error {
	for {
		select {
		case <-c.done:
			return nil
		default:
			ids := c.SpanCache.GetTraceIDs(processTracesBatchSize)
			if len(ids) == 0 {
				continue
			}

			statuses, err := c.Store.GetStatusForTraces(context.Background(), ids)
			if err != nil {
				c.Logger.Error().Logf("error getting statuses for traces: %s", err)
				continue
			}
			for _, status := range statuses {
				switch status.State {
				case centralstore.DecisionKeep:
					c.send(status)
					c.SpanCache.Remove(status.TraceID)

				case centralstore.DecisionDrop:
					c.SpanCache.Remove(status.TraceID)
				default:
					c.Logger.Debug().Logf("trace %s is still pending", status.TraceID)
				}
			}
		}

		select {
		case <-c.done:
			return nil
		default:
		}

		ticker := c.Clock.NewTicker(processTracesBackoffInterval)
		select {
		case <-c.done:
			return nil
		case <-ticker.Chan():
			ticker.Stop()
			continue
		}
	}
}

func (c *CentralCollector) decide() error {
	for {
		select {
		case <-c.done:
			return nil
		default:
			if c.BlockOnDecider {
				continue
			}
			ctx := context.Background()
			tracesIDs, err := c.Store.GetTracesNeedingDecision(ctx, deciderBatchSize)
			if err != nil {
				c.Logger.Error().Logf("error getting traces needing decision: %s", err)
				continue
			}

			if len(tracesIDs) == 0 {
				continue
			}
			statuses, err := c.Store.GetStatusForTraces(ctx, tracesIDs)
			if err != nil {
				c.Logger.Error().Logf("error getting statuses for traces: %s", err)
				continue
			}
			traces := make([]*centralstore.CentralTrace, len(statuses))
			stateMap := make(map[string]*centralstore.CentralTraceStatus, len(statuses))
			var wg sync.WaitGroup
			for i, status := range statuses {
				// make a decision on each trace
				if status.State != centralstore.AwaitingDecision {
					// someone else got to it first
					continue
				}
				stateMap[status.TraceID] = status

				wg.Add(1)
				go func(status *centralstore.CentralTraceStatus, idx int) {
					defer wg.Done()

					trace, err := c.Store.GetTrace(ctx, status.TraceID)
					if err != nil {
						c.Logger.Error().Logf("error getting trace %s: %s", status.TraceID, err)
						return
					}
					traces[idx] = trace
				}(status, i)
			}
			wg.Wait()

			for _, trace := range traces {
				if trace == nil {
					continue
				}

				if trace.Root != nil {
					c.Metrics.Increment("trace_decision_has_root")
				} else {
					c.Metrics.Increment("trace_decision_no_root")
				}

				var sampler sample.Sampler
				var found bool

				tr := c.SpanCache.Get(trace.TraceID)
				if tr == nil {
					// This refinery instance does not have the trace in cache
					// so we can't make a decision on it.
					continue
				}

				// get sampler key (dataset for legacy keys, environment for new keys)
				samplerKey, isLegacyKey := tr.GetSamplerKey()
				logFields := logrus.Fields{
					"trace_id": trace.TraceID,
				}
				if isLegacyKey {
					logFields["dataset"] = samplerKey
				} else {
					logFields["environment"] = samplerKey
				}

				// use sampler key to find sampler; create and cache if not found
				if sampler, found = c.samplersByDestination[samplerKey]; !found {
					sampler = c.SamplerFactory.GetSamplerImplementationForKey(samplerKey, isLegacyKey)
					c.samplersByDestination[samplerKey] = sampler
				}

				// make sampling decision and update the trace
				rate, shouldSend, reason, key := sampler.GetSampleRate(tr)
				tr.SetSampleRate(rate)
				logFields["reason"] = reason
				if key != "" {
					logFields["sample_key"] = key
				}
				// This will observe sample rate attempts even if the trace is dropped
				c.Metrics.Histogram("trace_aggregate_sample_rate", float64(rate))

				// if we're supposed to drop this trace, and dry run mode is not enabled, then we're done.
				if !shouldSend && !c.Config.GetIsDryRun() {
					c.Metrics.Increment("trace_decision_dropped")
					c.Logger.Info().WithFields(logFields).Logf("Dropping trace because of sampling")
				}
				c.Metrics.Increment("trace_decision_kept")
				// This will observe sample rate decisions only if the trace is kept
				c.Metrics.Histogram("trace_kept_sample_rate", float64(rate))

				// ok, we're not dropping this trace; send all the spans
				if c.Config.GetIsDryRun() && !shouldSend {
					c.Logger.Info().WithFields(logFields).Logf("Trace would have been dropped, but dry run mode is enabled")
				}

				// These meta data should be stored on the central trace status object
				// so that it's synced across all refinery instances
				status := stateMap[trace.TraceID]
				if c.Config.GetAddRuleReasonToTrace() {
					status.Metadata["meta.refinery.reason"] = reason
					if status.Metadata["meta.refinery.send_reason"] == "" {
						sendReason := TraceSendExpired
						if trace.Root != nil {
							sendReason = TraceSendGotRoot
						}
						status.Metadata["meta.refinery.send_reason"] = sendReason
					}
					if key != "" {
						status.Metadata["meta.refinery.sample_key"] = key
					}
				}

				if c.hostname != "" {
					status.Metadata["meta.refinery.decider.local_hostname"] = c.hostname
				}

				var state centralstore.CentralTraceState
				if shouldSend {
					state = centralstore.DecisionKeep
				} else {
					state = centralstore.DecisionDrop
				}
				status.State = state
				stateMap[status.TraceID] = status
			}

			updatedStatuses := make([]*centralstore.CentralTraceStatus, 0, len(stateMap))
			for _, status := range stateMap {
				if status == nil {
					continue
				}
				updatedStatuses = append(updatedStatuses, status)
			}

			err = c.Store.SetTraceStatuses(ctx, updatedStatuses)
			if err != nil {
				c.Logger.Error().Logf("error setting trace statuses: %s", err)
			}
		}

		select {
		case <-c.done:
			return nil
		default:
		}

		timer := c.Clock.NewTicker(deciderBackoffInterval)
		select {
		case <-c.done:
			timer.Stop()
			return nil
		case <-timer.Chan():
			timer.Stop()
			continue
		}
	}
}

func (c *CentralCollector) processSpan(sp *types.Span) {
	err := c.SpanCache.Set(sp)
	if err != nil {
		c.Logger.Error().Logf("error adding span with trace ID %s to cache: %s", sp.TraceID, err)
		return
	}

	trace := c.SpanCache.Get(sp.TraceID)

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
	err = c.Store.WriteSpan(ctx, cs)
	if err != nil {
		c.Logger.Error().WithFields(logFields).Logf("error writing span to central store: %s", err)
	}

}

func (c *CentralCollector) sendTracesForDecision() {
	ctx := context.Background()
	traces := c.SpanCache.GetOldest(cacheEjectBatchSize)
	for _, t := range traces {
		// TODO: we should add the metadata about this trace
		// is sent for decision due to cache ejection
		// to the trace status object
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

func (c *CentralCollector) send(status *centralstore.CentralTraceStatus) {
	trace := c.SpanCache.Get(status.TraceID)
	if trace == nil {
		return
	}
	traceDur := time.Since(trace.ArrivalTime)
	c.Metrics.Histogram("trace_duration_ms", float64(traceDur.Milliseconds()))
	c.Metrics.Histogram("trace_span_count", float64(status.DescendantCount()))

	c.Metrics.Increment(status.KeepReason)

	// get sampler key (dataset for legacy keys, environment for new keys)
	samplerKey, isLegacyKey := trace.GetSamplerKey()
	logFields := logrus.Fields{
		"trace_id": trace.TraceID,
	}
	if isLegacyKey {
		logFields["dataset"] = samplerKey
	} else {
		logFields["environment"] = samplerKey
	}

	// If we have a root span, update it with the count before determining the SampleRate.
	if trace.RootSpan != nil {
		rs := trace.RootSpan
		rs.Data["meta.span_event_count"] = int64(status.SpanEventCount())
		rs.Data["meta.span_link_count"] = int64(status.SpanLinkCount())
		rs.Data["meta.span_count"] = int64(status.SpanCount())
		rs.Data["meta.event_count"] = int64(status.DescendantCount())
	}

	logFields["reason"] = status.KeepReason

	c.Metrics.Increment("trace_send_kept")
	// This will observe sample rate decisions only if the trace is kept
	c.Metrics.Histogram("trace_kept_sample_rate", float64(status.Rate))

	c.Logger.Info().WithFields(logFields).Logf("Sending trace")
	for _, sp := range trace.GetSpans() {
		if c.Config.GetAddRuleReasonToTrace() {
			sp.Data["meta.refinery.reason"] = status.Metadata["meta.refinery.reason"]
			sp.Data["meta.refinery.send_reason"] = status.Metadata["meta.refinery.send_reason"]
			sp.Data["meta.refinery.sample_key"] = status.Metadata["meta.refinery.sample_key"]
		}

		if c.hostname != "" {
			sp.Data["meta.refinery.sender.local_hostname"] = c.hostname
		}
		mergeTraceAndSpanSampleRates(sp, status.SampleRate(), false)
		c.addAdditionalAttributes(sp)
		c.Transmission.EnqueueSpan(sp)
	}
}

func (c *CentralCollector) addAdditionalAttributes(sp *types.Span) {
	for k, v := range c.Config.GetAdditionalAttributes() {
		sp.Data[k] = v
	}
}

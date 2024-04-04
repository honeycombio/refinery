package collect

import (
	"context"
	"runtime"
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
)

const (
	// TODO: these should be configurable
	cacheEjectBatchSize        = 100
	processTracesBatchSize     = 100
	processTracesPauseDuration = 200 * time.Microsecond
	deciderPauseDuration       = 100 * time.Microsecond
	deciderBatchSize           = 100
	retryLimit                 = 5
)

type traceForDecision struct {
	*centralstore.CentralTrace
	descendantCount uint32
}

func (t *traceForDecision) DescendantCount() uint32 {
	return t.descendantCount
}

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

	done    chan struct{}
	limiter *limiter

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
	c.limiter = newLimiter(retryLimit)
	c.limiter.Go(func() {
		err := catchPanic(c.collect)
		if err != nil {
			c.Logger.Error().Logf("error collecting spans: %s", err)
		}
	})

	c.limiter.Go(func() {
		err := catchPanic(c.processTraces)
		if err != nil {
			c.Logger.Error().Logf("error processing traces: %s", err)
		}
	})

	c.limiter.Go(func() {
		err := catchPanic(c.decide)
		if err != nil {
			c.Logger.Error().Logf("error making decision for traces: %s", err)
		}
	})

	return nil
}

func (c *CentralCollector) Stop() error {
	close(c.done)
	close(c.incoming)
	close(c.reload)
	c.limiter.Close()
	return nil
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

func (c *CentralCollector) processTraces() {
	for {
		select {
		case <-c.done:
			return
		default:
			ids := c.SpanCache.GetTraceIDs(processTracesBatchSize)
			c.Metrics.Histogram("collector_processor_batch_count", len(ids))
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
			return
		default:
		}

		ticker := c.Clock.NewTicker(processTracesPauseDuration)
		select {
		case <-c.done:
			return
		case <-ticker.Chan():
			ticker.Stop()
			continue
		}
	}
}

func (c *CentralCollector) decide() {
	for {
		select {
		case <-c.done:
			return
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

				// use sampler key to find sampler; create and cache if not found
				if sampler, found = c.samplersByDestination[samplerKey]; !found {
					sampler = c.SamplerFactory.GetSamplerImplementationForKey(samplerKey, isLegacyKey)
					c.samplersByDestination[samplerKey] = sampler
				}

				status, ok := stateMap[trace.TraceID]
				if !ok {
					c.Logger.Error().Logf("trace %s not found in state map", trace.TraceID)
					continue
				}

				tr := &traceForDecision{
					CentralTrace:    trace,
					descendantCount: status.DescendantCount(),
				}

				// make sampling decision and update the trace
				rate, shouldSend, reason, key := sampler.GetSampleRate(tr)
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
			return
		default:
		}

		timer := c.Clock.NewTicker(deciderPauseDuration)
		select {
		case <-c.done:
			timer.Stop()
			return
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
		TraceID:   sp.TraceID,
		SpanID:    sp.ID,
		KeyFields: make(map[string]interface{}),
	}
	parentID, ok := c.GetParentID(sp)
	if !ok {
		cs.IsRoot = true
	}
	cs.ParentID = parentID

	samplerKey, isLegacyKey := trace.GetSamplerKey()
	if samplerKey != "" {
		cs.KeyFields["sampler_key"] = samplerKey
		cs.KeyFields["is_legacy_key"] = isLegacyKey
	}

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

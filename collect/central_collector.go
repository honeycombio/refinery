package collect

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/honeycombio/refinery/centralstore"
	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type Collector interface {
	// AddSpan adds a span to be collected, buffered, and merged into a trace.
	// Once the trace is "complete", it'll be passed off to the sampler then
	// scheduled for transmission.
	AddSpan(*types.Span) error
	Stressed() bool
}

func GetCollectorImplementation(c config.Config) Collector {
	return &CentralCollector{}
}

var ErrWouldBlock = errors.New("not adding span, channel buffer is full")

// These are the names of the metrics we use to track our send decisions.
const (
	TraceSendGotRoot        = "trace_send_got_root"
	TraceSendExpired        = "trace_send_expired"
	TraceSendEjectedFull    = "trace_send_ejected_full"
	TraceSendEjectedMemsize = "trace_send_ejected_memsize"
	TraceSendLateSpan       = "trace_send_late_span"

	metricsCycleInterval = 1 * time.Second
)

type traceForDecision struct {
	*centralstore.CentralTrace
	descendantCount uint32
}

func (t *traceForDecision) DescendantCount() uint32 {
	return t.descendantCount
}

var _ Collector = &CentralCollector{}

type CentralCollector struct {
	Store          centralstore.SmartStorer `inject:""`
	Config         config.Config            `inject:""`
	Clock          clockwork.Clock          `inject:""`
	Transmission   transmit.Transmission    `inject:"upstreamTransmission"`
	Logger         logger.Logger            `inject:""`
	Metrics        metrics.Metrics          `inject:"genericMetrics"`
	StressRelief   StressReliever           `inject:"stressRelief"`
	SamplerFactory *sample.SamplerFactory   `inject:""`
	SpanCache      cache.SpanCache          `inject:""`
	Health         health.Recorder          `inject:""`

	// whenever samplersByDestination is accessed, it should be protected by
	// the mut mutex
	mut                   sync.RWMutex
	samplersByDestination map[string]sample.Sampler

	incoming chan *types.Span
	reload   chan struct{}

	done           chan struct{}
	eg             *errgroup.Group
	processorCycle *Cycle
	deciderCycle   *Cycle
	metricsCycle   *Cycle

	hostname string

	// test hooks
	blockOnCollect bool
	isTest         bool
}

const (
	receiverHealth  = "receiver"
	deciderHealth   = "decider"
	processorHealth = "processor"
)

func (c *CentralCollector) Start() error {
	// call reload config and then get the updated unique fields
	collectorCfg := c.Config.GetCollectionConfig()

	// we're a health check reporter so register ourselves for each of our major routines
	c.Health.Register(receiverHealth, 2*c.Config.GetSendTickerValue())
	c.Health.Register(deciderHealth, 2*collectorCfg.GetDeciderPauseDuration())
	c.Health.Register(processorHealth, 2*collectorCfg.GetProcessTracesPauseDuration())

	c.done = make(chan struct{})

	// listen for config reloads
	c.Config.RegisterReloadCallback(c.sendReloadSignal)

	c.incoming = make(chan *types.Span, collectorCfg.GetIncomingQueueSize())
	c.reload = make(chan struct{}, 1)
	c.samplersByDestination = make(map[string]sample.Sampler)

	// test hooks
	c.metricsCycle = NewCycle(c.Clock, c.Config.GetSendTickerValue(), c.done)
	c.processorCycle = NewCycle(c.Clock, collectorCfg.GetProcessTracesPauseDuration(), c.done)
	c.deciderCycle = NewCycle(c.Clock, collectorCfg.GetDeciderPauseDuration(), c.done)

	c.Metrics.Register("collector_processor_batch_count", "histogram")
	c.Metrics.Register("collector_decider_batch_count", "histogram")
	c.Metrics.Register("trace_send_kept", "counter")
	c.Metrics.Register("trace_duration_ms", "histogram")
	c.Metrics.Register("trace_span_count", "histogram")
	c.Metrics.Register("trace_decision_kept", "counter")
	c.Metrics.Register("trace_decision_dropped", "counter")
	c.Metrics.Register("trace_decision_has_root", "counter")
	c.Metrics.Register("trace_decision_no_root", "counter")
	c.Metrics.Register("collector_incoming_queue", "histogram")
	c.Metrics.Register("collector_incoming_queue_length", "gauge")
	c.Metrics.Register("collector_cache_size", "gauge")
	c.Metrics.Register("memory_heap_allocation", "gauge")
	c.Metrics.Register("span_received", "counter")
	c.Metrics.Register("span_processed", "counter")
	c.Metrics.Register("spans_waiting", "updown")

	if c.Config.GetAddHostMetadataToTrace() {
		if hostname, err := os.Hostname(); err == nil && hostname != "" {
			c.hostname = hostname
		}
	}
	c.Metrics.Store("INCOMING_CAP", float64(cap(c.incoming)))

	// spin up one collector because this is a single threaded collector
	c.eg = &errgroup.Group{}
	c.eg.Go(c.receive)
	c.eg.Go(c.process)
	c.eg.Go(c.decide)
	c.eg.Go(func() error {
		return c.metricsCycle.Run(context.Background(), func(ctx context.Context) error {
			if err := c.Store.RecordMetrics(ctx); err != nil {
				c.Logger.Error().Logf("error recording metrics: %s", err)
			}

			return nil
		})
	})
	return nil
}

// Stop will be called when the refinery is shutting down.
func (c *CentralCollector) Stop() error {
	close(c.done)
	close(c.incoming)
	close(c.reload)

	if err := c.eg.Wait(); err != nil {
		c.Logger.Error().Logf("error waiting for goroutines to finish: %s", err)
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, c.Config.GetCollectionConfig().GetShutdownDelay())
	defer cancel()

	if err := c.shutdown(ctx); err != nil {
		c.Logger.Error().Logf("error shutting down collector: %s", err)
	}

	return nil
}

// shutdown implements the shutdown logic for the collector.
// It starts a new processor cycle with a shorter interval to monitor
// trace decisions made for the remaining traces in the cache.
//
// After the processor cycle is finished, it also uploads all the
// remaining traces in the cache to the central store.
//
// The shutdown process is expected to finish within the shutdown delay.
// Half of the shutdown delay is used for the processor cycle and the
// other half is used for uploading the remaining traces.
// If the shutdown process exceeds the shutdown delay, it will return an error.
func (c *CentralCollector) shutdown(ctx context.Context) error {
	// try to send the remaining traces to the cache
	interval := 1 * time.Second
	done := make(chan struct{})
	processCycle := NewCycle(c.Clock, interval, done)

	// create a new context with a deadline that's half of the shutdown delay for the processor cycle
	processCtx, cancel := context.WithTimeout(ctx, c.Config.GetCollectionConfig().GetShutdownDelay()/2)
	defer cancel()

	if err := processCycle.Run(processCtx, func(ctx context.Context) error {
		err := c.processTraces(ctx)
		if err != nil {
			c.Logger.Error().Logf("during shutdown - error processing remaining traces: %s", err)
			if c.isTest {
				return err
			}
		}
		// we have to make sure the health check says we're alive but not accepting data during shutdown
		c.Health.Ready(receiverHealth, false)
		c.Health.Ready(deciderHealth, false)
		c.Health.Ready(processorHealth, true)
		return nil
	}); err != nil {
		// this is expected to happen whenever long traces haven't finished during shutdown;
		// log it, but it's not an error
		if errors.Is(err, context.DeadlineExceeded) {
			c.Logger.Info().Logf("traces did not drain in time for shutdown -- forwarding remaining spans")
		} else {
			c.Logger.Error().Logf("during shutdown: context error processing remaining traces: %s", err)
		}
	}
	defer close(done)

	// send the remaining traces to the central store
	ids := c.SpanCache.GetTraceIDs(c.SpanCache.Len())
	var sentCount int
	defer c.Logger.Info().Logf("sending %d traces to central store during shutdown", sentCount)

	for _, id := range ids {
		trace := c.SpanCache.Get(id)

		for _, sp := range trace.GetSpans() {
			// send the spans to the central store
			cs := &centralstore.CentralSpan{
				TraceID:   id,
				SpanID:    sp.ID,
				Type:      sp.Type(),
				AllFields: sp.Data,
				IsRoot:    sp.IsRoot,
			}

			cs.SetSamplerSelector(trace.GetSamplerSelector(c.Config.GetDatasetPrefix()))
			err := c.Store.WriteSpan(ctx, cs)
			if err != nil {
				c.Logger.Error().Logf("error sending span %s for trace %s during shutdown: %s", sp.ID, id, err)
			}
			sentCount++
			// if the context deadline is exceeded, that means we are
			// about to exceed the shutdown delay, so we should stop
			// sending traces to the central store. Unfortunately, the
			// remaining traces will be lost.
			if errors.Is(err, context.DeadlineExceeded) {
				return err
			}
		}
	}

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

func (c *CentralCollector) receive() error {
	tickerDuration := c.Config.GetSendTickerValue()
	ticker := c.Clock.NewTicker(tickerDuration)
	defer ticker.Stop()

	if c.blockOnCollect {
		return nil
	}

	for {
		// record channel lengths as histogram but also as gauges
		c.Metrics.Histogram("collector_incoming_queue", float64(len(c.incoming)))
		c.Metrics.Gauge("collector_incoming_queue_length", float64(len(c.incoming)))
		c.Health.Ready(receiverHealth, true)

		select {
		case <-c.done:
			return nil
		case <-ticker.Chan():
			c.checkAlloc()

		case sp, ok := <-c.incoming:
			if !ok {
				return nil
			}
			err := c.processSpan(sp)
			if err != nil {
				c.Logger.Error().Logf("error processing span: %s", err)
			}
		case <-c.reload:
			c.reloadConfig()
			// reload config
		}
	}

}

func (c *CentralCollector) process() error {
	return c.processorCycle.Run(context.Background(), func(ctx context.Context) error {
		err := c.processTraces(ctx)
		if err != nil {
			c.Logger.Error().Logf("error processing traces: %s", err)
			if c.isTest {
				return err
			}
		}
		c.Health.Ready(processorHealth, true)

		return nil
	})
}

func (c *CentralCollector) processTraces(ctx context.Context) error {
	ids := c.SpanCache.GetTraceIDs(c.Config.GetCollectionConfig().GetProcessTracesBatchSize())

	c.Metrics.Histogram("collector_processor_batch_count", len(ids))
	if len(ids) == 0 {
		return nil
	}

	statuses, err := c.Store.GetStatusForTraces(ctx, ids)
	if err != nil {
		return err
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

	return nil
}

func (c *CentralCollector) decide() error {
	return c.deciderCycle.Run(context.Background(), func(ctx context.Context) error {
		err := c.makeDecision(ctx)
		if err != nil {
			c.Logger.Error().Logf("error making decision: %s", err)
			if c.isTest {
				return err
			}
		}
		c.Health.Ready(deciderHealth, true)

		return nil
	})
}

func (c *CentralCollector) makeDecision(ctx context.Context) error {
	tracesIDs, err := c.Store.GetTracesNeedingDecision(ctx, c.Config.GetCollectionConfig().GetDeciderBatchSize())
	if err != nil {
		return err
	}

	c.Metrics.Histogram("collector_decider_batch_count", len(tracesIDs))

	if len(tracesIDs) == 0 {
		return nil
	}
	statuses, err := c.Store.GetStatusForTraces(ctx, tracesIDs)
	if err != nil {
		return err
	}

	traces := make([]*centralstore.CentralTrace, len(statuses))
	stateMap := make(map[string]*centralstore.CentralTraceStatus, len(statuses))

	eg := &errgroup.Group{}
	concurrency := c.Config.GetCollectionConfig().TraceFetcherConcurrency
	if concurrency <= 0 {
		concurrency = 10
	}
	eg.SetLimit(concurrency)

	for idx, status := range statuses {
		// make a decision on each trace
		if status.State != centralstore.AwaitingDecision {
			// someone else got to it first
			continue
		}
		currentStatus, currentIdx := status, idx
		stateMap[status.TraceID] = status

		eg.Go(func() error {
			trace, err := c.Store.GetTrace(ctx, currentStatus.TraceID)
			if err != nil {
				return err
			}
			traces[currentIdx] = trace
			return nil
		})
	}
	err = eg.Wait()
	if err != nil {
		c.Logger.Error().Logf("error getting trace information: %s", err)
	}

	if len(traces) == 0 {
		return err
	}

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
		selector := stateMap[trace.TraceID].SamplerSelector
		logFields := logrus.Fields{
			"trace_id": trace.TraceID,
		}
		logFields["sampler_selector"] = selector

		// use sampler key to find sampler; create and cache if not found
		c.mut.RLock()
		sampler, found = c.samplersByDestination[selector]
		c.mut.RUnlock()
		if !found {
			sampler = c.SamplerFactory.GetSamplerImplementationForKey(selector)
			c.mut.Lock()
			c.samplersByDestination[selector] = sampler
			c.mut.Unlock()
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

		if !shouldSend {
			c.Metrics.Increment("trace_decision_dropped")
			c.Logger.Info().WithFields(logFields).Logf("Dropping trace because of sampling")
		}
		c.Metrics.Increment("trace_decision_kept")
		// This will observe sample rate decisions only if the trace is kept
		c.Metrics.Histogram("trace_kept_sample_rate", float64(rate))

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
		status.Rate = rate
		stateMap[status.TraceID] = status
	}

	updatedStatuses := make([]*centralstore.CentralTraceStatus, 0, len(stateMap))
	for _, status := range stateMap {
		if status == nil {
			continue
		}
		updatedStatuses = append(updatedStatuses, status)
	}

	return c.Store.SetTraceStatuses(ctx, updatedStatuses)
}

func (c *CentralCollector) processSpan(sp *types.Span) error {
	defer func() {
		c.Metrics.Increment("span_processed")
		c.Metrics.Down("spans_waiting")
	}()
	err := c.SpanCache.Set(sp)
	if err != nil {
		c.Logger.Error().Logf("error adding span with trace ID %s to cache: %s", sp.TraceID, err)
		return err
	}

	trace := c.SpanCache.Get(sp.TraceID)

	// construct a central store span
	cs := &centralstore.CentralSpan{
		TraceID:   sp.TraceID,
		SpanID:    sp.ID,
		KeyFields: make(map[string]interface{}),
		IsRoot:    sp.IsRoot,
	}
	cs.Type = sp.Type()

	selector := trace.GetSamplerSelector(c.Config.GetDatasetPrefix())
	cs.SetSamplerSelector(selector)
	if selector == "" {
		c.Logger.Error().Logf("error getting sampler selection key for trace %s", sp.TraceID)
	}

	logFields := logrus.Fields{
		"trace_id": trace.TraceID,
	}
	logFields["sampler_selector"] = selector

	c.mut.RLock()
	sampler, found := c.samplersByDestination[selector]
	c.mut.RUnlock()
	if !found {
		sampler = c.SamplerFactory.GetSamplerImplementationForKey(selector)
		c.mut.Lock()
		c.samplersByDestination[selector] = sampler
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
	return c.Store.WriteSpan(ctx, cs)
}

func (c *CentralCollector) checkAlloc() {
	inMemConfig := c.Config.GetCollectionConfig()
	maxAlloc := inMemConfig.GetMaxAlloc()

	var mem runtime.MemStats
	// Manually GC here - so we can get a more accurate picture of memory usage
	runtime.GC()
	runtime.ReadMemStats(&mem)
	c.Metrics.Gauge("memory_heap_allocation", int64(mem.Alloc))
	if maxAlloc == 0 || mem.Alloc < uint64(maxAlloc) {
		return
	}

	// Figure out what fraction of the total cache we should remove. We'd like it to be
	// enough to get us below the max capacity, but not TOO much below.
	// Because our impact numbers are only the data size, reducing by enough to reach
	// max alloc will actually do more than that.
	totalToRemove := mem.Alloc - uint64(maxAlloc)
	totalTraces := c.SpanCache.Len()
	c.Metrics.Gauge("collector_cache_size", totalTraces)

	percentage := float64(totalToRemove) / float64(totalTraces)
	traceIDs := c.SpanCache.GetOldest(percentage)

	ctx := context.Background()
	totalDataSizeSent := 0
	var numOfTracesSent int
	// send traces for decisions
	for _, id := range traceIDs {
		trace := c.SpanCache.Get(id)
		if trace == nil {
			continue
		}
		totalDataSizeSent += trace.DataSize
		numOfTracesSent++
		err := c.Store.WriteSpan(ctx, &centralstore.CentralSpan{TraceID: id})
		if err != nil {
			c.Logger.Error().Logf("error sending trace %s for decision: %s", id, err)
		}
	}

	// Treat any MaxAlloc overage as an error so we know it's happening
	c.Logger.Error().
		WithField("cache_size", totalTraces).
		WithField("alloc", mem.Alloc).
		WithField("num_traces_sent", numOfTracesSent).
		WithField("datasize_sent", totalDataSizeSent).
		Logf("evicting large traces early due to memory overage")

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

func (c *CentralCollector) send(status *centralstore.CentralTraceStatus) {
	trace := c.SpanCache.Get(status.TraceID)
	if trace == nil {
		c.Logger.Error().Logf("trace %s not found in cache", status.TraceID)
		return
	}

	traceDur := time.Since(trace.ArrivalTime)
	c.Metrics.Histogram("trace_duration_ms", float64(traceDur.Milliseconds()))
	c.Metrics.Histogram("trace_span_count", float64(status.DescendantCount()))

	c.Metrics.Increment(status.KeepReason)

	// get sampler selector (dataset for legacy keys, environment for new keys)
	selector := trace.GetSamplerSelector(c.Config.GetDatasetPrefix())
	logFields := logrus.Fields{
		"trace_id": trace.TraceID,
	}
	logFields["sampler_selector"] = selector

	logFields["reason"] = status.KeepReason

	c.Metrics.Increment("trace_send_kept")
	// This will observe sample rate decisions only if the trace is kept
	c.Metrics.Histogram("trace_send_kept_sample_rate", float64(status.Rate))

	c.Logger.Info().WithFields(logFields).Logf("Sending trace")
	for _, sp := range trace.GetSpans() {
		if sp.Data == nil {
			sp.Data = make(map[string]interface{})
		}

		if c.Config.GetAddRuleReasonToTrace() {
			reason, ok := status.Metadata["meta.refinery.reason"]
			sendReason := status.Metadata["meta.refinery.send_reason"]
			if sp.ArrivalTime.After(status.Timestamp) {
				if !ok {
					reason = "late arriving span"
				} else {
					reason = fmt.Sprintf("%s - late arriving span", reason)
				}
				sendReason = TraceSendLateSpan
			}
			sp.Data["meta.refinery.reason"] = reason
			if sendReason != nil {
				sp.Data["meta.refinery.send_reason"] = sendReason
			}
		}

		sp.Data["meta.span_event_count"] = int(status.SpanEventCount())
		sp.Data["meta.span_link_count"] = int(status.SpanLinkCount())
		sp.Data["meta.span_count"] = int(status.SpanCount())
		sp.Data["meta.event_count"] = int(status.DescendantCount())
		for k, v := range status.Metadata {
			if k == "meta.refinery.decider.local_hostname" && !c.Config.GetAddHostMetadataToTrace() {
				continue
			}
			if k == "meta.refinery.send_reason" || k == "meta.refinery.reason" {
				continue
			}
			sp.Data[k] = v
		}

		mergeTraceAndSpanSampleRates(sp, status.SampleRate())
		c.addAdditionalAttributes(sp)
		c.Transmission.EnqueueSpan(sp)
	}
}

func (c *CentralCollector) addAdditionalAttributes(sp *types.Span) {
	for k, v := range c.Config.GetAdditionalAttributes() {
		sp.Data[k] = v
	}
}

func (c *CentralCollector) reloadConfig() {
	c.Logger.Debug().Logf("reloading central collector config")

	c.StressRelief.UpdateFromConfig(c.Config.GetStressReliefConfig())

	c.Metrics.Store("MEMORY_MAX_ALLOC", float64(c.Config.GetCollectionConfig().GetMaxAlloc()))

	// clear out any samplers that we have previously created
	// so that the new configuration will be propagated
	c.mut.Lock()
	c.samplersByDestination = make(map[string]sample.Sampler)
	c.mut.Unlock()
}

func (c *CentralCollector) Stressed() bool {
	return c.StressRelief.Stressed()
}

func mergeTraceAndSpanSampleRates(sp *types.Span, traceSampleRate uint) {
	tempSampleRate := sp.SampleRate
	if sp.SampleRate != 0 {
		// Write down the original sample rate so that that information
		// is more easily recovered
		sp.Data["meta.refinery.original_sample_rate"] = sp.SampleRate
	}

	if tempSampleRate < 1 {
		// See https://docs.honeycomb.io/manage-data-volume/sampling/
		// SampleRate is the denominator of the ratio of sampled spans
		// HoneyComb treats a missing or 0 SampleRate the same as 1, but
		// behaves better/more consistently if the SampleRate is explicitly
		// set instead of inferred
		tempSampleRate = 1
	}

	sp.SampleRate = tempSampleRate * traceSampleRate
}

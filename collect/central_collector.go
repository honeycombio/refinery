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
	GetStressedSampleRate(traceID string) (rate uint, keep bool, reason string)
	ProcessSpanImmediately(sp *types.Span, keep bool, sampleRate uint, reason string)
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
)

const (
	// TODO: these should be configurable
	cacheEjectBatchSize         = 100
	retryLimit                  = 5
	concurrentTraceFetcherCount = 10
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
	SamplerFactory *sample.SamplerFactory   `inject:""`
	SpanCache      cache.SpanCache          `inject:""`

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

	hostname string

	// test hooks
	blockOnCollect bool
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

	// test hooks
	c.processorCycle = NewCycle(c.Clock, collectorCfg.GetProcessTracesPauseDuration(), c.done)
	c.deciderCycle = NewCycle(c.Clock, collectorCfg.GetDeciderPauseDuration(), c.done)

	c.Metrics.Register("collector_processor_batch_count", "histogram")
	c.Metrics.Register("collector_decider_batch_count", "histogram")
	c.Metrics.Register("trace_send_kept", "counter")

	if c.Config.GetAddHostMetadataToTrace() {
		if hostname, err := os.Hostname(); err == nil && hostname != "" {
			c.hostname = hostname
		}
	}

	// spin up one collector because this is a single threaded collector
	c.eg = &errgroup.Group{}
	c.eg.SetLimit(retryLimit)
	c.eg.Go(func() error {
		err := catchPanic(c.receive)
		if err != nil {
			c.Logger.Error().Logf("error collecting spans: %s", err)
		}
		return nil
	})

	c.eg.Go(func() error {
		err := catchPanic(c.process)
		if err != nil {
			c.Logger.Error().Logf("error processing traces: %s", err)
		}
		return nil
	})

	c.eg.Go(func() error {
		err := catchPanic(c.decide)
		if err != nil {
			c.Logger.Error().Logf("error making decision for traces: %s", err)
		}
		return nil
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
// It will immediately stop the receiver and the decider.
// The processor will finish its current cycle before stopping.
// It will also upload all the remaining traces in the cache to
// the central store.
func (c *CentralCollector) shutdown(ctx context.Context) error {
	// try to send the remaining traces to the cache
	interval := 20 * time.Microsecond
	done := make(chan struct{})
	processCycle := NewCycle(c.Clock, interval, done)
	processCtx, cancel := context.WithTimeout(ctx, c.Config.GetCollectionConfig().GetShutdownDelay()/2)
	defer cancel()

	if err := processCycle.Run(processCtx, c.processTraces); err != nil {
		c.Logger.Error().Logf("error processing remaining traces: %s", err)
	}
	defer close(done)

	fmt.Println("Sending remaining traces to central store")

	// send the remaining traces to the central store
	ids := c.SpanCache.GetTraceIDs(c.SpanCache.Len())
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

			cs.SetSamplerKey(trace.GetSamplerKey(c.Config.GetDatasetPrefix()))
			err := c.Store.WriteSpan(ctx, cs)
			if err != nil {
				c.Logger.Error().Logf("error sending span %s for trace %s during shutdown: %s", sp.ID, id, err)
			}
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
		select {
		case <-c.done:
			return nil
		case <-ticker.Chan():
			c.sendTracesForDecision()
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
	return c.processorCycle.Run(context.Background(), c.processTraces)
}

func (c *CentralCollector) processTraces(ctx context.Context) error {
	ids := c.SpanCache.GetTraceIDs(c.Config.GetCollectionConfig().GetProcessTracesBatchSize())

	c.Metrics.Histogram("collector_processor_batch_count", len(ids))
	if len(ids) == 0 {
		return nil
	}

	statuses, err := c.Store.GetStatusForTraces(ctx, ids)
	if err != nil {
		c.Logger.Error().Logf("error getting statuses for traces: %s", err)
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
	return c.deciderCycle.Run(context.Background(), c.makeDecision)
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
	eg.SetLimit(concurrentTraceFetcherCount)

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
		samplerKey := trace.GetSamplerKey()
		logFields := logrus.Fields{
			"trace_id": trace.TraceID,
		}
		logFields["sampler_key"] = samplerKey

		// use sampler key to find sampler; create and cache if not found
		c.mut.RLock()
		sampler, found = c.samplersByDestination[samplerKey]
		c.mut.RUnlock()
		if !found {
			sampler = c.SamplerFactory.GetSamplerImplementationForKey(samplerKey)
			c.mut.Lock()
			c.samplersByDestination[samplerKey] = sampler
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

	samplerKey := trace.GetSamplerKey(c.Config.GetDatasetPrefix())
	cs.SetSamplerKey(samplerKey)
	if samplerKey == "" {
		c.Logger.Error().Logf("error getting sampler key for trace %s", sp.TraceID)
	}

	logFields := logrus.Fields{
		"trace_id": trace.TraceID,
	}
	logFields["sampler_key"] = samplerKey

	c.mut.RLock()
	sampler, found := c.samplersByDestination[samplerKey]
	c.mut.RUnlock()
	if !found {
		sampler = c.SamplerFactory.GetSamplerImplementationForKey(samplerKey)
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
	return c.Store.WriteSpan(ctx, cs)
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
	c.Metrics.Store("MEMORY_MAX_ALLOC", float64(maxAlloc))

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

	// get sampler key (dataset for legacy keys, environment for new keys)
	samplerKey := trace.GetSamplerKey(c.Config.GetDatasetPrefix())
	logFields := logrus.Fields{
		"trace_id": trace.TraceID,
	}
	logFields["sampler_key"] = samplerKey

	// If we have a root span, update it with the count before determining the SampleRate.
	if trace.RootSpan != nil {
		rs := trace.RootSpan
		if c.Config.GetAddCountsToRoot() {
			rs.Data["meta.span_event_count"] = int(status.SpanEventCount())
			rs.Data["meta.span_link_count"] = int(status.SpanLinkCount())
			rs.Data["meta.span_count"] = int(status.SpanCount())
			rs.Data["meta.event_count"] = int(status.DescendantCount())
		}
	}

	logFields["reason"] = status.KeepReason

	c.Metrics.Increment("trace_send_kept")
	// This will observe sample rate decisions only if the trace is kept
	c.Metrics.Histogram("trace_kept_sample_rate", float64(status.Rate))

	c.Logger.Info().WithFields(logFields).Logf("Sending trace")
	for _, sp := range trace.GetSpans() {
		if c.Config.GetAddRuleReasonToTrace() {
			reason, ok := status.Metadata["meta.refinery.reason"]
			if sp.ArrivalTime.After(status.Timestamp) {
				if !ok {
					reason = "late arriving span"
				} else {
					reason = fmt.Sprintf("%s - late arriving span", reason)
				}
			}
			sp.Data["meta.refinery.reason"] = reason
			sp.Data["meta.refinery.send_reason"] = TraceSendLateSpan

		}
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

	// TODO enable this when stress relief is implemented
	// c.StressRelief.UpdateFromConfig(c.Config.GetStressReliefConfig())

	// clear out any samplers that we have previously created
	// so that the new configuration will be propagated
	c.mut.Lock()
	c.samplersByDestination = make(map[string]sample.Sampler)
	c.mut.Unlock()
}

// TODO: REMOVE THIS
func (c *CentralCollector) AddSpanFromPeer(sp *types.Span) error {
	return c.add(sp, c.incoming)
}

// TODO: REMOVE THIS
func (c *CentralCollector) ProcessSpanImmediately(sp *types.Span, keep bool, sampleRate uint, reason string) {
	c.processSpan(sp)
	return
}

func (c *CentralCollector) Stressed() bool {
	return false
}

func (c *CentralCollector) GetStressedSampleRate(traceID string) (rate uint, keep bool, reason string) {
	return 1, true, "stressed"
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

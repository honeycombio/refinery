package collect

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/honeycombio/refinery/centralstore"
	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/collect/stressRelief"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/generics"
	"github.com/honeycombio/refinery/internal/gossip"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/internal/otelutil"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

type Collector interface {
	// AddSpan adds a span to be collected, buffered, and merged into a trace.
	// Once the trace is "complete", it'll be passed off to the sampler then
	// scheduled for transmission.
	AddSpan(*types.Span) error
	Stressed() bool
	ProcessSpanImmediately(*types.Span) (bool, error)
}

func GetCollectorImplementation(c config.Config) Collector {
	return &CentralCollector{}
}

var ErrWouldBlock = errors.New("not adding span, channel buffer is full")

// These are the names of the metrics we use to track our send decisions.
const (
	TraceSendGotRoot        = "trace_send_got_root"
	TraceSendExpired        = "trace_send_expired"
	TraceSendEjectedMemsize = "trace_send_ejected_memsize"
	TraceSendLateSpan       = "trace_send_late_span"
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
	Store          centralstore.SmartStorer    `inject:""`
	Config         config.Config               `inject:""`
	Clock          clockwork.Clock             `inject:""`
	Transmission   transmit.Transmission       `inject:"upstreamTransmission"`
	DecisionCache  cache.TraceSentCache        `inject:""`
	Logger         logger.Logger               `inject:""`
	Metrics        metrics.Metrics             `inject:"genericMetrics"`
	Tracer         trace.Tracer                `inject:"tracer"`
	StressRelief   stressRelief.StressReliever `inject:"stressRelief"`
	SamplerFactory *sample.SamplerFactory      `inject:""`
	Health         health.Recorder             `inject:""`
	SpanCache      cache.SpanCache             `inject:""`
	Gossip         gossip.Gossiper             `inject:"gossip"`

	// whenever samplersByDestination is accessed, it should be protected by
	// the mut mutex
	mut                   sync.RWMutex
	samplersByDestination map[string]sample.Sampler

	incoming chan *types.Span
	reload   chan struct{}

	done         chan struct{}
	eg           *errgroup.Group
	senderCycle  *Cycle
	deciderCycle *Cycle
	metricsCycle *Cycle
	cleanupCycle *Cycle

	// can't close these because gossip doesn't unregister itself
	keepChan chan []byte
	dropChan chan []byte

	hostname string

	// test hooks
	blockOnCollect bool
	isTest         bool
}

const (
	receiverHealth = "receiver"
	deciderHealth  = "decider"
	senderHealth   = "sender"
)

func (c *CentralCollector) Start() error {
	// call reload config and then get the updated unique fields
	collectorCfg := c.Config.GetCollectionConfig()

	// we're a health check reporter so register ourselves for each of our major routines
	c.Health.Register(receiverHealth, time.Duration(5*collectorCfg.MemoryCycleDuration))
	deciderHealthThreshold := collectorCfg.GetDeciderHealthThreshold()
	if deciderHealthThreshold == 0 {
		deciderHealthThreshold = 5 * collectorCfg.GetDeciderCycleDuration()
	}
	c.Health.Register(deciderHealth, deciderHealthThreshold)

	// the sender health check should only be run if we're using it
	if !collectorCfg.UseDecisionGossip {
		c.Health.Register(senderHealth, 5*collectorCfg.GetSenderCycleDuration())
	}

	c.done = make(chan struct{})

	// listen for config reloads
	c.Config.RegisterReloadCallback(c.sendReloadSignal)
	c.StressRelief.UpdateFromConfig(c.Config.GetStressReliefConfig())

	c.incoming = make(chan *types.Span, collectorCfg.GetIncomingQueueSize())
	c.reload = make(chan struct{}, 1)
	c.samplersByDestination = make(map[string]sample.Sampler)

	// The cycles manage a periodic task and also provide some test hooks
	c.metricsCycle = NewCycle(c.Clock, c.Config.GetSendTickerValue(), c.done)
	c.deciderCycle = NewCycle(c.Clock, collectorCfg.GetDeciderCycleDuration(), c.done)
	if collectorCfg.UseDecisionGossip {
		c.cleanupCycle = NewCycle(c.Clock, collectorCfg.GetCleanupCycleDuration(), c.done)
	} else {
		c.senderCycle = NewCycle(c.Clock, collectorCfg.GetSenderCycleDuration(), c.done)
	}

	c.Metrics.Register("collector_sender_batch_count", "histogram")
	c.Metrics.Register("collector_decider_batch_count", "histogram")
	c.Metrics.Register("trace_send_kept", "counter")
	c.Metrics.Register("trace_send_kept_sample_rate", "histogram")
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
	c.Metrics.Register("dropped_from_stress", "counter")
	c.Metrics.Register("kept_from_stress", "counter")
	c.Metrics.Register("collector_keep_trace", "counter")
	c.Metrics.Register("collector_drop_trace", "counter")
	c.Metrics.Register("collector_drop_old_trace", "counter")
	c.Metrics.Register("collector_decide_trace", "counter")
	c.Metrics.Register("decider_decided_per_second", "histogram")
	c.Metrics.Register("decider_considered_per_second", "histogram")
	c.Metrics.Register("sender_considered_per_second", "histogram")
	c.Metrics.Register("collector_receiver_runs", "counter")
	c.Metrics.Register("collector_sender_runs", "counter")
	c.Metrics.Register("collector_decider_runs", "counter")
	c.Metrics.Register("collector_cleanup_runs", "counter")
	c.Metrics.Register("collector_span_decision_cache_hit", "counter")

	if c.Config.GetAddHostMetadataToTrace() {
		if hostname, err := os.Hostname(); err == nil && hostname != "" {
			c.hostname = hostname
		}
	}
	c.Metrics.Store("INCOMING_CAP", float64(cap(c.incoming)))

	// spin up one collector because this is a single threaded collector
	c.eg = &errgroup.Group{}
	c.eg.Go(c.receive)
	c.eg.Go(c.decide)
	if collectorCfg.UseDecisionGossip {
		c.eg.Go(c.cleanup)
	} else {
		c.eg.Go(c.send)
	}
	c.eg.Go(func() error {
		return c.metricsCycle.Run(context.Background(), func(ctx context.Context) error {
			if err := c.Store.RecordMetrics(ctx); err != nil {
				c.Logger.Error().Logf("error recording metrics: %s", err)
			}

			m, err := c.DecisionCache.GetMetrics()
			if err != nil {
				c.Logger.Error().Logf("error getting decision cache metrics: %s", err)
			} else {
				for k, v := range m {
					c.Metrics.Count("collector_"+k, v)
				}
			}

			return nil
		})
	})

	// do we need these to be configurable?
	maxTime := time.Duration(collectorCfg.AggregationInterval)
	if maxTime <= 0 {
		maxTime = 100 * time.Millisecond
	}
	maxCount := collectorCfg.AggregationCount
	if maxCount <= 0 {
		maxCount = 500
	}
	maxConcurrency := collectorCfg.AggregationConcurrency
	if maxConcurrency <= 0 {
		maxConcurrency = 4
	}
	egKeepAgg := &errgroup.Group{}
	egDropAgg := &errgroup.Group{}
	egKeepAgg.SetLimit(maxConcurrency) // we want to limit the number of goroutines that are aggregating trace IDs
	egDropAgg.SetLimit(maxConcurrency) // we want to limit the number of goroutines that are aggregating trace IDs

	// subscribe to the Keep and Drop decisions
	c.keepChan = c.Gossip.Subscribe(c.Gossip.GetChannel(gossip.ChannelKeep), maxCount)
	c.dropChan = c.Gossip.Subscribe(c.Gossip.GetChannel(gossip.ChannelDrop), maxCount)

	go c.aggregateTraceIDChannel(c.keepChan, c.keepTraces, egKeepAgg, maxTime, maxCount)
	go c.aggregateTraceIDChannel(c.dropChan, c.dropTraces, egDropAgg, maxTime, maxCount)

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

	// we have to make sure the health check says we're alive but not accepting data during shutdown
	c.Health.Unregister(receiverHealth)
	c.Health.Unregister(deciderHealth)
	// reregister the sender health check to a much longer time so we can finish sending traces
	c.Health.Register(senderHealth, 5*time.Second)

	if err := c.shutdown(ctx); err != nil {
		c.Logger.Error().Logf("error shutting down collector: %s", err)
	}

	return nil
}

// shutdown implements the shutdown logic for the collector.
// It starts a new sender cycle with a shorter interval to monitor
// trace decisions made for the remaining traces in the cache.
//
// After the sender cycle is finished, it also uploads all the
// remaining traces in the cache to the central store.
//
// The shutdown process is expected to finish within the shutdown delay.
// Half of the shutdown delay is used for the sender cycle and the
// other half is used for uploading the remaining traces.
// If the shutdown process exceeds the shutdown delay, it will return an error.
func (c *CentralCollector) shutdown(ctx context.Context) error {
	ctx, span := otelutil.StartSpanWith(ctx, c.Tracer, "CentralCollector.shutdown", "span_cache_len", c.SpanCache.Len())
	defer span.End()
	// keep processing, hoping to send the remaining traces
	interval := 1 * time.Second
	done := make(chan struct{})
	sendCycle := NewCycle(c.Clock, interval, done)

	// create a new context with a deadline that's half of the shutdown delay for the sender cycle
	sendCtx, cancel := context.WithTimeout(ctx, c.Config.GetCollectionConfig().GetShutdownDelay()/2)
	defer cancel()

	// this is the function that will be called by the sender cycle during shutdown
	shutdownMonitor := func(ctx context.Context) error {
		err := c.sendTraces(ctx)
		if err != nil {
			c.Logger.Error().Logf("during shutdown - error processing remaining traces: %s", err)
			if c.isTest {
				return err
			}
		}
		c.Health.Ready(senderHealth, true)
		return nil
	}

	if err := sendCycle.Run(sendCtx, shutdownMonitor); err != nil {
		// this is expected to happen whenever long traces haven't finished sending during shutdown;
		// log it, but it's not an error
		if errors.Is(err, context.DeadlineExceeded) {
			c.Logger.Info().Logf("traces did not drain in time for shutdown -- forwarding remaining spans")
		} else {
			c.Logger.Error().Logf("during shutdown: context error processing remaining traces: %s", err)
		}
	}
	defer close(done)

	ctxForward, spanForward := otelutil.StartSpanWith(ctx, c.Tracer, "CentralCollector.shutdown.forward", "span_cache_len", c.SpanCache.Len())
	defer spanForward.End()

	// send the remaining traces to the central store
	ids := c.SpanCache.GetTraceIDs(c.SpanCache.Len())
	var sentCount int
	defer func() {
		c.Logger.Info().Logf("sent %d traces to central store during shutdown", sentCount)
		otelutil.AddSpanField(spanForward, "sent_count", sentCount)
	}()

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
			err := c.Store.WriteSpan(ctxForward, cs)
			if err != nil {
				logField := logrus.Fields{
					"span_id":  sp.ID,
					"trace_id": id,
				}
				spanForward.RecordError(err)
				c.Logger.Error().WithFields(logField).Logf("error sending span during shutdown: %s", err)
			}
			sentCount++
			// if the context deadline is exceeded, that means we are
			// about to exceed the shutdown delay, so we should stop
			// sending traces to the central store. Unfortunately, the
			// remaining traces will be lost.
			if errors.Is(err, context.DeadlineExceeded) {
				spanForward.RecordError(err)
				return err
			}
		}
	}

	return nil
}

// ProcessSpanImmediately determines if this trace should be part of the deterministic sample.
// If it's part of the deterministic sample, the decision is written to the central store and
// the span is enqueued for transmission.
func (c *CentralCollector) ProcessSpanImmediately(sp *types.Span) (bool, error) {
	_, span := otelutil.StartSpanWith(context.Background(), c.Tracer, "CentralCollector.ProcessSpanImmediately", "trace_id", sp.TraceID)

	if !c.StressRelief.ShouldSampleDeterministically(sp.TraceID) {
		otelutil.AddSpanField(span, "nondeterministic", 1)
		return false, nil
	}

	var keep bool
	var rate uint
	status := &centralstore.CentralTraceStatus{
		TraceID: sp.TraceID,
	}

	record, reason, found := c.DecisionCache.Check(sp)
	if !found {
		rate, keep, reason = c.StressRelief.GetSampleRate(sp.TraceID)
	} else {
		rate = record.Rate()
		keep = record.Kept()
	}

	otelutil.AddSpanFields(span, map[string]interface{}{
		"rate":   rate,
		"keep":   keep,
		"reason": reason,
	})

	if keep {
		status.State = centralstore.DecisionKeep
		status.KeepReason = reason
		status.Rate = rate
	} else {
		status.State = centralstore.DecisionDrop
	}

	c.DecisionCache.Record(status, keep, reason)

	if !keep {
		c.Metrics.Increment("dropped_from_stress")
		return true, nil
	}
	c.Metrics.Increment("kept_from_stress")

	sp.Event.Data["meta.stressed"] = true
	if c.Config.GetAddRuleReasonToTrace() {
		sp.Event.Data["meta.refinery.reason"] = reason
	}
	if c.hostname != "" {
		sp.Data["meta.refinery.host.name"] = c.hostname
	}
	c.addAdditionalAttributes(sp)
	mergeTraceAndSpanSampleRates(sp, rate)
	c.Transmission.EnqueueSpan(sp)
	return true, nil

}

// implement the Collector interface
func (c *CentralCollector) AddSpan(span *types.Span) error {
	select {
	case c.incoming <- span:
		c.Metrics.Increment("span_received")
		c.Metrics.Up("spans_waiting")
		return nil
	default:
		return ErrWouldBlock
	}
}

func (c *CentralCollector) receive() error {
	tickerDuration := time.Duration(c.Config.GetCollectionConfig().MemoryCycleDuration)
	if tickerDuration <= 0 {
		tickerDuration = 1 * time.Second
	}
	memTicker := c.Clock.NewTicker(tickerDuration)
	defer memTicker.Stop()

	if c.blockOnCollect {
		return nil
	}

	for {
		// record channel lengths as histogram but also as gauges
		c.Metrics.Histogram("collector_incoming_queue", float64(len(c.incoming)))
		c.Metrics.Gauge("collector_incoming_queue_length", float64(len(c.incoming)))
		c.Metrics.Increment("collector_receiver_runs")
		c.Health.Ready(receiverHealth, true)

		select {
		case <-c.done:
			return nil
		case <-memTicker.Chan():
			_, span := otelutil.StartSpanMulti(context.Background(), c.Tracer, "CentralCollector.receive",
				map[string]interface{}{
					"incoming_queue_length": len(c.incoming),
					"select":                "checkAlloc",
				})
			c.checkAlloc()
			span.End()
		case sp, ok := <-c.incoming:
			if !ok {
				return nil
			}
			_, span := otelutil.StartSpanMulti(context.Background(), c.Tracer, "CentralCollector.receive",
				map[string]interface{}{
					"incoming_queue_length": len(c.incoming),
					"select":                "incoming",
				})
			err := c.processSpan(sp)
			if err != nil {
				otelutil.AddException(span, err)
				c.Logger.Error().Logf("error processing span: %s", err)
			}
			span.End()
		case <-c.reload:
			_, span := otelutil.StartSpanMulti(context.Background(), c.Tracer, "CentralCollector.receive",
				map[string]interface{}{
					"incoming_queue_length": len(c.incoming),
					"select":                "reload",
				})
			c.reloadConfig()
			span.End()
		}
	}

}

func (c *CentralCollector) send() error {
	return c.senderCycle.Run(context.Background(), func(ctx context.Context) error {
		err := c.sendTraces(ctx)
		if err != nil {
			c.Logger.Error().Logf("error processing traces: %s", err)
			if c.isTest {
				return err
			}
		}
		c.Health.Ready(senderHealth, true)
		c.Metrics.Increment("collector_sender_runs")

		return nil
	})
}

func (c *CentralCollector) sendTraces(ctx context.Context) error {
	ctx, span := otelutil.StartSpan(ctx, c.Tracer, "CentralCollector.sendTraces")
	defer span.End()
	ids := c.SpanCache.GetTraceIDs(c.Config.GetCollectionConfig().GetSenderBatchSize())
	otelutil.AddSpanField(span, "num_ids", len(ids))

	c.Metrics.Histogram("collector_sender_batch_count", len(ids))
	if len(ids) == 0 {
		return nil
	}

	var tracesConsidered float64
	now := c.Clock.Now()
	defer func() {
		sendTime := c.Clock.Since(now)
		c.Metrics.Histogram("sender_considered_per_second", tracesConsidered/sendTime.Seconds())
	}()

	// if a trace is dropped, we don't want to send it to the central store
	ids = slices.DeleteFunc(ids, func(id string) bool {
		if c.DecisionCache.Dropped(id) {
			c.SpanCache.Remove(id)
			tracesConsidered++
			c.Metrics.Increment("collector_drop_trace")

			return true
		}

		return false
	})

	statuses, err := c.Store.GetStatusForTraces(ctx, ids, centralstore.DecisionKeep, centralstore.DecisionDrop)
	if err != nil {
		return err
	}

	for _, status := range statuses {
		switch status.State {
		case centralstore.DecisionKeep:
			c.sendSpans(status)
			c.SpanCache.Remove(status.TraceID)
			tracesConsidered++
			c.Metrics.Increment("collector_keep_trace")

		case centralstore.DecisionDrop:
			c.SpanCache.Remove(status.TraceID)
			tracesConsidered++
			c.Metrics.Increment("collector_drop_trace")
		default:
			// this shouldn't happen, but we want to be safe about it.
			// we don't want to send traces that are in any other state;
			// it's an error, but ending the loop is not what we want
			c.Logger.Error().WithFields(logrus.Fields{
				"trace_id": status.TraceID,
				"state":    status.State,
			}).Logf("unexpected state for trace")
			continue
		}
	}

	return nil
}

// aggregateTraceIDChannel listens on the provided chan for up to maxTime or
// until it receives maxCount trace IDs. As long as there's at least one, it
// forwards them in aggregate to the supplied processing function. We do this so we can batch up trace
// IDs and make fewer calls to the central store.
//
// The process function is called in a goroutine and there's no guarantee that the goroutines won't
// overlap. The concurrency max of these goroutines is controlled by the errorgroup called egAgg.
//
// The function should return any traceIDs that aren't processed immediately;
// they will be sent again eventually.
func (c *CentralCollector) aggregateTraceIDChannel(
	ch chan []byte, process func([]string) []string, eg *errgroup.Group, maxTime time.Duration, maxCount int) {

	ticker := c.Clock.NewTicker(maxTime)
	defer ticker.Stop()
	traceIDs := make([]string, 0, maxCount)
	send := false
	for {
		select {
		case <-c.done:
			eg.Wait()
			return
		case msgBytes := <-ch:
			// unmarshal the message into a slice of trace IDs
			msgIDs, err := decodeBatch(msgBytes)
			if err != nil {
				c.Logger.Error().Logf("error decompressing trace IDs: %s", err)
				continue
			}
			// if we get a trace ID, add it to the list
			traceIDs = append(traceIDs, msgIDs...)
			// if we exceeded the max count, we need to send
			if len(traceIDs) >= maxCount {
				send = true
			}
		case <-ticker.Chan():
			// ticker fired, so send what we have
			send = true
		}

		// if we need to send, do so
		if send && len(traceIDs) > 0 {
			// copy the traceIDs so we can clear the list
			idsToProcess := make([]string, len(traceIDs))
			copy(idsToProcess, traceIDs)
			// clear the list
			traceIDs = traceIDs[:0]

			// now process the result in a goroutine so we can keep listening
			eg.Go(func() error {
				select {
				case <-c.done:
					return nil
				default:
				}

				// we get back the ones that didn't process immediately
				notready := process(idsToProcess)
				// put any unused traceIDs back into the channel but do it a bit
				// slowly (we're still in our goroutine here)
				ids, err := encodeBatch(notready)
				if err != nil {
					c.Logger.Error().Logf("error compressing trace IDs that are not ready to be processed: %s", err)
					return nil
				}

				select {
				case ch <- ids:
				case <-c.done:
					return nil
				}
				return nil
			})
			send = false
		}
	}
}

// keepTraces needs to retrieve the status of the traces from the central store
// so that it can attach the metadata as needed
func (c *CentralCollector) keepTraces(ids []string) []string {
	ctx, span := otelutil.StartSpanWith(context.Background(), c.Tracer, "CentralCollector.keepTraces", "num_ids", len(ids))
	defer span.End()
	// check to make sure that we're tracking at least one span for each trace ID we're given
	// if we're not, there's no point in further worrying about it for this refinery
	ids = slices.DeleteFunc(ids, func(id string) bool {
		return c.SpanCache.Get(id) == nil
	})
	otelutil.AddSpanField(span, "num_ids_after_filter", len(ids))

	statuses, err := c.Store.GetStatusForTraces(ctx, ids, centralstore.DecisionKeep)
	if err != nil {
		c.Logger.Error().Logf("error getting status for traces: %s", err)
	}

	idset := generics.NewSet(ids...)
	for _, status := range statuses {
		c.sendSpans(status)
		c.SpanCache.Remove(status.TraceID)
		idset.Remove(status.TraceID)
		c.Metrics.Increment("collector_keep_trace")
	}
	return idset.Members()
}

// dropTraces doesn't need to retrieve the status of the traces from the central store
// because it's only removing the traces from the cache
func (c *CentralCollector) dropTraces(ids []string) []string {
	_, span := otelutil.StartSpanWith(context.Background(), c.Tracer, "CentralCollector.dropTraces", "num_ids", len(ids))
	defer span.End()
	for _, traceID := range ids {
		c.SpanCache.Remove(traceID)
		c.Metrics.Increment("collector_drop_trace")
	}
	return nil
}

// The cleanup task is responsible for removing traces from the cache that have
// been around for too long. This is a hedge against process restarts where the
// gossiped message was lost or happened before we existed.
func (c *CentralCollector) cleanup() error {
	return c.cleanupCycle.Run(context.Background(), func(ctx context.Context) error {
		c.cleanupTraces(ctx)
		c.Metrics.Increment("collector_cleanup_runs")
		return nil
	})
}

// Cleanup traces asks for old (expired) trace IDs from the cache and then gets the status
// (either keep or drop) from the central store, and then dispatches them appropriately.
// In a stable refinery cluster, this should be a no-op.
func (c *CentralCollector) cleanupTraces(ctx context.Context) {
	ctx, span := otelutil.StartSpan(ctx, c.Tracer, "CentralCollector.cleanupTraces")
	defer span.End()
	ids := c.SpanCache.GetOldTraceIDs()
	otelutil.AddSpanField(span, "num_ids", len(ids))

	c.Metrics.Histogram("collector_cleanup_batch_count", len(ids))
	if len(ids) == 0 {
		return
	}

	var tracesConsidered float64
	now := c.Clock.Now()
	defer func() {
		sendTime := c.Clock.Since(now)
		c.Metrics.Histogram("sender_considered_per_second", tracesConsidered/sendTime.Seconds())
	}()

	statuses, err := c.Store.GetStatusForTraces(ctx, ids, centralstore.AllTraceStates...)
	if err != nil {
		span.RecordError(err)
		c.Logger.Error().Logf("error getting status for traces in cleanupTraces: %s", err)
	}

	for _, status := range statuses {
		switch status.State {
		case centralstore.DecisionKeep:
			c.sendSpans(status)
			c.SpanCache.Remove(status.TraceID)
			tracesConsidered++
			c.Metrics.Increment("collector_keep_trace")

		case centralstore.DecisionDrop:
			c.SpanCache.Remove(status.TraceID)
			tracesConsidered++
			c.Metrics.Increment("collector_drop_trace")

		default:
			// if we didn't find the trace, but it's already
			// this old, we need to drop it so it doesn't live forever.
			// but let's record that we did.
			c.SpanCache.Remove(status.TraceID)
			tracesConsidered++
			c.Logger.Info().WithField("trace_id", status.TraceID).Logf("dropping old trace")
			c.Metrics.Increment("collector_drop_old_trace")
			continue
		}
	}
}

func (c *CentralCollector) decide() error {
	return c.deciderCycle.Run(context.Background(), func(ctx context.Context) error {
		err := c.makeDecisions(ctx)
		if err != nil {
			c.Logger.Error().Logf("error making decision: %s", err)
			if c.isTest {
				return err
			}
		}
		c.Health.Ready(deciderHealth, true)
		c.Metrics.Increment("collector_decider_runs")

		return nil
	})
}

// encodeBatch and decodeBatch are used to serialize and deserialize the trace IDs
// we tried several different methods of encoding and compression:
// - encoding/gob alone
// - encoding/gob + gzip
// - encoding/gob + snappy
// - encoding/json
// - snappy alone
// Turns out that in the context of Redis gossip and the overhead of gossip itself,
// the best performance was achieved with just making a list of strings.

func encodeBatch(traceIDs []string) ([]byte, error) {
	s := strings.Join(traceIDs, "\t")
	return []byte(s), nil
}

func decodeBatch(data []byte) ([]string, error) {
	return strings.Split(string(data), "\t"), nil
}

func (c *CentralCollector) makeDecisions(ctx context.Context) error {
	ctx, span := otelutil.StartSpan(ctx, c.Tracer, "CentralCollector.makeDecision")
	defer span.End()
	tracesIDs, err := c.Store.GetTracesNeedingDecision(ctx, c.Config.GetCollectionConfig().GetDeciderBatchSize())
	if err != nil {
		span.RecordError(err)
		return err
	}

	c.Metrics.Histogram("collector_decider_batch_count", len(tracesIDs))

	if len(tracesIDs) == 0 {
		return nil
	}

	var tracesDecided float64
	var tracesConsidered float64
	now := c.Clock.Now()
	defer func() {
		sendTime := c.Clock.Since(now)
		c.Metrics.Histogram("decider_decided_per_second", tracesDecided/sendTime.Seconds())
		c.Metrics.Histogram("decider_considered_per_second", tracesConsidered/sendTime.Seconds())
	}()

	statuses, err := c.Store.GetStatusForTraces(ctx, tracesIDs, centralstore.AwaitingDecision)
	if err != nil {
		span.RecordError(err)
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
	otelutil.AddSpanField(span, "concurrency", concurrency)

	for idx, status := range statuses {
		// make a decision on each trace
		if status.State != centralstore.AwaitingDecision {
			return fmt.Errorf("unexpected state %s for trace %s", status.State, status.TraceID)
		}
		currentStatus, currentIdx := status, idx
		stateMap[status.TraceID] = status

		eg.Go(func() error {
			ctx, span := otelutil.StartSpan(ctx, c.Tracer, "CentralCollector.makeDecision.getTrace")
			defer span.End()
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

	ctxTraces, spanTraces := otelutil.StartSpanWith(ctx, c.Tracer, "CentralCollector.makeDecision.traceLoop", "num_traces", len(traces))
	defer spanTraces.End()

	gossip_keep_channel := c.Gossip.GetChannel(gossip.ChannelKeep)
	gossip_drop_channel := c.Gossip.GetChannel(gossip.ChannelDrop)
	keptIDs := make([]string, 0)
	droppedIDs := make([]string, 0)

	for _, trace := range traces {
		if trace == nil {
			continue
		}
		tracesConsidered++

		_, span := otelutil.StartSpan(ctxTraces, c.Tracer, "CentralCollector.makeDecision.trace")

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
			"trace_id":         trace.TraceID,
			"sampler_selector": selector,
		}

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
			c.Logger.Error().WithFields(logFields).Logf("trace not found in state map")
			continue
		}

		tr := &traceForDecision{
			CentralTrace:    trace,
			descendantCount: status.DescendantCount(),
		}

		// make sampling decision and update the trace
		rate, shouldSend, reason, key := sampler.GetSampleRate(tr)
		otelutil.AddSpanFields(span, map[string]interface{}{
			"trace_id": trace.TraceID,
			"rate":     rate,
			"keep":     shouldSend,
			"reason":   reason,
			"key":      key,
		})
		logFields["reason"] = reason
		if key != "" {
			logFields["sample_key"] = key
		}
		// This will observe sample rate attempts even if the trace is dropped
		c.Metrics.Histogram("trace_aggregate_sample_rate", float64(rate))
		tracesDecided++

		if !shouldSend {
			c.Metrics.Increment("trace_decision_dropped")
			c.Logger.Info().WithFields(logFields).Logf("Dropping trace because of sampling")
		}
		c.Metrics.Increment("trace_decision_kept")

		// These meta data should be stored on the central trace status object
		// so that it's synced across all refinery instances
		if c.Config.GetAddRuleReasonToTrace() {
			status.Metadata["meta.refinery.reason"] = reason
			sendReason := TraceSendExpired
			if trace.Root != nil {
				sendReason = TraceSendGotRoot
			}
			status.Metadata["meta.refinery.send_reason"] = sendReason
			if key != "" {
				status.Metadata["meta.refinery.sample_key"] = key
			}
		}

		if c.hostname != "" {
			status.Metadata["meta.refinery.decider.host.name"] = c.hostname
		}

		var state centralstore.CentralTraceState
		if shouldSend {
			state = centralstore.DecisionKeep
			status.KeepReason = reason
			keptIDs = append(keptIDs, trace.TraceID)
		} else {
			state = centralstore.DecisionDrop
			droppedIDs = append(droppedIDs, trace.TraceID)
		}
		status.State = state
		status.Rate = rate

		c.DecisionCache.Record(status, shouldSend, reason)

		stateMap[status.TraceID] = status
		c.Metrics.Increment("collector_decide_trace")
		span.End()
	}

	updatedStatuses := make([]*centralstore.CentralTraceStatus, 0, len(stateMap))
	for _, status := range stateMap {
		if status == nil {
			continue
		}
		updatedStatuses = append(updatedStatuses, status)
	}

	if err := c.Store.SetTraceStatuses(ctx, updatedStatuses); err != nil {
		return err
	}

	if len(keptIDs) > 0 {
		data, err := encodeBatch(keptIDs)
		if err != nil {
			c.Logger.Error().Logf("error compressing kept trace IDs: %s", err)
		} else {
			c.Gossip.Publish(gossip_keep_channel, data)
		}
	}

	if len(droppedIDs) > 0 {
		data, err := encodeBatch(droppedIDs)
		if err != nil {
			c.Logger.Error().Logf("error compressing dropped trace IDs: %s", err)
		} else {
			c.Gossip.Publish(gossip_drop_channel, data)
		}
	}

	return nil

}

func (c *CentralCollector) processSpan(sp *types.Span) error {
	defer func() {
		c.Metrics.Increment("span_processed")
		c.Metrics.Down("spans_waiting")
	}()

	err := c.SpanCache.Set(sp)
	if err != nil {
		c.Logger.Error().WithField("trace_id", sp.TraceID).Logf("error adding span to cache: %s", err)
		return err
	}

	// check if we have a decision for this trace
	// if we do, process the span immediately
	record, _, found := c.DecisionCache.Check(sp)
	if found {
		c.Metrics.Increment("collector_span_decision_cache_hit")
		if record.Kept() {
			c.keepTraces([]string{sp.TraceID})
		} else {
			c.dropTraces([]string{sp.TraceID})
		}
		return nil
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
		c.Logger.Error().WithField("trace_id", trace.ID()).Logf("error getting sampler selection key for trace")
	}

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
	// We originally used to call runtime.GC() here, but we no longer thing it's necessary.
	// Leaving it commented out for now in case we need to re-enable it.
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
	traceIDs := c.SpanCache.GetHighImpactTraceIDs(percentage)

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
		// in order to eject a trace from refinery's cache, we pretend that its root span has
		// arrived. This will force the trace to enter the decision-making process. Once a decision
		// is made, the trace will be removed from the cache.
		err := c.Store.WriteSpan(ctx, &centralstore.CentralSpan{TraceID: id, IsRoot: true})
		if err != nil {
			c.Logger.Error().WithField("trace_id", id).Logf("error sending trace for decision: %s", err)
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

func (c *CentralCollector) sendSpans(status *centralstore.CentralTraceStatus) {
	trace := c.SpanCache.Get(status.TraceID)
	if trace == nil {
		c.Logger.Error().WithField("trace_id", status.TraceID).Logf("trace not found in cache")
		return
	}
	if !trace.TryMarkTraceForSending() {
		// someone else beat us to it, so get out of here
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
			if !ok {
				reason = status.KeepReason
			}
			var sendReason string
			if sp.ArrivalTime.After(status.Timestamp) {
				if reason == "" {
					reason = "late arriving span"
				} else {
					reason = fmt.Sprintf("%s - late arriving span", reason)
				}
				sendReason = TraceSendLateSpan
			}
			sp.Data["meta.refinery.reason"] = reason
			if sendReason == "" {
				val, ok := status.Metadata["meta.refinery.send_reason"]
				if ok {
					stringVal, ok := val.(string)
					if ok {
						sendReason = stringVal
					}
				}
			}
			sp.Data["meta.refinery.send_reason"] = sendReason
		}
		sp.Data["meta.span_event_count"] = int(status.SpanEventCount())
		sp.Data["meta.span_link_count"] = int(status.SpanLinkCount())
		sp.Data["meta.span_count"] = int(status.SpanCount())
		sp.Data["meta.event_count"] = int(status.DescendantCount())
		for k, v := range status.Metadata {
			if k == "meta.refinery.decider.host.name" && !c.Config.GetAddHostMetadataToTrace() {
				continue
			}
			if k == "meta.refinery.send_reason" || k == "meta.refinery.reason" {
				continue
			}
			sp.Data[k] = v
		}

		if c.hostname != "" && c.Config.GetAddHostMetadataToTrace() {
			sp.Data["meta.refinery.sender.host.name"] = c.hostname
		}

		// if the trace doesn't have a sample rate and is kept, it
		traceSampleRate := status.SampleRate()
		if traceSampleRate == 0 {
			traceSampleRate = uint(c.Config.GetStressReliefConfig().SamplingRate)
		}

		mergeTraceAndSpanSampleRates(sp, traceSampleRate)
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

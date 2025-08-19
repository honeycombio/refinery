package collect

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/dgryski/go-wyhash"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/internal/otelutil"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/sharder"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"
)

const (
	keptTraceDecisionTopic            = "trace_decision_kept"
	dropTraceDecisionTopic            = "trace_decision_dropped"
	decisionMessageBufferSize         = 10_000
	defaultDropDecisionTickerInterval = 1 * time.Second
	defaultKeptDecisionTickerInterval = 1 * time.Second

	collectorHealthKey = "collector"
)

var ErrWouldBlock = errors.New("Dropping span as channel buffer is full. Span will not be processed and will be lost.")

type Collector interface {
	// AddSpan adds a span to be collected, buffered, and merged into a trace.
	// Once the trace is "complete", it'll be passed off to the sampler then
	// scheduled for transmission.
	AddSpan(*types.Span) error
	AddSpanFromPeer(*types.Span) error
	Stressed() bool
	GetStressedSampleRate(traceID string) (rate uint, keep bool, reason string)
	ProcessSpanImmediately(sp *types.Span) (processed bool, keep bool)
}

func GetCollectorImplementation(c config.Config) Collector {
	return &InMemCollector{}
}

// These are the names of the metrics we use to track our send decisions.
const (
	TraceSendGotRoot        = "trace_send_got_root"
	TraceSendExpired        = "trace_send_expired"
	TraceSendSpanLimit      = "trace_send_span_limit"
	TraceSendEjectedFull    = "trace_send_ejected_full"
	TraceSendEjectedMemsize = "trace_send_ejected_memsize"
	TraceSendLateSpan       = "trace_send_late_span"
)

type sendableTrace struct {
	*types.Trace
	reason          string
	sendReason      string
	sampleKey       string
	shouldSend      bool
	rate            uint
	samplerSelector string
}

// InMemCollector is a collector that can use multiple concurrent collection loops.
type InMemCollector struct {
	Config  config.Config   `inject:""`
	Logger  logger.Logger   `inject:""`
	Clock   clockwork.Clock `inject:""`
	Tracer  trace.Tracer    `inject:"tracer"`
	Health  health.Recorder `inject:""`
	Sharder sharder.Sharder `inject:""`

	Transmission     transmit.Transmission  `inject:"upstreamTransmission"`
	PeerTransmission transmit.Transmission  `inject:"peerTransmission"`
	PubSub           pubsub.PubSub          `inject:""`
	Metrics          metrics.Metrics        `inject:"metrics"`
	SamplerFactory   *sample.SamplerFactory `inject:""`
	StressRelief     StressReliever         `inject:"stressRelief"`
	Peers            peer.Peers             `inject:""`

	// For test use only
	TestMode       bool
	BlockOnAddSpan bool

	// Parallel collection support
	collectLoops []*CollectLoop

	// mutex must be held whenever non-channel internal fields are accessed.
	mutex sync.RWMutex

	sampleTraceCache cache.TraceSentCache

	houseKeepingWG sync.WaitGroup
	collectLoopsWG sync.WaitGroup // Separate WaitGroup for collect loops
	sendTracesWG   sync.WaitGroup
	reload         chan struct{} // Channel for config reload signals
	outgoingTraces chan sendableTrace
	done           chan struct{}

	hostname string
}

// These are the names of the metrics we use to track the number of events sent to peers through the router.
// Defining them here to avoid computing the names in the hot path of the collector.
const (
	peerRouterPeerMetricName     = "peer_router_peer"
	incomingRouterPeerMetricName = "incoming_router_peer"
)

var inMemCollectorMetrics = []metrics.Metadata{
	{Name: "trace_duration_ms", Type: metrics.Histogram, Unit: metrics.Milliseconds, Description: "time taken to process a trace from arrival to send"},
	{Name: "trace_span_count", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "number of spans in a trace"},
	{Name: "collector_incoming_queue", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "number of spans currently in the incoming queue"},
	{Name: "collector_peer_queue_length", Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "number of spans in the peer queue"},
	{Name: "collector_incoming_queue_length", Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "number of spans in the incoming queue"},
	{Name: "collector_peer_queue", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "number of spans currently in the peer queue"},
	{Name: "collector_cache_size", Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "number of traces currently stored in the trace cache"},
	{Name: "memory_heap_allocation", Type: metrics.Gauge, Unit: metrics.Bytes, Description: "current heap allocation"},
	{Name: "span_received", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of spans received by the collector"},
	{Name: "span_processed", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of spans processed by the collector"},
	{Name: "spans_waiting", Type: metrics.UpDown, Unit: metrics.Dimensionless, Description: "number of spans waiting to be processed by the collector"},
	{Name: "trace_sent_cache_hit", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of late spans received for traces that have already been sent"},
	{Name: "trace_accepted", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of new traces received by the collector"},
	{Name: "trace_send_kept", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces that has been kept"},
	{Name: "trace_send_dropped", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces that has been dropped"},
	{Name: "trace_send_has_root", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of kept traces that have a root span"},
	{Name: "trace_send_no_root", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of kept traces that do not have a root span"},
	{Name: "trace_forwarded_on_peer_change", Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "number of traces forwarded due to peer membership change"},
	{Name: "trace_send_on_shutdown", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces sent during shutdown"},
	{Name: "trace_forwarded_on_shutdown", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces forwarded during shutdown"},

	{Name: TraceSendGotRoot, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces that are ready for decision due to root span arrival"},
	{Name: TraceSendExpired, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces that are ready for decision due to TraceTimeout or SendDelay"},
	{Name: TraceSendSpanLimit, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces that are ready for decision due to span limit"},
	{Name: TraceSendEjectedFull, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces that are ready for decision due to cache capacity overrun"},
	{Name: TraceSendEjectedMemsize, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces that are ready for decision due to memory overrun"},
	{Name: TraceSendLateSpan, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of spans that are sent due to late span arrival"},

	{Name: "dropped_from_stress", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of spans dropped due to stress relief"},
	{Name: "kept_from_stress", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of spans kept due to stress relief"},
	{Name: "trace_kept_sample_rate", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "sample rate of kept traces"},
	{Name: "trace_aggregate_sample_rate", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "aggregate sample rate of both kept and dropped traces"},
	{Name: "collector_collect_loop_duration_ms", Type: metrics.Histogram, Unit: metrics.Milliseconds, Description: "duration of the collect loop, the primary event processing goroutine"},
	{Name: "collector_send_expired_traces_in_cache_dur_ms", Type: metrics.Histogram, Unit: metrics.Milliseconds, Description: "duration of sending expired traces in cache"},
	{Name: "collector_outgoing_queue", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "number of traces waiting to be send to upstream"},
	{Name: "collector_drop_decision_batch_count", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "number of drop decisions sent in a batch"},
	{Name: "collector_expired_traces_missing_decisions", Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "number of decision spans forwarded for expired traces missing trace decision"},
	{Name: "collector_expired_traces_orphans", Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "number of expired traces missing trace decision when they are sent"},
	{Name: "drop_decision_batches_received", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of drop decision batches received"},
	{Name: "kept_decision_batches_received", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of kept decision batches received"},
	{Name: "drop_decisions_received", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "total number of drop decisions received"},
	{Name: "kept_decisions_received", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "total number of kept decisions received"},
	{Name: "collector_kept_decisions_queue_full", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of times kept trace decision queue is full"},
	{Name: "collector_drop_decisions_queue_full", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of times drop trace decision queue is full"},
	{Name: "collector_cache_eviction", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of times cache eviction has occurred"},
}

func (i *InMemCollector) Start() error {
	i.Logger.Debug().Logf("Starting InMemCollector")
	defer func() { i.Logger.Debug().Logf("Finished starting InMemCollector") }()
	imcConfig := i.Config.GetCollectionConfig()

	numLoops := imcConfig.GetNumCollectLoops()

	i.Logger.Info().WithField("num_loops", numLoops).Logf("Starting InMemCollector with %d collection loops", numLoops)

	i.StressRelief.UpdateFromConfig()

	// listen for config reloads
	i.Config.RegisterReloadCallback(i.sendReloadSignal)

	// Find or create a test, make sure we signal health based (somehow)
	// on all the collect loops running.
	i.Health.Register(collectorHealthKey, i.Config.GetHealthCheckTimeout())

	for _, metric := range inMemCollectorMetrics {
		i.Metrics.Register(metric)
	}

	sampleCacheConfig := i.Config.GetSampleCacheConfig()
	var err error
	i.sampleTraceCache, err = cache.NewCuckooSentCache(sampleCacheConfig, i.Metrics)
	if err != nil {
		return err
	}

	i.outgoingTraces = make(chan sendableTrace, 100_000)
	i.done = make(chan struct{})
	i.reload = make(chan struct{}, 1)

	if i.Config.GetAddHostMetadataToTrace() {
		if hostname, err := os.Hostname(); err == nil && hostname != "" {
			i.hostname = hostname
		}
	}

	i.collectLoops = make([]*CollectLoop, numLoops)

	// Divide queue sizes among loops
	incomingPerLoop := imcConfig.GetIncomingQueueSize() / numLoops
	peerPerLoop := imcConfig.GetPeerQueueSize() / numLoops

	for loopID := range i.collectLoops {
		loop := NewCollectLoop(loopID, i, incomingPerLoop, peerPerLoop)
		i.collectLoops[loopID] = loop

		// Start the collect goroutine for this loop
		i.collectLoopsWG.Add(1)
		go loop.collect()
	}

	i.sendTracesWG.Add(1)
	go i.sendTraces()

	i.houseKeepingWG.Add(1)
	go i.houseKeeping()

	return nil
}

// sendReloadSignal will trigger the collector reloading its config, eventually.
func (i *InMemCollector) sendReloadSignal(cfgHash, ruleHash string) {
	// non-blocking insert of the signal here so we don't leak goroutines
	select {
	case i.reload <- struct{}{}:
		i.Logger.Debug().Logf("sending collect reload signal")
	default:
		i.Logger.Debug().Logf("collect already waiting to reload; skipping additional signal")
	}
}

func (i *InMemCollector) reloadConfigs() {
	i.Logger.Debug().Logf("reloading in-mem collect config")

	i.sampleTraceCache.Resize(i.Config.GetSampleCacheConfig())

	i.StressRelief.UpdateFromConfig()

	// Send reload signals to all collect loops to clear their local samplers
	// so that the new configuration will be propagated
	for _, loop := range i.collectLoops {
		select {
		case loop.reload <- struct{}{}:
		default:
			// Channel already has a signal pending, skip
		}
	}
	// TODO add resizing the LRU sent trace cache on config reload
}

func (i *InMemCollector) checkAlloc(ctx context.Context) {
	_, span := otelutil.StartSpan(ctx, i.Tracer, "checkAlloc")
	defer span.End()

	inMemConfig := i.Config.GetCollectionConfig()
	maxAlloc := inMemConfig.GetMaxAlloc()
	i.Metrics.Store("MEMORY_MAX_ALLOC", float64(maxAlloc))

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	i.Metrics.Gauge("memory_heap_allocation", float64(mem.Alloc))
	if maxAlloc == 0 || mem.Alloc < uint64(maxAlloc) {
		return
	}
	i.Metrics.Increment("collector_cache_eviction")

	// Figure out what fraction of the total cache we should remove. We'd like it to be
	// enough to get us below the max capacity, but not TOO much below.
	// Because our impact numbers are only the data size, reducing by enough to reach
	// max alloc will actually do more than that.
	totalToRemove := mem.Alloc - uint64(maxAlloc)
	perLoopToRemove := int(totalToRemove) / len(i.collectLoops)

	var wg sync.WaitGroup
	var cacheSizeBefore, cacheSizeAfter int
	wg.Add(len(i.collectLoops))
	for _, loop := range i.collectLoops {
		cacheSizeBefore += loop.GetCacheSize()
		loop.sendEarly <- sendEarly{
			wg:          &wg,
			bytesToSend: perLoopToRemove,
		}
	}
	wg.Wait()

	for _, loop := range i.collectLoops {
		cacheSizeAfter += loop.GetCacheSize()
	}

	// Treat any MaxAlloc overage as an error so we know it's happening
	i.Logger.Warn().
		WithField("alloc", mem.Alloc).
		WithField("old_trace_count", cacheSizeBefore).
		WithField("new_trace_count", cacheSizeAfter).
		Logf("Making some trace decisions early due to memory overrun.")

	// Manually GC here - without this we can easily end up evicting more than we
	// need to, since total alloc won't be updated until after a GC pass.
	runtime.GC()
	return
}

// houseKeeping listens for reload signals and calls reloadConfigs
func (i *InMemCollector) houseKeeping() {
	defer i.houseKeepingWG.Done()

	ctx := context.Background()

	ticker := i.Clock.NewTicker(100 * time.Millisecond)
	i.Health.Ready(collectorHealthKey, true)
	for {
		select {
		case <-ticker.Chan():
			i.Health.Ready(collectorHealthKey, true)

			// Aggregate metrics
			totalIncoming := 0
			totalPeer := 0
			totalCacheSize := 0

			for _, loop := range i.collectLoops {
				totalIncoming += len(loop.incoming)
				totalPeer += len(loop.fromPeer)
				totalCacheSize += loop.GetCacheSize()
			}

			i.Metrics.Histogram("collector_incoming_queue", float64(totalIncoming))
			i.Metrics.Histogram("collector_peer_queue", float64(totalPeer))
			i.Metrics.Gauge("collector_incoming_queue_length", float64(totalIncoming))
			i.Metrics.Gauge("collector_peer_queue_length", float64(totalPeer))
			i.Metrics.Gauge("collector_cache_size", float64(totalCacheSize))
			i.Metrics.Gauge("collector_num_loops", float64(len(i.collectLoops)))

			// Send traces early if we're over memory budget
			i.checkAlloc(ctx)
		case <-i.reload:
			i.reloadConfigs()
		case <-i.done:
			ticker.Stop()
			return
		}
	}
}

// getLoopForTrace determines which CollectLoop should handle a given trace ID
// using consistent hashing to ensure all spans for a trace go to the same loop
func (i *InMemCollector) getLoopForTrace(traceID string) int {
	// Hash with a seed so that we don't align with any other hashes of this
	// trace. We use a different algorithm to assign traces to nodes, but we
	// still want to minimize the risk of any synchronization beteween that
	// distribution and this one.
	hash := wyhash.Hash([]byte(traceID), 7215963184435617557)

	// Map to loop index
	loopIndex := int(hash % uint64(len(i.collectLoops)))

	return loopIndex
}

// AddSpan accepts the incoming span to a queue and returns immediately
func (i *InMemCollector) AddSpan(sp *types.Span) error {
	// Route to the appropriate loop
	loopIndex := i.getLoopForTrace(sp.TraceID)
	return i.collectLoops[loopIndex].addSpan(sp)
}

// AddSpanFromPeer accepts the incoming span from a peer to a queue and returns immediately
func (i *InMemCollector) AddSpanFromPeer(sp *types.Span) error {
	// Route to the appropriate loop
	loopIndex := i.getLoopForTrace(sp.TraceID)
	return i.collectLoops[loopIndex].addSpanFromPeer(sp)
}

// Stressed returns true if the collector is undergoing significant stress
func (i *InMemCollector) Stressed() bool {
	return i.StressRelief.Stressed()
}

func (i *InMemCollector) GetStressedSampleRate(traceID string) (rate uint, keep bool, reason string) {
	return i.StressRelief.GetSampleRate(traceID)
}

// ProcessSpanImmediately is an escape hatch used under stressful conditions --
// it submits a span for immediate transmission without enqueuing it for normal
// processing. This means it ignores dry run mode and doesn't build a complete
// trace context or cache the trace in the active trace buffer. It only gets
// called on the first span for a trace under stressful conditions; we got here
// because the StressRelief system detected that this is a new trace AND that it
// is being sampled. Therefore, we also put the traceID into the sent traces
// cache as "kept".
// It doesn't do any logging and barely touches metrics; this is about as
// minimal as we can make it.
func (i *InMemCollector) ProcessSpanImmediately(sp *types.Span) (processed bool, keep bool) {
	_, span := otelutil.StartSpanWith(context.Background(), i.Tracer, "collector.ProcessSpanImmediately", "trace_id", sp.TraceID)
	defer span.End()

	var rate uint
	record, reason, found := i.sampleTraceCache.CheckSpan(sp)
	if !found {
		rate, keep, reason = i.StressRelief.GetSampleRate(sp.TraceID)
		now := i.Clock.Now()
		trace := &types.Trace{
			APIHost:     sp.APIHost,
			APIKey:      sp.APIKey,
			Dataset:     sp.Dataset,
			TraceID:     sp.TraceID,
			ArrivalTime: now,
			SendBy:      now,
		}
		trace.SetSampleRate(rate)
		// we do want a record of how we disposed of traces in case more come in after we've
		// turned off stress relief (if stress relief is on we'll keep making the same decisions)
		i.sampleTraceCache.Record(trace, keep, reason)
	} else {
		rate = record.Rate()
		keep = record.Kept()
	}

	if !keep {
		i.Metrics.Increment("dropped_from_stress")
		return true, false
	}

	i.Metrics.Increment("kept_from_stress")
	// ok, we're sending it, so decorate it first
	sp.Data.Set(types.MetaStressed, true)
	if i.Config.GetAddRuleReasonToTrace() {
		sp.Data.Set(types.MetaRefineryReason, reason)
	}
	if i.hostname != "" {
		sp.Data.Set(types.MetaRefineryLocalHostname, i.hostname)
	}

	i.addAdditionalAttributes(sp)
	mergeTraceAndSpanSampleRates(sp, rate, i.Config.GetIsDryRun())
	i.Transmission.EnqueueSpan(sp)

	return true, true
}

// dealWithSentTrace handles a span that has arrived after the sampling decision
// on the trace has already been made, and it obeys that decision by either
// sending the span immediately or dropping it.
// This method is made public so CollectLoop can access it.
func (i *InMemCollector) dealWithSentTrace(ctx context.Context, tr cache.TraceSentRecord, keptReason string, sp *types.Span) {
	_, span := otelutil.StartSpanMulti(ctx, i.Tracer, "dealWithSentTrace", map[string]interface{}{
		"trace_id":    sp.TraceID,
		"kept_reason": keptReason,
		"hostname":    i.hostname,
	})
	defer span.End()

	if i.Config.GetAddRuleReasonToTrace() {
		var metaReason string
		if len(keptReason) > 0 {
			metaReason = fmt.Sprintf("%s - late arriving span", keptReason)
		} else {
			metaReason = "late arriving span"
		}
		sp.Data.Set(types.MetaRefineryReason, metaReason)
		sp.Data.Set(types.MetaRefinerySendReason, TraceSendLateSpan)

	}
	if i.hostname != "" {
		sp.Data.Set(types.MetaRefineryLocalHostname, i.hostname)
	}
	isDryRun := i.Config.GetIsDryRun()
	keep := tr.Kept()
	otelutil.AddSpanFields(span, map[string]interface{}{
		"keep":      keep,
		"is_dryrun": isDryRun,
	})

	if isDryRun {
		// if dry run mode is enabled, we keep all traces and mark the spans with the sampling decision
		sp.Data.Set(config.DryRunFieldName, keep)
		if !keep {
			i.Logger.Debug().WithField("trace_id", sp.TraceID).Logf("Sending span that would have been dropped, but dry run mode is enabled")
			i.Metrics.Increment(TraceSendLateSpan)
			i.addAdditionalAttributes(sp)
			i.Transmission.EnqueueSpan(sp)
			return
		}
	}
	if keep {
		i.Logger.Debug().WithField("trace_id", sp.TraceID).Logf("Sending span because of previous decision to send trace")
		mergeTraceAndSpanSampleRates(sp, tr.Rate(), isDryRun)
		// if this span is a late root span, possibly update it with our current span count
		if sp.IsRoot {
			if i.Config.GetAddCountsToRoot() {
				sp.Data.Set(types.MetaSpanEventCount, int64(tr.SpanEventCount()))
				sp.Data.Set(types.MetaSpanLinkCount, int64(tr.SpanLinkCount()))
				sp.Data.Set(types.MetaSpanCount, int64(tr.SpanCount()))
				sp.Data.Set(types.MetaEventCount, int64(tr.DescendantCount()))
			} else if i.Config.GetAddSpanCountToRoot() {
				sp.Data.Set(types.MetaSpanCount, int64(tr.DescendantCount()))
			}
		}
		otelutil.AddSpanField(span, "is_root_span", sp.IsRoot)
		i.Metrics.Increment(TraceSendLateSpan)
		i.addAdditionalAttributes(sp)
		i.Transmission.EnqueueSpan(sp)
		return
	}
	i.Logger.Debug().WithField("trace_id", sp.TraceID).Logf("Dropping span because of previous decision to drop trace")
}

func mergeTraceAndSpanSampleRates(sp *types.Span, traceSampleRate uint, dryRunMode bool) {
	tempSampleRate := sp.SampleRate
	if sp.SampleRate != 0 {
		// Write down the original sample rate so that that information
		// is more easily recovered
		sp.Data.Set(types.MetaRefineryOriginalSampleRate, int64(sp.SampleRate))
	}

	if tempSampleRate < 1 {
		// See https://docs.honeycomb.io/manage-data-volume/sampling/
		// SampleRate is the denominator of the ratio of sampled spans
		// HoneyComb treats a missing or 0 SampleRate the same as 1, but
		// behaves better/more consistently if the SampleRate is explicitly
		// set instead of inferred
		tempSampleRate = 1
	}

	// if spans are already sampled, take that into account when computing
	// the final rate
	if dryRunMode {
		sp.Data.Set("meta.dryrun.sample_rate", tempSampleRate*traceSampleRate)
		sp.SampleRate = tempSampleRate
	} else {
		sp.SampleRate = tempSampleRate * traceSampleRate
	}
}

// this is only called when a trace decision is received
// TODO it may be desirable to move this and sendTraes() into the CollectLoop.
func (i *InMemCollector) send(ctx context.Context, trace sendableTrace) {
	if trace.Sent {
		// someone else already sent this so we shouldn't also send it.
		i.Logger.Debug().
			WithString("trace_id", trace.TraceID).
			WithString("dataset", trace.Dataset).
			Logf("skipping send because someone else already sent trace to dataset")
		return
	}
	trace.Sent = true
	_, span := otelutil.StartSpan(ctx, i.Tracer, "send")
	defer span.End()

	traceDur := i.Clock.Since(trace.ArrivalTime)
	i.Metrics.Histogram("trace_duration_ms", float64(traceDur.Milliseconds()))

	logFields := logrus.Fields{
		"trace_id": trace.TraceID,
	}
	// if we're supposed to drop this trace, and dry run mode is not enabled, then we're done.
	if !trace.KeepSample && !i.Config.GetIsDryRun() {
		i.Metrics.Increment("trace_send_dropped")
		i.Logger.Info().WithFields(logFields).Logf("Dropping trace because of sampling decision")
		return
	}

	if trace.RootSpan != nil {
		rs := trace.RootSpan
		if rs != nil {
			if i.Config.GetAddCountsToRoot() {
				rs.Data.Set(types.MetaSpanEventCount, int64(trace.SpanEventCount()))
				rs.Data.Set(types.MetaSpanLinkCount, int64(trace.SpanLinkCount()))
				rs.Data.Set(types.MetaSpanCount, int64(trace.SpanCount()))
				rs.Data.Set(types.MetaEventCount, int64(trace.DescendantCount()))
			} else if i.Config.GetAddSpanCountToRoot() {
				rs.Data.Set(types.MetaSpanCount, int64(trace.DescendantCount()))
			}
		}
	}

	i.Metrics.Increment(trace.reason)
	if config.IsLegacyAPIKey(trace.APIKey) {
		logFields["dataset"] = trace.samplerSelector
	} else {
		logFields["environment"] = trace.samplerSelector
	}
	logFields["reason"] = trace.reason
	if trace.sampleKey != "" {
		logFields["sample_key"] = trace.sampleKey
	}

	i.Metrics.Increment("trace_send_kept")
	// This will observe sample rate decisions only if the trace is kept
	i.Metrics.Histogram("trace_kept_sample_rate", float64(trace.Trace.SampleRate()))

	// ok, we're not dropping this trace; send all the spans
	if i.Config.GetIsDryRun() && !trace.shouldSend {
		i.Logger.Info().WithFields(logFields).Logf("Trace would have been dropped, but sending because dry run mode is enabled")
	} else {
		i.Logger.Info().WithFields(logFields).Logf("Sending trace")
	}

	i.outgoingTraces <- trace
}

func (i *InMemCollector) Stop() error {
	i.Logger.Debug().Logf("Starting InMemCollector shutdown")

	// Signal shutdown to all components
	close(i.done)

	// signal the health system to not be ready and
	// stop liveness check so that no new traces are accepted
	i.Health.Unregister(collectorHealthKey)

	// Stop housekeeping first - we want to make sure we don't start a checkAlloc
	// after shutting down the collect loops.
	i.houseKeepingWG.Wait()

	// Close all loop input channels, which will cause the loops to stop.
	for idx, loop := range i.collectLoops {
		i.Logger.Debug().WithField("loop_id", idx).Logf("closing loop channels")
		close(loop.incoming)
		close(loop.fromPeer)
	}
	i.collectLoopsWG.Wait()

	// Stop samplers in each collect loop
	for _, loop := range i.collectLoops {
		loop.samplersMutex.RLock()
		for _, sampler := range loop.datasetSamplers {
			sampler.Stop()
		}
		loop.samplersMutex.RUnlock()
	}

	// Stop the sample trace cache
	if i.sampleTraceCache != nil {
		i.sampleTraceCache.Stop()
	}

	// Now it's safe to close the outgoing traces channel
	// No more traces will be sent to it
	close(i.outgoingTraces)
	i.sendTracesWG.Wait()

	i.Logger.Debug().Logf("InMemCollector shutdown complete")
	return nil
}

// sentRecord is a struct that holds a span and the record of the trace decision made.
type sentRecord struct {
	span   *types.Span
	record cache.TraceSentRecord
	reason string
}

func (i *InMemCollector) addAdditionalAttributes(sp *types.Span) {
	for k, v := range i.Config.GetAdditionalAttributes() {
		sp.Data.Set(k, v)
	}
}

func (i *InMemCollector) sendTraces() {
	defer i.sendTracesWG.Done()

	for t := range i.outgoingTraces {
		i.Metrics.Histogram("collector_outgoing_queue", float64(len(i.outgoingTraces)))
		_, span := otelutil.StartSpanMulti(context.Background(), i.Tracer, "sendTrace", map[string]interface{}{"num_spans": t.DescendantCount(), "outgoingTraces_size": len(i.outgoingTraces)})

		// if we have a key replacement rule, we should
		// replace the key with the new key
		keycfg := i.Config.GetAccessKeyConfig()
		overwriteWith, err := keycfg.GetReplaceKey(t.APIKey)
		if err != nil {
			i.Logger.Warn().Logf("error replacing key: %s", err.Error())
			continue
		}
		if overwriteWith != t.APIKey {
			t.APIKey = overwriteWith
		}

		for _, sp := range t.GetSpans() {

			if i.Config.GetAddRuleReasonToTrace() {
				sp.Data.Set(types.MetaRefineryReason, t.reason)
				sp.Data.Set(types.MetaRefinerySendReason, t.sendReason)
				if t.sampleKey != "" {
					sp.Data.Set(types.MetaRefinerySampleKey, t.sampleKey)
				}
			}

			// update the root span (if we have one, which we might not if the trace timed out)
			// with the final total as of our send time
			if sp.IsRoot {
				if i.Config.GetAddCountsToRoot() {
					sp.Data.Set(types.MetaSpanEventCount, int64(t.SpanEventCount()))
					sp.Data.Set(types.MetaSpanLinkCount, int64(t.SpanLinkCount()))
					sp.Data.Set(types.MetaSpanCount, int64(t.SpanCount()))
					sp.Data.Set(types.MetaEventCount, int64(t.DescendantCount()))
				} else if i.Config.GetAddSpanCountToRoot() {
					sp.Data.Set(types.MetaSpanCount, int64(t.DescendantCount()))
				}
			}

			isDryRun := i.Config.GetIsDryRun()
			if isDryRun {
				sp.Data.Set(config.DryRunFieldName, t.shouldSend)
			}
			if i.hostname != "" {
				sp.Data.Set(types.MetaRefineryLocalHostname, i.hostname)
			}
			mergeTraceAndSpanSampleRates(sp, t.SampleRate(), isDryRun)
			i.addAdditionalAttributes(sp)

			sp.APIKey = t.APIKey
			i.Transmission.EnqueueSpan(sp)
		}
		span.End()
	}
}


func (i *InMemCollector) IsMyTrace(traceID string) (sharder.Shard, bool) {
	// if trace locality is disabled, we should only process
	// traces that belong to the current refinery
	targeShard := i.Sharder.WhichShard(traceID)

	return targeShard, i.Sharder.MyShard().Equals(targeShard)

}

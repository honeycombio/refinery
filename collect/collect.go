package collect

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/generics"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/internal/otelutil"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/sharder"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
)

var ErrWouldBlock = errors.New("Dropping span as channel buffer is full. Span will not be processed and will be lost.")
var CollectorHealthKey = "collector"

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

// InMemCollector is a single threaded collector.
type InMemCollector struct {
	Config  config.Config   `inject:""`
	Logger  logger.Logger   `inject:""`
	Clock   clockwork.Clock `inject:""`
	Tracer  trace.Tracer    `inject:"tracer"`
	Health  health.Recorder `inject:""`
	Sharder sharder.Sharder `inject:""`

	Transmission     transmit.Transmission  `inject:"upstreamTransmission"`
	PeerTransmission transmit.Transmission  `inject:"peerTransmission"`
	Metrics          metrics.Metrics        `inject:"genericMetrics"`
	SamplerFactory   *sample.SamplerFactory `inject:""`
	StressRelief     StressReliever         `inject:"stressRelief"`
	Peers            peer.Peers             `inject:""`

	// For test use only
	BlockOnAddSpan bool

	// mutex must be held whenever non-channel internal fields are accessed.
	// This exists to avoid data races in tests and startup/shutdown.
	mutex sync.RWMutex
	cache cache.Cache

	datasetSamplers map[string]sample.Sampler

	sampleTraceCache cache.TraceSentCache

	incoming          chan *types.Span
	fromPeer          chan *types.Span
	reload            chan struct{}
	done              chan struct{}
	redistributeTimer *redistributeNotifier

	hostname string
}

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
	{Name: "trace_redistribution_count", Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "number of traces redistributed due to peer membership change"},
	{Name: "trace_send_on_shutdown", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces sent during shutdown"},
	{Name: "trace_forwarded_on_shutdown", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces forwarded during shutdown"},

	{Name: TraceSendGotRoot, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces that are ready for decision due to root span arrival"},
	{Name: TraceSendExpired, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces that are ready for decision due to TraceTimeout or SendDelay"},
	{Name: TraceSendSpanLimit, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces that are ready for decision due to span limit"},
	{Name: TraceSendEjectedFull, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces that are ready for decision due to cache capacity overrun"},
	{Name: TraceSendEjectedMemsize, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces that are ready for decision due to memory overrun"},
	{Name: TraceSendLateSpan, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of spans that are sent due to late span arrival"},

	{Name: "dropped_from_stress", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces dropped due to stress relief"},
	{Name: "trace_kept_sample_rate", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "sample rate of kept traces"},
	{Name: "trace_aggregate_sample_rate", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "aggregate sample rate of both kept and dropped traces"},
	{Name: "collector_redistribute_traces_duration_ms", Type: metrics.Histogram, Unit: metrics.Milliseconds, Description: "duration of redistributing traces to peers"},
}

func (i *InMemCollector) Start() error {
	i.Logger.Debug().Logf("Starting InMemCollector")
	defer func() { i.Logger.Debug().Logf("Finished starting InMemCollector") }()
	imcConfig := i.Config.GetCollectionConfig()
	i.cache = cache.NewInMemCache(imcConfig.CacheCapacity, i.Metrics, i.Logger)
	i.StressRelief.UpdateFromConfig(i.Config.GetStressReliefConfig())

	// listen for config reloads
	i.Config.RegisterReloadCallback(i.sendReloadSignal)

	i.Health.Register(CollectorHealthKey, time.Duration(imcConfig.HealthCheckTimeout))

	for _, metric := range inMemCollectorMetrics {
		i.Metrics.Register(metric)
	}

	sampleCacheConfig := i.Config.GetSampleCacheConfig()
	var err error
	i.sampleTraceCache, err = cache.NewCuckooSentCache(sampleCacheConfig, i.Metrics)
	if err != nil {
		return err
	}

	i.incoming = make(chan *types.Span, imcConfig.GetIncomingQueueSize())
	i.fromPeer = make(chan *types.Span, imcConfig.GetPeerQueueSize())
	i.Metrics.Store("INCOMING_CAP", float64(cap(i.incoming)))
	i.Metrics.Store("PEER_CAP", float64(cap(i.fromPeer)))
	i.reload = make(chan struct{}, 1)
	i.done = make(chan struct{})
	i.datasetSamplers = make(map[string]sample.Sampler)
	i.done = make(chan struct{})
	i.redistributeTimer = newRedistributeNotifier(i.Logger, i.Metrics, i.Clock)

	if i.Config.GetAddHostMetadataToTrace() {
		if hostname, err := os.Hostname(); err == nil && hostname != "" {
			i.hostname = hostname
		}
	}

	if !i.Config.GetCollectionConfig().DisableRedistribution {
		i.Peers.RegisterUpdatedPeersCallback(i.redistributeTimer.Reset)
	}

	// spin up one collector because this is a single threaded collector
	go i.collect()

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
	imcConfig := i.Config.GetCollectionConfig()

	if imcConfig.CacheCapacity != i.cache.GetCacheCapacity() {
		i.Logger.Debug().WithField("cache_size.previous", i.cache.GetCacheCapacity()).WithField("cache_size.new", imcConfig.CacheCapacity).Logf("refreshing the cache because it changed size")
		c := cache.NewInMemCache(imcConfig.CacheCapacity, i.Metrics, i.Logger)
		// pull the old cache contents into the new cache
		for j, trace := range i.cache.GetAll() {
			if j >= imcConfig.CacheCapacity {
				i.send(trace, TraceSendEjectedFull)
				continue
			}
			c.Set(trace)
		}
		i.cache = c
	} else {
		i.Logger.Debug().Logf("skipping reloading the in-memory cache on config reload because it hasn't changed capacity")
	}

	i.sampleTraceCache.Resize(i.Config.GetSampleCacheConfig())

	i.StressRelief.UpdateFromConfig(i.Config.GetStressReliefConfig())

	// clear out any samplers that we have previously created
	// so that the new configuration will be propagated
	i.datasetSamplers = make(map[string]sample.Sampler)
	// TODO add resizing the LRU sent trace cache on config reload
}

func (i *InMemCollector) checkAlloc() {
	inMemConfig := i.Config.GetCollectionConfig()
	maxAlloc := inMemConfig.GetMaxAlloc()
	i.Metrics.Store("MEMORY_MAX_ALLOC", float64(maxAlloc))

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	i.Metrics.Gauge("memory_heap_allocation", int64(mem.Alloc))
	if maxAlloc == 0 || mem.Alloc < uint64(maxAlloc) {
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
	allTraces := i.cache.GetAll()
	timeout := i.Config.GetTracesConfig().GetTraceTimeout()
	if timeout == 0 {
		timeout = 60 * time.Second
	} // Sort traces by CacheImpact, heaviest first
	sort.Slice(allTraces, func(i, j int) bool {
		return allTraces[i].CacheImpact(timeout) > allTraces[j].CacheImpact(timeout)
	})

	// Now start removing the biggest traces, by summing up DataSize for
	// successive traces until we've crossed the totalToRemove threshold
	// or just run out of traces to delete.

	cap := i.cache.GetCacheCapacity()
	i.Metrics.Gauge("collector_cache_size", cap)

	totalDataSizeSent := 0
	tracesSent := generics.NewSet[string]()
	// Send the traces we can't keep.
	for _, trace := range allTraces {
		tracesSent.Add(trace.TraceID)
		totalDataSizeSent += trace.DataSize
		i.send(trace, TraceSendEjectedMemsize)
		if totalDataSizeSent > int(totalToRemove) {
			break
		}
	}
	i.cache.RemoveTraces(tracesSent)

	// Treat any MaxAlloc overage as an error so we know it's happening
	i.Logger.Warn().
		WithField("cache_size", cap).
		WithField("alloc", mem.Alloc).
		WithField("num_traces_sent", len(tracesSent)).
		WithField("datasize_sent", totalDataSizeSent).
		WithField("new_trace_count", i.cache.GetCacheCapacity()).
		Logf("Making some trace decisions early due to memory overrun.")

	// Manually GC here - without this we can easily end up evicting more than we
	// need to, since total alloc won't be updated until after a GC pass.
	runtime.GC()
}

// AddSpan accepts the incoming span to a queue and returns immediately
func (i *InMemCollector) AddSpan(sp *types.Span) error {
	return i.add(sp, i.incoming)
}

// AddSpan accepts the incoming span to a queue and returns immediately
func (i *InMemCollector) AddSpanFromPeer(sp *types.Span) error {
	return i.add(sp, i.fromPeer)
}

// Stressed returns true if the collector is undergoing significant stress
func (i *InMemCollector) Stressed() bool {
	return i.StressRelief.Stressed()
}

func (i *InMemCollector) GetStressedSampleRate(traceID string) (rate uint, keep bool, reason string) {
	return i.StressRelief.GetSampleRate(traceID)
}

func (i *InMemCollector) add(sp *types.Span, ch chan<- *types.Span) error {
	if i.BlockOnAddSpan {
		ch <- sp
		i.Metrics.Increment("span_received")
		i.Metrics.Up("spans_waiting")
		return nil
	}

	select {
	case ch <- sp:
		i.Metrics.Increment("span_received")
		i.Metrics.Up("spans_waiting")
		return nil
	default:
		return ErrWouldBlock
	}
}

// collect handles both accepting spans that have been handed to it and sending
// the complete traces. These are done with channels in order to keep collecting
// single threaded so we don't need any locks. Actions taken from this select
// block is the only place we are allowed to modify any running data
// structures.
func (i *InMemCollector) collect() {
	tickerDuration := i.Config.GetTracesConfig().GetSendTickerValue()
	ticker := time.NewTicker(tickerDuration)
	defer ticker.Stop()

	// mutex is normally held by this goroutine at all times.
	// It is unlocked once per ticker cycle for tests.
	i.mutex.Lock()
	defer i.mutex.Unlock()

	for {
		i.Health.Ready(CollectorHealthKey, true)
		// record channel lengths as histogram but also as gauges
		i.Metrics.Histogram("collector_incoming_queue", float64(len(i.incoming)))
		i.Metrics.Histogram("collector_peer_queue", float64(len(i.fromPeer)))
		i.Metrics.Gauge("collector_incoming_queue_length", float64(len(i.incoming)))
		i.Metrics.Gauge("collector_peer_queue_length", float64(len(i.fromPeer)))

		// Always drain peer channel before doing anything else. By processing peer
		// traffic preferentially we avoid the situation where the cluster essentially
		// deadlocks because peers are waiting to get their events handed off to each
		// other.
		select {
		case <-i.done:
			return
		case <-i.redistributeTimer.Notify():
			i.redistributeTraces()
		case sp, ok := <-i.fromPeer:
			if !ok {
				// channel's been closed; we should shut down.
				return
			}
			i.processSpan(sp)
		default:
			select {
			case <-i.done:
				return
			case <-ticker.C:
				select {
				case <-i.done:
				default:
					i.sendExpiredTracesInCache(i.Clock.Now())
					i.checkAlloc()

					// Briefly unlock the cache, to allow test access.
					i.mutex.Unlock()
					runtime.Gosched()
					i.mutex.Lock()
				}
			case <-i.redistributeTimer.Notify():
				i.redistributeTraces()
			case sp, ok := <-i.incoming:
				if !ok {
					// channel's been closed; we should shut down.
					return
				}
				i.processSpan(sp)
				continue
			case sp, ok := <-i.fromPeer:
				if !ok {
					// channel's been closed; we should shut down.
					return
				}
				i.processSpan(sp)
				continue
			case <-i.reload:
				i.reloadConfigs()
			}
		}
	}
}

func (i *InMemCollector) redistributeTraces() {
	_, span := otelutil.StartSpan(context.Background(), i.Tracer, "redistributeTraces")
	redistrubutionStartTime := i.Clock.Now()

	defer func() {
		i.Metrics.Histogram("collector_redistribute_traces_duration_ms", i.Clock.Now().Sub(redistrubutionStartTime).Milliseconds())
		span.End()
	}()

	// loop through eveything in the cache of live traces
	// if it doesn't belong to this peer, we should forward it to the correct peer
	peers, err := i.Peers.GetPeers()
	if err != nil {
		i.Logger.Error().Logf("unable to get peer list with error %s", err.Error())
		return
	}
	numOfPeers := len(peers)
	if numOfPeers == 0 {
		return
	}

	traces := i.cache.GetAll()
	forwardedTraces := generics.NewSetWithCapacity[string](len(traces) / numOfPeers)
	for _, trace := range traces {
		if trace == nil {
			continue
		}

		newTarget := i.Sharder.WhichShard(trace.TraceID)

		if newTarget.Equals(i.Sharder.MyShard()) {
			if !i.Config.GetCollectionConfig().EnableTraceLocality {
				// Drop all proxy spans since peers will resend them
				trace.RemoveDecisionSpans()
			}
			continue
		}

		for _, sp := range trace.GetSpans() {
			if sp.IsDecisionSpan() {
				continue
			}

			if !i.Config.GetCollectionConfig().EnableTraceLocality {
				dc := i.createDecisionSpan(sp, trace, newTarget)
				i.PeerTransmission.EnqueueEvent(dc)
				continue
			}

			sp.APIHost = newTarget.GetAddress()

			if sp.Data == nil {
				sp.Data = make(map[string]interface{})
			}
			if v, ok := sp.Data["meta.refinery.forwarded"]; ok {
				sp.Data["meta.refinery.forwarded"] = fmt.Sprintf("%s,%s", v, i.hostname)
			} else {
				sp.Data["meta.refinery.forwarded"] = i.hostname
			}

			i.PeerTransmission.EnqueueSpan(sp)
		}

		forwardedTraces.Add(trace.TraceID)
	}

	otelutil.AddSpanFields(span, map[string]interface{}{
		"forwarded_trace_count": len(forwardedTraces.Members()),
		"total_trace_count":     len(traces),
		"hostname":              i.hostname,
	})

	i.Metrics.Gauge("trace_forwarded_on_peer_change", len(forwardedTraces))
	if len(forwardedTraces) > 0 {
		i.cache.RemoveTraces(forwardedTraces)
	}
}

func (i *InMemCollector) sendExpiredTracesInCache(now time.Time) {
	traces := i.cache.TakeExpiredTraces(now)
	spanLimit := uint32(i.Config.GetTracesConfig().SpanLimit)
	for _, t := range traces {
		if t.RootSpan != nil {
			i.send(t, TraceSendGotRoot)
		} else {
			if spanLimit > 0 && t.DescendantCount() > spanLimit {
				i.send(t, TraceSendSpanLimit)
			} else {
				i.send(t, TraceSendExpired)
			}
		}
	}
}

// processSpan does all the stuff necessary to take an incoming span and add it
// to (or create a new placeholder for) a trace.
func (i *InMemCollector) processSpan(sp *types.Span) {
	ctx := context.Background()
	defer func() {
		i.Metrics.Increment("span_processed")
		i.Metrics.Down("spans_waiting")
	}()

	tcfg := i.Config.GetTracesConfig()

	trace := i.cache.Get(sp.TraceID)
	if trace == nil {
		// if the trace has already been sent, just pass along the span
		if sr, keptReason, found := i.sampleTraceCache.CheckSpan(sp); found {
			i.Metrics.Increment("trace_sent_cache_hit")
			// bump the count of records on this trace -- if the root span isn't
			// the last late span, then it won't be perfect, but it will be better than
			// having none at all
			i.dealWithSentTrace(ctx, sr, keptReason, sp)
			return
		}
		// trace hasn't already been sent (or this span is really old); let's
		// create a new trace to hold it
		i.Metrics.Increment("trace_accepted")

		timeout := tcfg.GetTraceTimeout()
		if timeout == 0 {
			timeout = 60 * time.Second
		}

		now := i.Clock.Now()
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
		ejectedTrace := i.cache.Set(trace)
		if ejectedTrace != nil {
			i.send(ejectedTrace, TraceSendEjectedFull)
		}
	}
	// if the trace we got back from the cache has already been sent, deal with the
	// span.
	if trace.Sent {
		if sr, reason, found := i.sampleTraceCache.CheckSpan(sp); found {
			i.Metrics.Increment("trace_sent_cache_hit")
			i.dealWithSentTrace(ctx, sr, reason, sp)
			return
		}
		// trace has already been sent, but this is not in the sent cache.
		// we will just use the default late span reason as the sent reason which is
		// set inside the dealWithSentTrace function
		i.dealWithSentTrace(ctx, cache.NewKeptTraceCacheEntry(trace), "", sp)
	}

	// great! trace is live. add the span.
	trace.AddSpan(sp)

	// Figure out if we should handle this span locally or pass on to a peer
	var spanForwarded bool
	if !i.Config.GetCollectionConfig().EnableTraceLocality {
		// if this trace doesn't belong to us, we should forward a decision span to its decider
		targetShard := i.Sharder.WhichShard(trace.ID())
		if !targetShard.Equals(i.Sharder.MyShard()) && !sp.IsDecisionSpan() {
			i.Metrics.Increment("incoming_router_peer")
			i.Logger.Debug().
				WithString("peer", targetShard.GetAddress()).
				Logf("Sending span to peer")

			dc := i.createDecisionSpan(sp, trace, targetShard)

			i.PeerTransmission.EnqueueEvent(dc)
			spanForwarded = true
		}
	}

	// we may override these values in conditions below
	var markTraceForSending bool
	timeout := tcfg.GetSendDelay()
	if timeout == 0 {
		timeout = 2 * time.Second // a sensible default
	}

	// if this is a root span and its destination shard is the current refinery, say so and send the trace
	if sp.IsRoot && !spanForwarded {
		markTraceForSending = true
		trace.RootSpan = sp
	}

	// if the span count has exceeded our SpanLimit, send the trace immediately
	if tcfg.SpanLimit > 0 && uint(trace.DescendantCount()) > tcfg.SpanLimit {
		markTraceForSending = true
		timeout = 0 // don't use a timeout in this case; this is an "act fast" situation
	}

	// we should only mark a trace for sending if we are the destination shard
	if markTraceForSending && !spanForwarded {
		trace.SendBy = i.Clock.Now().Add(timeout)
	}
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

	if !i.StressRelief.ShouldSampleDeterministically(sp.TraceID) {
		otelutil.AddSpanField(span, "nondeterministic", 1)
		return false, false
	}

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
	sp.Data["meta.stressed"] = true
	if i.Config.GetAddRuleReasonToTrace() {
		sp.Data["meta.refinery.reason"] = reason
	}
	if i.hostname != "" {
		sp.Data["meta.refinery.local_hostname"] = i.hostname
	}

	i.addAdditionalAttributes(sp)
	mergeTraceAndSpanSampleRates(sp, rate, i.Config.GetIsDryRun())
	i.Transmission.EnqueueSpan(sp)

	return true, true
}

// dealWithSentTrace handles a span that has arrived after the sampling decision
// on the trace has already been made, and it obeys that decision by either
// sending the span immediately or dropping it.
func (i *InMemCollector) dealWithSentTrace(ctx context.Context, tr cache.TraceSentRecord, keptReason string, sp *types.Span) {
	_, span := otelutil.StartSpanMulti(ctx, i.Tracer, "dealWithSentTrace", map[string]interface{}{
		"trace_id":    sp.TraceID,
		"kept_reason": keptReason,
		"hostname":    i.hostname,
	})
	defer span.End()

	// if we receive a proxy span after a trace decision has been made,
	// we should just broadcast the decision again
	if sp.IsDecisionSpan() {
		// TODO: broadcast the decision again
		return
	}

	if i.Config.GetAddRuleReasonToTrace() {
		var metaReason string
		if len(keptReason) > 0 {
			metaReason = fmt.Sprintf("%s - late arriving span", keptReason)
		} else {
			metaReason = "late arriving span"
		}
		sp.Data["meta.refinery.reason"] = metaReason
		sp.Data["meta.refinery.send_reason"] = TraceSendLateSpan

	}
	if i.hostname != "" {
		sp.Data["meta.refinery.local_hostname"] = i.hostname
	}
	isDryRun := i.Config.GetIsDryRun()
	keep := tr.Kept()
	otelutil.AddSpanFields(span, map[string]interface{}{
		"keep":      keep,
		"is_dryrun": isDryRun,
	})

	if isDryRun {
		// if dry run mode is enabled, we keep all traces and mark the spans with the sampling decision
		sp.Data[config.DryRunFieldName] = keep
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
				sp.Data["meta.span_event_count"] = int64(tr.SpanEventCount())
				sp.Data["meta.span_link_count"] = int64(tr.SpanLinkCount())
				sp.Data["meta.span_count"] = int64(tr.SpanCount())
				sp.Data["meta.event_count"] = int64(tr.DescendantCount())
			} else if i.Config.GetAddSpanCountToRoot() {
				sp.Data["meta.span_count"] = int64(tr.DescendantCount())
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

	// if spans are already sampled, take that into account when computing
	// the final rate
	if dryRunMode {
		sp.Data["meta.dryrun.sample_rate"] = tempSampleRate * traceSampleRate
		sp.SampleRate = tempSampleRate
	} else {
		sp.SampleRate = tempSampleRate * traceSampleRate
	}
}

func (i *InMemCollector) send(trace *types.Trace, sendReason string) {
	if trace.Sent {
		// someone else already sent this so we shouldn't also send it.
		i.Logger.Debug().
			WithString("trace_id", trace.TraceID).
			WithString("dataset", trace.Dataset).
			Logf("skipping send because someone else already sent trace to dataset")
		return
	}
	trace.Sent = true

	traceDur := i.Clock.Since(trace.ArrivalTime)
	i.Metrics.Histogram("trace_duration_ms", float64(traceDur.Milliseconds()))
	i.Metrics.Histogram("trace_span_count", float64(trace.DescendantCount()))
	if trace.RootSpan != nil {
		i.Metrics.Increment("trace_send_has_root")
	} else {
		i.Metrics.Increment("trace_send_no_root")
	}

	i.Metrics.Increment(sendReason)

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

	// If we have a root span, update it with the count before determining the SampleRate.
	if trace.RootSpan != nil {
		rs := trace.RootSpan
		if i.Config.GetAddCountsToRoot() {
			rs.Data["meta.span_event_count"] = int64(trace.SpanEventCount())
			rs.Data["meta.span_link_count"] = int64(trace.SpanLinkCount())
			rs.Data["meta.span_count"] = int64(trace.SpanCount())
			rs.Data["meta.event_count"] = int64(trace.DescendantCount())
		} else if i.Config.GetAddSpanCountToRoot() {
			rs.Data["meta.span_count"] = int64(trace.DescendantCount())
		}
	}

	// use sampler key to find sampler; create and cache if not found
	if sampler, found = i.datasetSamplers[samplerKey]; !found {
		sampler = i.SamplerFactory.GetSamplerImplementationForKey(samplerKey, isLegacyKey)
		i.datasetSamplers[samplerKey] = sampler
	}

	// make sampling decision and update the trace
	rate, shouldSend, reason, key := sampler.GetSampleRate(trace)
	trace.SetSampleRate(rate)
	trace.KeepSample = shouldSend
	logFields["reason"] = reason
	if key != "" {
		logFields["sample_key"] = key
	}
	// This will observe sample rate attempts even if the trace is dropped
	i.Metrics.Histogram("trace_aggregate_sample_rate", float64(rate))

	i.sampleTraceCache.Record(trace, shouldSend, reason)

	// if we're supposed to drop this trace, and dry run mode is not enabled, then we're done.
	if !shouldSend && !i.Config.GetIsDryRun() {
		i.Metrics.Increment("trace_send_dropped")
		i.Logger.Info().WithFields(logFields).Logf("Dropping trace because of sampling decision")
		return
	}
	i.Metrics.Increment("trace_send_kept")
	// This will observe sample rate decisions only if the trace is kept
	i.Metrics.Histogram("trace_kept_sample_rate", float64(rate))

	// ok, we're not dropping this trace; send all the spans
	if i.Config.GetIsDryRun() && !shouldSend {
		i.Logger.Info().WithFields(logFields).Logf("Trace would have been dropped, but sending because dry run mode is enabled")
	} else {
		i.Logger.Info().WithFields(logFields).Logf("Sending trace")
	}

	for _, sp := range trace.GetSpans() {
		if sp.IsDecisionSpan() {
			continue
		}
		if i.Config.GetAddRuleReasonToTrace() {
			sp.Data["meta.refinery.reason"] = reason
			sp.Data["meta.refinery.send_reason"] = sendReason
			if key != "" {
				sp.Data["meta.refinery.sample_key"] = key
			}
		}

		// update the root span (if we have one, which we might not if the trace timed out)
		// with the final total as of our send time
		if sp.IsRoot {
			if i.Config.GetAddCountsToRoot() {
				sp.Data["meta.span_event_count"] = int64(trace.SpanEventCount())
				sp.Data["meta.span_link_count"] = int64(trace.SpanLinkCount())
				sp.Data["meta.span_count"] = int64(trace.SpanCount())
				sp.Data["meta.event_count"] = int64(trace.DescendantCount())
			} else if i.Config.GetAddSpanCountToRoot() {
				sp.Data["meta.span_count"] = int64(trace.DescendantCount())
			}
		}

		isDryRun := i.Config.GetIsDryRun()
		if isDryRun {
			sp.Data[config.DryRunFieldName] = shouldSend
		}
		if i.hostname != "" {
			sp.Data["meta.refinery.local_hostname"] = i.hostname
		}
		mergeTraceAndSpanSampleRates(sp, trace.SampleRate(), isDryRun)
		i.addAdditionalAttributes(sp)
		i.Transmission.EnqueueSpan(sp)
	}
}

func (i *InMemCollector) Stop() error {
	i.redistributeTimer.Stop()
	close(i.done)
	// signal the health system to not be ready and
	// stop liveness check
	// so that no new traces are accepted
	i.Health.Unregister(CollectorHealthKey)

	i.mutex.Lock()

	if !i.Config.GetCollectionConfig().DisableRedistribution {
		peers, err := i.Peers.GetPeers()
		if err != nil {
			i.Logger.Error().Logf("unable to get peer list with error %s", err.Error())
		}
		if len(peers) > 0 {
			i.sendTracesOnShutdown()
		}
	}

	if i.Transmission != nil {
		i.Transmission.Flush()
	}

	i.sampleTraceCache.Stop()
	i.mutex.Unlock()

	close(i.incoming)
	close(i.fromPeer)

	return nil
}

// sentRecord is a struct that holds a span and the record of the trace decision made.
type sentRecord struct {
	span   *types.Span
	record cache.TraceSentRecord
	reason string
}

// sendTracesInCache sends all traces in the cache to their final destination.
// This is done on shutdown to ensure that all traces are sent before the collector
// is stopped.
// It does this by pulling spans out of both the incoming queue and the peer queue so that
// any spans that are still in the queues when the collector is stopped are also sent.
// It also pulls traces out of the cache and sends them to their final destination.
func (i *InMemCollector) sendTracesOnShutdown() {
	wg := &sync.WaitGroup{}
	sentChan := make(chan sentRecord, len(i.incoming))
	forwardChan := make(chan *types.Span, i.Config.GetCollectionConfig().CacheCapacity)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(i.Config.GetCollectionConfig().ShutdownDelay))
	defer cancel()

	// start a goroutine that will pull spans off of the channels passed in
	// and send them to their final destination
	wg.Add(1)
	go func() {
		defer wg.Done()
		i.sendSpansOnShutdown(ctx, sentChan, forwardChan)
	}()

	// start a goroutine that will pull spans off of the incoming queue
	// and place them on the sentChan or forwardChan
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case sp, ok := <-i.incoming:
				if !ok {
					return
				}

				i.distributeSpansOnShutdown(sentChan, forwardChan, sp)
			}
		}
	}()

	// start a goroutine that will pull spans off of the peer queue
	// and place them on the sentChan or forwardChan
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case sp, ok := <-i.fromPeer:
				if !ok {
					return
				}

				i.distributeSpansOnShutdown(sentChan, forwardChan, sp)
			}
		}
	}()

	// pull traces from the trace cache and place them on the sentChan or forwardChan
	if i.cache != nil {
		traces := i.cache.GetAll()
		for _, trace := range traces {
			i.distributeSpansOnShutdown(sentChan, forwardChan, trace.GetSpans()...)
		}
	}

	wg.Wait()

	close(sentChan)
	close(forwardChan)

}

// distributeSpansInCache takes a list of spans and sends them to the appropriate channel based on the state of the trace.
func (i *InMemCollector) distributeSpansOnShutdown(sentSpanChan chan sentRecord, forwardSpanChan chan *types.Span, spans ...*types.Span) {
	for _, sp := range spans {
		// if the span is a decision span, we don't need to do anything with it
		if sp != nil && !sp.IsDecisionSpan() {

			// first check if there's a trace decision
			record, reason, found := i.sampleTraceCache.CheckSpan(sp)
			if found {
				sentSpanChan <- sentRecord{sp, record, reason}
				continue
			}

			// if there's no trace decision, then we need to forward the trace to its new home
			forwardSpanChan <- sp
		}
	}
}

// sendSpansOnShutdown is a helper function that sends span to their final destination
// on shutdown.
func (i *InMemCollector) sendSpansOnShutdown(ctx context.Context, sentSpanChan <-chan sentRecord, forwardSpanChan <-chan *types.Span) {
	sentTraces := make(map[string]struct{})
	forwardedTraces := make(map[string]struct{})

	for {
		select {
		case <-ctx.Done():
			i.Logger.Info().Logf("Timed out waiting for traces to send")
			return

		case r, ok := <-sentSpanChan:
			if !ok {
				return
			}

			ctx, span := otelutil.StartSpanMulti(ctx, i.Tracer, "shutdown_sent_span", map[string]interface{}{"trace_id": r.span.TraceID, "hostname": i.hostname})
			r.span.Data["meta.refinery.shutdown.send"] = true

			i.dealWithSentTrace(ctx, r.record, r.reason, r.span)
			_, exist := sentTraces[r.span.TraceID]
			if !exist {
				sentTraces[r.span.TraceID] = struct{}{}
				i.Metrics.Count("trace_send_on_shutdown", 1)

			}

			span.End()

		case sp, ok := <-forwardSpanChan:
			if !ok {
				return
			}

			_, span := otelutil.StartSpanMulti(ctx, i.Tracer, "shutdown_forwarded_span", map[string]interface{}{"trace_id": sp.TraceID, "hostname": i.hostname})

			targetShard := i.Sharder.WhichShard(sp.TraceID)
			url := targetShard.GetAddress()

			otelutil.AddSpanField(span, "target_shard", url)

			// TODO: we need to decorate the expired traces before forwarding them so that
			// the downstream consumers can make decisions based on the metadata without having
			// to restart the TraceTimeout or SendDelay
			sp.APIHost = url

			if sp.Data == nil {
				sp.Data = make(map[string]interface{})
			}
			if v, ok := sp.Data["meta.refinery.forwarded"]; ok {
				sp.Data["meta.refinery.forwarded"] = fmt.Sprintf("%s,%s", v, i.hostname)
			} else {
				sp.Data["meta.refinery.forwarded"] = i.hostname
			}

			i.PeerTransmission.EnqueueSpan(sp)
			_, exist := forwardedTraces[sp.TraceID]
			if !exist {
				forwardedTraces[sp.TraceID] = struct{}{}
				i.Metrics.Count("trace_forwarded_on_shutdown", 1)

			}

			span.End()
		}

	}
}

// Convenience method for tests.
func (i *InMemCollector) getFromCache(traceID string) *types.Trace {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	return i.cache.Get(traceID)
}

func (i *InMemCollector) addAdditionalAttributes(sp *types.Span) {
	for k, v := range i.Config.GetAdditionalAttributes() {
		sp.Data[k] = v
	}
}

func (i *InMemCollector) createDecisionSpan(sp *types.Span, trace *types.Trace, targetShard sharder.Shard) *types.Event {
	selector, isLegacyKey := trace.GetSamplerKey()
	if selector == "" {
		i.Logger.Error().WithField("trace_id", trace.ID()).Logf("error getting sampler selection key for trace")
	}

	sampler, found := i.datasetSamplers[selector]
	if !found {
		sampler = i.SamplerFactory.GetSamplerImplementationForKey(selector, isLegacyKey)
		i.datasetSamplers[selector] = sampler
	}

	dc := sp.ExtractDecisionContext()
	// extract all key fields from the span
	keyFields := sampler.GetKeyFields()
	for _, keyField := range keyFields {
		if val, ok := sp.Data[keyField]; ok {
			dc.Data[keyField] = val
		}
	}

	dc.APIHost = targetShard.GetAddress()
	return dc
}

func newRedistributeNotifier(logger logger.Logger, met metrics.Metrics, clock clockwork.Clock) *redistributeNotifier {
	r := &redistributeNotifier{
		initialDelay: 3 * time.Second,
		maxDelay:     30 * time.Second,
		maxAttempts:  5,
		done:         make(chan struct{}),
		clock:        clock,
		logger:       logger,
		metrics:      met,
		triggered:    make(chan struct{}),
		reset:        make(chan struct{}),
	}

	return r
}

type redistributeNotifier struct {
	clock        clockwork.Clock
	logger       logger.Logger
	initialDelay time.Duration
	maxAttempts  int
	maxDelay     time.Duration
	metrics      metrics.Metrics

	reset     chan struct{}
	done      chan struct{}
	triggered chan struct{}
	once      sync.Once
}

func (r *redistributeNotifier) Notify() <-chan struct{} {
	return r.triggered
}

func (r *redistributeNotifier) Reset() {
	var started bool
	r.once.Do(func() {
		go r.run()
		started = true
	})

	if started {
		return
	}

	select {
	case r.reset <- struct{}{}:
	case <-r.done:
		return
	default:
		r.logger.Debug().Logf("A trace redistribution is ongoing. Ignoring reset.")
	}
}

func (r *redistributeNotifier) Stop() {
	close(r.done)
}

func (r *redistributeNotifier) run() {
	var attempts int
	lastBackoff := r.initialDelay
	for {
		// if we've reached the max attempts, reset the backoff and attempts
		// only when the reset signal is received.
		if attempts >= r.maxAttempts {
			r.metrics.Gauge("trace_redistribution_count", 0)
			<-r.reset
			lastBackoff = r.initialDelay
			attempts = 0
		}
		select {
		case <-r.done:
			return
		case r.triggered <- struct{}{}:
		}

		attempts++
		r.metrics.Gauge("trace_redistribution_count", attempts)

		// Calculate the backoff interval using exponential backoff with a base time.
		backoff := time.Duration(math.Min(float64(lastBackoff)*2, float64(r.maxDelay)))
		// Add jitter to the backoff to avoid retry collisions.
		jitter := time.Duration(rand.Float64() * float64(backoff) * 0.5)
		nextBackoff := backoff + jitter
		lastBackoff = nextBackoff

		timer := r.clock.NewTimer(nextBackoff)
		select {
		case <-timer.Chan():
			timer.Stop()
		case <-r.reset:
			lastBackoff = r.initialDelay
			attempts = 0
			timer.Stop()
		case <-r.done:
			timer.Stop()
			return
		}
	}
}

package collect

import (
	"errors"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"
	"github.com/sirupsen/logrus"
)

var ErrWouldBlock = errors.New("not adding span, channel buffer is full")

type Collector interface {
	// AddSpan adds a span to be collected, buffered, and merged into a trace.
	// Once the trace is "complete", it'll be passed off to the sampler then
	// scheduled for transmission.
	AddSpan(*types.Span) error
	AddSpanFromPeer(*types.Span) error
	Stressed() bool
	GetStressedSampleRate(traceID string) (rate uint, keep bool, reason string)
	ProcessSpanImmediately(sp *types.Span, keep bool, sampleRate uint, reason string)
}

func GetCollectorImplementation(c config.Config) Collector {
	return &InMemCollector{}
}

// These are the names of the metrics we use to track our send decisions.
const (
	TraceSendGotRoot        = "trace_send_got_root"
	TraceSendExpired        = "trace_send_expired"
	TraceSendEjectedFull    = "trace_send_ejected_full"
	TraceSendEjectedMemsize = "trace_send_ejected_memsize"
)

// InMemCollector is a single threaded collector.
type InMemCollector struct {
	Config         config.Config          `inject:""`
	Logger         logger.Logger          `inject:""`
	Transmission   transmit.Transmission  `inject:"upstreamTransmission"`
	Metrics        metrics.Metrics        `inject:"genericMetrics"`
	SamplerFactory *sample.SamplerFactory `inject:""`
	StressRelief   StressReliever         `inject:"stressRelief"`

	// For test use only
	BlockOnAddSpan bool

	// mutex must be held whenever non-channel internal fields are accessed.
	// This exists to avoid data races in tests and startup/shutdown.
	mutex sync.RWMutex

	cache           cache.Cache
	datasetSamplers map[string]sample.Sampler

	sampleTraceCache cache.TraceSentCache

	incoming chan *types.Span
	fromPeer chan *types.Span
	reload   chan struct{}

	hostname string
}

func (i *InMemCollector) Start() error {
	i.Logger.Debug().Logf("Starting InMemCollector")
	defer func() { i.Logger.Debug().Logf("Finished starting InMemCollector") }()
	imcConfig, err := i.Config.GetCollectionConfig()
	if err != nil {
		return err
	}
	i.cache = cache.NewInMemCache(imcConfig.CacheCapacity, i.Metrics, i.Logger)
	i.StressRelief.UpdateFromConfig(i.Config.GetStressReliefConfig())

	// listen for config reloads
	i.Config.RegisterReloadCallback(i.sendReloadSignal)

	i.Metrics.Register("trace_duration_ms", "histogram")
	i.Metrics.Register("trace_span_count", "histogram")
	i.Metrics.Register("collector_incoming_queue", "histogram")
	i.Metrics.Register("collector_peer_queue_length", "gauge")
	i.Metrics.Register("collector_incoming_queue_length", "gauge")
	i.Metrics.Register("collector_peer_queue", "histogram")
	i.Metrics.Register("stress_level", "gauge")
	i.Metrics.Register("stress_relief_activated", "gauge")
	i.Metrics.Register("collector_cache_size", "gauge")
	i.Metrics.Register("memory_heap_allocation", "gauge")
	i.Metrics.Register("span_received", "counter")
	i.Metrics.Register("span_processed", "counter")
	i.Metrics.Register("spans_waiting", "updown")
	i.Metrics.Register("trace_sent_cache_hit", "counter")
	i.Metrics.Register("trace_accepted", "counter")
	i.Metrics.Register("trace_send_kept", "counter")
	i.Metrics.Register("trace_send_dropped", "counter")
	i.Metrics.Register("trace_send_has_root", "counter")
	i.Metrics.Register("trace_send_no_root", "counter")
	i.Metrics.Register(TraceSendGotRoot, "counter")
	i.Metrics.Register(TraceSendExpired, "counter")
	i.Metrics.Register(TraceSendEjectedFull, "counter")
	i.Metrics.Register(TraceSendEjectedMemsize, "counter")

	sampleCacheConfig := i.Config.GetSampleCacheConfig()
	i.Metrics.Register(cache.CurrentCapacity, "gauge")
	i.Metrics.Register(cache.FutureLoadFactor, "gauge")
	i.Metrics.Register(cache.CurrentLoadFactor, "gauge")
	i.sampleTraceCache, err = cache.NewCuckooSentCache(sampleCacheConfig, i.Metrics)
	if err != nil {
		return err
	}

	i.incoming = make(chan *types.Span, imcConfig.CacheCapacity*3)
	i.fromPeer = make(chan *types.Span, imcConfig.CacheCapacity*3)
	i.Metrics.Store("INCOMING_CAP", float64(cap(i.incoming)))
	i.Metrics.Store("PEER_CAP", float64(cap(i.fromPeer)))
	i.reload = make(chan struct{}, 1)
	i.datasetSamplers = make(map[string]sample.Sampler)

	if i.Config.GetAddHostMetadataToTrace() {
		if hostname, err := os.Hostname(); err == nil && hostname != "" {
			i.hostname = hostname
		}
	}

	// spin up one collector because this is a single threaded collector
	go i.collect()

	return nil
}

// sendReloadSignal will trigger the collector reloading its config, eventually.
func (i *InMemCollector) sendReloadSignal() {
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
	imcConfig, err := i.Config.GetCollectionConfig()
	if err != nil {
		i.Logger.Error().WithField("error", err).Logf("Failed to reload InMemCollector section when reloading configs")
	}

	if existingCache, ok := i.cache.(*cache.DefaultInMemCache); ok {
		if imcConfig.CacheCapacity != existingCache.GetCacheSize() {
			i.Logger.Debug().WithField("cache_size.previous", existingCache.GetCacheSize()).WithField("cache_size.new", imcConfig.CacheCapacity).Logf("refreshing the cache because it changed size")
			c := cache.NewInMemCache(imcConfig.CacheCapacity, i.Metrics, i.Logger)
			// pull the old cache contents into the new cache
			for j, trace := range existingCache.GetAll() {
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
	} else {
		i.Logger.Error().WithField("cache", i.cache.(*cache.DefaultInMemCache)).Logf("skipping reloading the cache on config reload because it's not an in-memory cache")
	}

	i.StressRelief.UpdateFromConfig(i.Config.GetStressReliefConfig())

	// clear out any samplers that we have previously created
	// so that the new configuration will be propagated
	i.datasetSamplers = make(map[string]sample.Sampler)
	// TODO add resizing the LRU sent trace cache on config reload
}

func (i *InMemCollector) checkAlloc() {
	inMemConfig, err := i.Config.GetCollectionConfig()
	maxAlloc := inMemConfig.GetMaxAlloc()
	i.Metrics.Store("MEMORY_MAX_ALLOC", float64(maxAlloc))

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	i.Metrics.Gauge("memory_heap_allocation", int64(mem.Alloc))
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
	existingCache, ok := i.cache.(*cache.DefaultInMemCache)
	if !ok {
		i.Logger.Error().WithField("alloc", mem.Alloc).Logf(
			"total allocation exceeds limit, but unable to control cache",
		)
		return
	}
	allTraces := existingCache.GetAll()
	timeout, err := i.Config.GetTraceTimeout()
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
	i.Metrics.Gauge("collector_cache_size", cap)

	totalDataSizeSent := 0
	tracesSent := make(map[string]struct{})
	// Send the traces we can't keep.
	for _, trace := range allTraces {
		tracesSent[trace.TraceID] = struct{}{}
		totalDataSizeSent += trace.DataSize
		i.send(trace, TraceSendEjectedMemsize)
		if totalDataSizeSent > int(totalToRemove) {
			break
		}
	}
	existingCache.RemoveTraces(tracesSent)

	// Treat any MaxAlloc overage as an error so we know it's happening
	i.Logger.Error().
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
	tickerDuration := i.Config.GetSendTickerValue()
	ticker := time.NewTicker(tickerDuration)
	defer ticker.Stop()

	// mutex is normally held by this goroutine at all times.
	// It is unlocked once per ticker cycle for tests.
	i.mutex.Lock()
	defer i.mutex.Unlock()

	for {
		// record channel lengths as histogram but also as gauges
		i.Metrics.Histogram("collector_incoming_queue", float64(len(i.incoming)))
		i.Metrics.Histogram("collector_peer_queue", float64(len(i.fromPeer)))
		i.Metrics.Gauge("collector_incoming_queue_length", float64(len(i.incoming)))
		i.Metrics.Gauge("collector_peer_queue_length", float64(len(i.fromPeer)))
		i.Metrics.Gauge("stress_level", float64(i.StressRelief.StressLevel()))
		if i.StressRelief.Stressed() {
			i.Metrics.Gauge("stress_relief_activated", 1)
		} else {
			i.Metrics.Gauge("stress_relief_activated", 0)
		}

		// Always drain peer channel before doing anything else. By processing peer
		// traffic preferentially we avoid the situation where the cluster essentially
		// deadlocks because peers are waiting to get their events handed off to each
		// other.
		select {
		case sp, ok := <-i.fromPeer:
			if !ok {
				// channel's been closed; we should shut down.
				return
			}
			i.processSpan(sp)
		default:
			select {
			case <-ticker.C:
				i.sendTracesInCache(time.Now())
				i.checkAlloc()

				// Briefly unlock the cache, to allow test access.
				i.mutex.Unlock()
				runtime.Gosched()
				i.mutex.Lock()
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

func (i *InMemCollector) sendTracesInCache(now time.Time) {
	traces := i.cache.TakeExpiredTraces(now)
	for _, t := range traces {
		if t.RootSpan != nil {
			i.send(t, TraceSendGotRoot)
		} else {
			i.send(t, TraceSendExpired)
		}
	}
}

// processSpan does all the stuff necessary to take an incoming span and add it
// to (or create a new placeholder for) a trace.
func (i *InMemCollector) processSpan(sp *types.Span) {
	defer func() {
		i.Metrics.Increment("span_processed")
		i.Metrics.Down("spans_waiting")
	}()

	trace := i.cache.Get(sp.TraceID)
	if trace == nil {
		// if the trace has already been sent, just pass along the span
		if sr, found := i.sampleTraceCache.Check(sp); found {
			i.Metrics.Increment("trace_sent_cache_hit")
			// bump the count of records on this trace -- if the root span isn't
			// the last late span, then it won't be perfect, but it will be better than
			// having none at all
			i.dealWithSentTrace(sr.Kept(), sr.Rate(), sr.DescendantCount(), sp)
			return
		}
		// trace hasn't already been sent (or this span is really old); let's
		// create a new trace to hold it
		i.Metrics.Increment("trace_accepted")

		timeout, err := i.Config.GetTraceTimeout()
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
			SampleRate:  sp.SampleRate, // if it had a sample rate, we want to keep it
		}
		// push this into the cache and if we eject an unsent trace, send it ASAP
		ejectedTrace := i.cache.Set(trace)
		if ejectedTrace != nil {
			i.send(ejectedTrace, TraceSendEjectedFull)
		}
	}
	// if the trace we got back from the cache has already been sent, deal with the
	// span.
	if trace.Sent {
		i.dealWithSentTrace(trace.KeepSample, trace.SampleRate, trace.DescendantCount(), sp)
	}

	// great! trace is live. add the span.
	trace.AddSpan(sp)

	// if this is a root span, send the trace
	if i.isRootSpan(sp) {
		timeout, err := i.Config.GetSendDelay()

		if err != nil {
			timeout = 2 * time.Second
		}

		trace.SendBy = time.Now().Add(timeout)
		trace.RootSpan = sp
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
func (i *InMemCollector) ProcessSpanImmediately(sp *types.Span, keep bool, sampleRate uint, reason string) {
	now := time.Now()
	trace := &types.Trace{
		APIHost:     sp.APIHost,
		APIKey:      sp.APIKey,
		Dataset:     sp.Dataset,
		TraceID:     sp.TraceID,
		ArrivalTime: now,
		SendBy:      now,
	}
	// we do want a record of how we disposed of traces in case more come in after we've
	// turned off stress relief (if stress relief is on we'll keep making the same decisions)
	i.sampleTraceCache.Record(trace, keep)
	if !keep {
		i.Metrics.Increment("dropped_from_stress")
		return
	}
	// ok, we're sending it, so decorate it first
	sp.Event.Data["meta.stressed"] = true
	if i.Config.GetAddRuleReasonToTrace() {
		sp.Event.Data["meta.refinery.reason"] = reason
	}
	if i.hostname != "" {
		sp.Data["meta.refinery.local_hostname"] = i.hostname
	}
	i.addAdditionalAttributes(sp)
	mergeTraceAndSpanSampleRates(sp, sampleRate, i.Config.GetIsDryRun())
	i.Transmission.EnqueueSpan(sp)
}

// dealWithSentTrace handles a span that has arrived after the sampling decision
// on the trace has already been made, and it obeys that decision by either
// sending the span immediately or dropping it.
func (i *InMemCollector) dealWithSentTrace(keep bool, sampleRate uint, spanCount uint, sp *types.Span) {
	if i.Config.GetAddRuleReasonToTrace() {
		sp.Data["meta.refinery.reason"] = "late"
	}
	if i.hostname != "" {
		sp.Data["meta.refinery.local_hostname"] = i.hostname
	}
	isDryRun := i.Config.GetIsDryRun()
	if isDryRun {
		// if dry run mode is enabled, we keep all traces and mark the spans with the sampling decision
		sp.Data[config.DryRunFieldName] = keep
		if !keep {
			i.Logger.Debug().WithField("trace_id", sp.TraceID).Logf("Sending span that would have been dropped, but dry run mode is enabled")
			i.addAdditionalAttributes(sp)
			i.Transmission.EnqueueSpan(sp)
			return
		}
	}
	if keep {
		i.Logger.Debug().WithField("trace_id", sp.TraceID).Logf("Sending span because of previous decision to send trace")
		mergeTraceAndSpanSampleRates(sp, sampleRate, isDryRun)
		// if this span is a late root span, possibly update it with our current span count
		if i.Config.GetAddSpanCountToRoot() && i.isRootSpan(sp) {
			sp.Data["meta.span_count"] = int64(spanCount)
		}
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

func (i *InMemCollector) isRootSpan(sp *types.Span) bool {
	for _, parentIdFieldName := range i.Config.GetParentIdFieldNames() {
		parentId := sp.Data[parentIdFieldName]
		if _, ok := parentId.(string); ok {
			return false
		}
	}
	return true
}

func (i *InMemCollector) send(trace *types.Trace, reason string) {
	if trace.Sent {
		// someone else already sent this so we shouldn't also send it.
		i.Logger.Debug().
			WithString("trace_id", trace.TraceID).
			WithString("dataset", trace.Dataset).
			Logf("skipping send because someone else already sent trace to dataset")
		return
	}
	trace.Sent = true

	traceDur := time.Since(trace.ArrivalTime)
	i.Metrics.Histogram("trace_duration_ms", float64(traceDur.Milliseconds()))
	i.Metrics.Histogram("trace_span_count", float64(trace.DescendantCount()))
	if trace.RootSpan != nil {
		i.Metrics.Increment("trace_send_has_root")
	} else {
		i.Metrics.Increment("trace_send_no_root")
	}

	i.Metrics.Increment(reason)

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
	if i.Config.GetAddSpanCountToRoot() && trace.RootSpan != nil {
		trace.RootSpan.Data["meta.span_count"] = int64(trace.DescendantCount())
	}

	// use sampler key to find sampler; create and cache if not found
	if sampler, found = i.datasetSamplers[samplerKey]; !found {
		sampler = i.SamplerFactory.GetSamplerImplementationForKey(samplerKey, isLegacyKey)
		i.datasetSamplers[samplerKey] = sampler
	}

	// make sampling decision and update the trace
	rate, shouldSend, reason, key := sampler.GetSampleRate(trace)
	trace.SampleRate = rate
	trace.KeepSample = shouldSend
	logFields["reason"] = reason
	if key != "" {
		logFields["sample_key"] = key
	}

	i.sampleTraceCache.Record(trace, shouldSend)

	// if we're supposed to drop this trace, and dry run mode is not enabled, then we're done.
	if !shouldSend && !i.Config.GetIsDryRun() {
		i.Metrics.Increment("trace_send_dropped")
		i.Logger.Info().WithFields(logFields).Logf("Dropping trace because of sampling")
		return
	}
	i.Metrics.Increment("trace_send_kept")

	// ok, we're not dropping this trace; send all the spans
	if i.Config.GetIsDryRun() && !shouldSend {
		i.Logger.Info().WithFields(logFields).Logf("Trace would have been dropped, but dry run mode is enabled")
	}
	i.Logger.Info().WithFields(logFields).Logf("Sending trace")
	for _, sp := range trace.GetSpans() {
		if i.Config.GetAddRuleReasonToTrace() {
			sp.Data["meta.refinery.reason"] = reason
			if key != "" {
				sp.Data["meta.refinery.sample_key"] = key
			}
		}

		// update the root span (if we have one, which we might not if the trace timed out)
		// with the final total as of our send time
		if i.Config.GetAddSpanCountToRoot() && i.isRootSpan(sp) {
			sp.Data["meta.span_count"] = int64(trace.DescendantCount())
		}

		isDryRun := i.Config.GetIsDryRun()
		if isDryRun {
			sp.Data[config.DryRunFieldName] = shouldSend
		}
		if i.hostname != "" {
			sp.Data["meta.refinery.local_hostname"] = i.hostname
		}
		mergeTraceAndSpanSampleRates(sp, trace.SampleRate, isDryRun)
		i.addAdditionalAttributes(sp)
		i.Transmission.EnqueueSpan(sp)
	}
}

func (i *InMemCollector) Stop() error {
	// close the incoming channel and (TODO) wait for all collectors to finish
	close(i.incoming)

	i.mutex.Lock()
	defer i.mutex.Unlock()

	// purge the collector of any in-flight traces
	if i.cache != nil {
		traces := i.cache.GetAll()
		for _, trace := range traces {
			if trace != nil {
				i.send(trace, TraceSendEjectedFull)
			}
		}
	}
	if i.Transmission != nil {
		i.Transmission.Flush()
	}

	i.sampleTraceCache.Stop()

	return nil
}

// Convenience method for tests.
func (i *InMemCollector) getFromCache(traceID string) *types.Trace {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	return i.cache.Get(traceID)
}

func (i *InMemCollector) addAdditionalAttributes(sp *types.Span) {
	for k, v := range i.Config.GetAdditionalAttributes() {
		sp.Data[k] = v
	}
}

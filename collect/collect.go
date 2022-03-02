package collect

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
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
	// AddSpan adds a span to be collected, buffered, and merged in to a trace.
	// Once the trace is "complete", it'll be passed off to the sampler then
	// scheduled for transmission.
	AddSpan(*types.Span) error
	AddSpanFromPeer(*types.Span) error
}

func GetCollectorImplementation(c config.Config) Collector {
	var collector Collector
	collectorType, err := c.GetCollectorType()
	if err != nil {
		fmt.Printf("unable to get collector type from config: %v\n", err)
		os.Exit(1)
	}
	switch collectorType {
	case "InMemCollector":
		collector = &InMemCollector{}
	default:
		fmt.Printf("unknown collector type %s. Exiting.\n", collectorType)
		os.Exit(1)
	}
	return collector
}

// InMemCollector is a single threaded collector
type InMemCollector struct {
	Config         config.Config          `inject:""`
	Logger         logger.Logger          `inject:""`
	Transmission   transmit.Transmission  `inject:"upstreamTransmission"`
	Metrics        metrics.Metrics        `inject:"metrics"`
	SamplerFactory *sample.SamplerFactory `inject:""`

	// For test use only
	BlockOnAddSpan bool

	// mutex must be held whenever non-channel internal fields are accessed.
	// This exists to avoid data races in tests and startup/shutdown.
	mutex sync.RWMutex

	cache           cache.Cache
	datasetSamplers map[string]sample.Sampler

	sentTraceCache *lru.Cache

	incoming chan *types.Span
	fromPeer chan *types.Span
	reload   chan struct{}

	hostname string
}

// traceSentRecord is the bit we leave behind when sending a trace to remember
// our decision for the future, so any delinquent spans that show up later can
// be dropped or passed along.
type traceSentRecord struct {
	keep bool // true if the trace was kept, false if it was dropped
	rate uint // sample rate used when sending the trace
}

func (i *InMemCollector) Start() error {
	i.Logger.Debug().Logf("Starting InMemCollector")
	defer func() { i.Logger.Debug().Logf("Finished starting InMemCollector") }()
	imcConfig, err := i.Config.GetInMemCollectorCacheCapacity()
	if err != nil {
		return err
	}
	i.cache = cache.NewInMemCache(imcConfig.CacheCapacity, i.Metrics, i.Logger)

	// listen for config reloads
	i.Config.RegisterReloadCallback(i.sendReloadSignal)

	i.Metrics.Register("trace_duration_ms", "histogram")
	i.Metrics.Register("trace_span_count", "histogram")
	i.Metrics.Register("collector_tosend_queue", "histogram")
	i.Metrics.Register("collector_incoming_queue", "histogram")
	i.Metrics.Register("collector_peer_queue", "histogram")
	i.Metrics.Register("trace_sent_cache_hit", "counter")
	i.Metrics.Register("trace_accepted", "counter")
	i.Metrics.Register("trace_send_kept", "counter")
	i.Metrics.Register("trace_send_dropped", "counter")
	i.Metrics.Register("trace_send_has_root", "counter")
	i.Metrics.Register("trace_send_no_root", "counter")

	stc, err := lru.New(imcConfig.CacheCapacity * 5) // keep 5x ring buffer size
	if err != nil {
		return err
	}
	i.sentTraceCache = stc

	i.incoming = make(chan *types.Span, imcConfig.CacheCapacity*3)
	i.fromPeer = make(chan *types.Span, imcConfig.CacheCapacity*3)
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
	imcConfig, err := i.Config.GetInMemCollectorCacheCapacity()
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
					i.send(trace)
					continue
				}
				c.Set(trace)
			}
			i.cache = c
		} else {
			i.Logger.Debug().Logf("skipping reloading the cache on config reload because it hasn't changed capacity")
		}
	} else {
		i.Logger.Error().WithField("cache", i.cache.(*cache.DefaultInMemCache)).Logf("skipping reloading the cache on config reload because it's not an in-memory cache")
	}

	// clear out any samplers that we have previously created
	// so that the new configuration will be propagated
	i.datasetSamplers = make(map[string]sample.Sampler)
	// TODO add resizing the LRU sent trace cache on config reload
}

func (i *InMemCollector) checkAlloc() {
	inMemConfig, err := i.Config.GetInMemCollectorCacheCapacity()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	if err != nil || inMemConfig.MaxAlloc == 0 || mem.Alloc < inMemConfig.MaxAlloc {
		return
	}

	existingCache, ok := i.cache.(*cache.DefaultInMemCache)
	if !ok || existingCache.GetCacheSize() < 100 {
		i.Logger.Error().WithField("alloc", mem.Alloc).Logf(
			"total allocation exceeds limit, but unable to shrink cache",
		)
		return
	}

	// Reduce cache size by a fixed 10%, successive overages will continue to shrink.
	// Base this on the total number of actual traces, which may be fewer than
	// the cache capacity.
	oldCap := existingCache.GetCacheSize()
	oldTraces := existingCache.GetAll()
	newCap := int(float64(len(oldTraces)) * 0.9)

	// Treat any MaxAlloc overage as an error. The configured cache capacity
	// should be reduced to avoid this condition.
	i.Logger.Error().
		WithField("cache_size.previous", oldCap).
		WithField("cache_size.new", newCap).
		WithField("alloc", mem.Alloc).
		Logf("reducing cache size due to memory overage")

	c := cache.NewInMemCache(newCap, i.Metrics, i.Logger)

	// Sort traces by deadline, oldest first.
	sort.Slice(oldTraces, func(i, j int) bool {
		return oldTraces[i].SendBy.Before(oldTraces[j].SendBy)
	})

	// Send the traces we can't keep, put the rest into the new cache.
	for _, trace := range oldTraces[:len(oldTraces)-newCap] {
		i.send(trace)
	}
	for _, trace := range oldTraces[len(oldTraces)-newCap:] {
		c.Set(trace)
	}
	i.cache = c

	// Manually GC here - it's unfortunate to block this long, but without
	// this we can easily end up shrinking more than we need to, since total
	// alloc won't be updated until after a GC pass.
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

func (i *InMemCollector) add(sp *types.Span, ch chan<- *types.Span) error {
	if i.BlockOnAddSpan {
		ch <- sp
		return nil
	}

	select {
	case ch <- sp:
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
		// record channel lengths
		i.Metrics.Histogram("collector_incoming_queue", float64(len(i.incoming)))
		i.Metrics.Histogram("collector_peer_queue", float64(len(i.fromPeer)))

		// Always drain peer channel before doing anyhting else. By processing peer
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
		i.send(t)
	}
}

// processSpan does all the stuff necessary to take an incoming span and add it
// to (or create a new placeholder for) a trace.
func (i *InMemCollector) processSpan(sp *types.Span) {
	trace := i.cache.Get(sp.TraceID)
	if trace == nil {
		// if the trace has already been sent, just pass along the span
		if sentRecord, found := i.sentTraceCache.Get(sp.TraceID); found {
			if sr, ok := sentRecord.(*traceSentRecord); ok {
				i.Metrics.Increment("trace_sent_cache_hit")
				i.dealWithSentTrace(sr.keep, sr.rate, sp)
				return
			}
		}
		// trace hasn't already been sent (or this span is really old); let's
		// create a new trace to hold it
		i.Metrics.Increment("trace_accepted")

		timeout, err := i.Config.GetTraceTimeout()
		if err != nil {
			timeout = 60 * time.Second
		}

		trace = &types.Trace{
			APIHost:   sp.APIHost,
			APIKey:    sp.APIKey,
			Dataset:   sp.Dataset,
			TraceID:   sp.TraceID,
			StartTime: time.Now(),
			SendBy:    time.Now().Add(timeout),
		}
		// push this into the cache and if we eject an unsent trace, send it ASAP
		ejectedTrace := i.cache.Set(trace)
		if ejectedTrace != nil {
			i.send(ejectedTrace)
		}
	}
	// if the trace we got back from the cache has already been sent, deal with the
	// span.
	if trace.Sent == true {
		i.dealWithSentTrace(trace.KeepSample, trace.SampleRate, sp)
	}

	// great! trace is live. add the span.
	trace.AddSpan(sp)

	// if this is a root span, send the trace
	if isRootSpan(sp) {
		timeout, err := i.Config.GetSendDelay()

		if err != nil {
			timeout = 2 * time.Second
		}

		trace.SendBy = time.Now().Add(timeout)
		trace.HasRootSpan = true
	}
}

// dealWithSentTrace handles a span that has arrived after the sampling decision
// on the trace has already been made, and it obeys that decision by either
// sending the span immediately or dropping it.
func (i *InMemCollector) dealWithSentTrace(keep bool, sampleRate uint, sp *types.Span) {
	if i.Config.GetIsDryRun() {
		field := i.Config.GetDryRunFieldName()
		// if dry run mode is enabled, we keep all traces and mark the spans with the sampling decision
		sp.Data[field] = keep
		if !keep {
			i.Logger.Debug().WithField("trace_id", sp.TraceID).Logf("Sending span that would have been dropped, but dry run mode is enabled")
			i.Transmission.EnqueueSpan(sp)
			return
		}
	}
	if keep {
		i.Logger.Debug().WithField("trace_id", sp.TraceID).Logf("Sending span because of previous decision to send trace")
		sp.SampleRate *= sampleRate
		i.Transmission.EnqueueSpan(sp)
		return
	}
	i.Logger.Debug().WithField("trace_id", sp.TraceID).Logf("Dropping span because of previous decision to drop trace")
}

func isRootSpan(sp *types.Span) bool {
	parentID := sp.Data["trace.parent_id"]
	if parentID == nil {
		parentID = sp.Data["parentId"]
		if parentID == nil {
			// no parent ID present; it's a root span
			return true
		}
	}
	return false
}

func (i *InMemCollector) send(trace *types.Trace) {
	if trace.Sent == true {
		// someone else already sent this so we shouldn't also send it. This happens
		// when two timers race and two signals for the same trace are sent down the
		// toSend channel
		i.Logger.Debug().
			WithString("trace_id", trace.TraceID).
			WithString("dataset", trace.Dataset).
			Logf("skipping send because someone else already sent trace to dataset")
		return
	}
	trace.Sent = true

	traceDur := time.Now().Sub(trace.StartTime)
	i.Metrics.Histogram("trace_duration_ms", float64(traceDur.Milliseconds()))
	i.Metrics.Histogram("trace_span_count", float64(len(trace.GetSpans())))
	if trace.HasRootSpan {
		i.Metrics.Increment("trace_send_has_root")
	} else {
		i.Metrics.Increment("trace_send_no_root")
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

	// use sampler key to find sampler, crete and cache if not found
	if sampler, found = i.datasetSamplers[samplerKey]; !found {
		sampler = i.SamplerFactory.GetSamplerImplementationForDataset(samplerKey)
		i.datasetSamplers[samplerKey] = sampler
	}

	// make sampling decision and update the trace
	rate, shouldSend := sampler.GetSampleRate(trace)
	trace.SampleRate = rate
	trace.KeepSample = shouldSend

	// record this decision in the sent record LRU for future spans
	sentRecord := traceSentRecord{
		keep: shouldSend,
		rate: rate,
	}
	i.sentTraceCache.Add(trace.TraceID, &sentRecord)

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
		if sp.SampleRate < 1 {
			sp.SampleRate = 1
		}
		if i.Config.GetIsDryRun() {
			field := i.Config.GetDryRunFieldName()
			sp.Data[field] = shouldSend
		}
		if i.hostname != "" {
			sp.Data["meta.refinery.local_hostname"] = i.hostname
		}
		// if spans are already sampled, take that in to account when computing
		// the final rate
		sp.SampleRate *= trace.SampleRate
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
				i.send(trace)
			}
		}
	}
	if i.Transmission != nil {
		i.Transmission.Flush()
	}
	return nil
}

// Convenience method for tests.
func (i *InMemCollector) getFromCache(traceID string) *types.Trace {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	return i.cache.Get(traceID)
}

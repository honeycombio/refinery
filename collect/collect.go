package collect

import (
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/honeycombio/samproxy/collect/cache"
	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/metrics"
	"github.com/honeycombio/samproxy/sample"
	"github.com/honeycombio/samproxy/transmit"
	"github.com/honeycombio/samproxy/types"
)

type Collector interface {
	// AddSpan adds a span to be collected, buffered, and merged in to a trace.
	// Once the trace is "complete", it'll be passed off to the sampler then
	// scheduled for transmission.
	AddSpan(*types.Span)
	AddSpanFromPeer(*types.Span)
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
	Metrics        metrics.Metrics        `inject:""`
	SamplerFactory *sample.SamplerFactory `inject:""`

	Cache           cache.Cache
	datasetSamplers map[string]sample.Sampler
	defaultSampler  sample.Sampler

	sentTraceCache *lru.Cache

	incoming chan *types.Span
	fromPeer chan *types.Span
	reload   chan struct{}
}

type imcConfig struct {
	CacheCapacity int
}

// traceSentRecord is the bit we leave behind when sending a trace to remember
// our decision for the future, so any delinquent spans that show up later can
// be dropped or passed along.
type traceSentRecord struct {
	keep bool // true if the trace was kept, false if it was dropped
	rate uint // sample rate used when sending the trace
}

func (i *InMemCollector) Start() error {
	i.Logger.Debugf("Starting InMemCollector")
	defer func() { i.Logger.Debugf("Finished starting InMemCollector") }()
	i.defaultSampler = i.SamplerFactory.GetDefaultSamplerImplementation()
	imcConfig := &imcConfig{}
	err := i.Config.GetOtherConfig("InMemCollector", imcConfig)
	if err != nil {
		return err
	}
	capacity := imcConfig.CacheCapacity
	if capacity > math.MaxInt32 {
		return errors.New(fmt.Sprintf("maximum cache capacity is %d", math.MaxInt32))
	}
	c := &cache.DefaultInMemCache{
		Config: cache.CacheConfig{
			CacheCapacity: capacity,
		},
		Metrics: i.Metrics,
		Logger:  i.Logger,
	}
	c.Start()
	i.Cache = c

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
	i.Metrics.Register("peer_queue_too_large", "counter")

	stc, err := lru.New(capacity * 5) // keep 5x ring buffer size
	if err != nil {
		return err
	}
	i.sentTraceCache = stc

	i.incoming = make(chan *types.Span, capacity*3)
	i.fromPeer = make(chan *types.Span, capacity*3)
	i.reload = make(chan struct{}, 1)
	// spin up one collector because this is a single threaded collector
	go i.collect()

	return nil
}

// sendReloadSignal will trigger the collector reloading its config, eventually.
func (i *InMemCollector) sendReloadSignal() {
	// non-blocking insert of the signal here so we don't leak goroutines
	select {
	case i.reload <- struct{}{}:
		i.Logger.Debugf("sending collect reload signal")
	default:
		i.Logger.Debugf("collect already waiting to reload; skipping additional signal")
	}
}

func (i *InMemCollector) reloadConfigs() {
	i.Logger.Debugf("reloading in-mem collect config")
	imcConfig := &imcConfig{}
	err := i.Config.GetOtherConfig("InMemCollector", imcConfig)
	if err != nil {
		i.Logger.WithField("error", err).Errorf("Failed to reload InMemCollector section when reloading configs")
	}
	capacity := imcConfig.CacheCapacity

	if existingCache, ok := i.Cache.(*cache.DefaultInMemCache); ok {
		if capacity != existingCache.GetCacheSize() {
			i.Logger.WithField("cache_size.previous", existingCache.GetCacheSize()).WithField("cache_size.new", capacity).Debugf("refreshing the cache because it changed size")
			c := &cache.DefaultInMemCache{
				Config: cache.CacheConfig{
					CacheCapacity: capacity,
				},
				Metrics: i.Metrics,
			}
			c.Start()
			// pull the old cache contents into the new cache
			for i, trace := range existingCache.GetAll() {
				if i > capacity {
					break
				}
				c.Set(trace)
			}
			i.Cache = c
		} else {
			i.Logger.Debugf("skipping reloading the cache on config reload because it hasn't changed capacity")
		}
	} else {
		i.Logger.WithField("cache", i.Cache.(*cache.DefaultInMemCache)).Errorf("skipping reloading the cache on config reload because it's not an in-memory cache")
	}
	// TODO add resizing the LRU sent trace cache on config reload
}

// AddSpan accepts the incoming span to a queue and returns immediately
func (i *InMemCollector) AddSpan(sp *types.Span) {
	// TODO protect against sending on a closed channel during shutdown
	i.incoming <- sp
}

// AddSpan accepts the incoming span to a queue and returns immediately
func (i *InMemCollector) AddSpanFromPeer(sp *types.Span) {
	// TODO protect against sending on a closed channel during shutdown
	i.fromPeer <- sp
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

	peerChanSize := cap(i.fromPeer)

	for {
		// record channel lengths
		i.Metrics.Histogram("collector_incoming_queue", float64(len(i.incoming)))
		i.Metrics.Histogram("collector_peer_queue", float64(len(i.fromPeer)))

		select {
		case <-ticker.C:
			i.sendTracesInCache(time.Now())
		default:
		}

		// process peer traffic at 2/1 ratio to incoming traffic. By processing peer
		// traffic preferentially we avoid the situation where the cluster essentially
		// deadlocks because peers are waiting to get their events handed off to each
		// other.
		select {
		case <-ticker.C:
			i.sendTracesInCache(time.Now())
		case sp, ok := <-i.fromPeer:
			if !ok {
				// channel's been closed; we should shut down.
				return
			}
			i.processSpan(sp)
			// additionally, if the peer channel is more than 80% full, restart this
			// loop to make sure it stays empty enough. We _really_ want to avoid
			// blocking peer traffic.
			if len(i.fromPeer)*100/peerChanSize > 80 {
				i.Metrics.IncrementCounter("peer_queue_too_large")
				continue
			}
		default:
		}

		// ok, the peer queue is low enough, let's wait for new events from anywhere
		select {
		case <-ticker.C:
			i.sendTracesInCache(time.Now())
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

func (i *InMemCollector) sendTracesInCache(now time.Time) {
	traces := i.Cache.GetAll()

	for _, t := range traces {
		if t != nil {
			if now.After(t.SendBy) {
				i.send(t)
			}
		}
	}
}

// processSpan does all the stuff necessary to take an incoming span and add it
// to (or create a new placeholder for) a trace.
func (i *InMemCollector) processSpan(sp *types.Span) {
	trace := i.Cache.Get(sp.TraceID)
	if trace == nil {
		// if the trace has already been sent, just pass along the span
		if sentRecord, found := i.sentTraceCache.Get(sp.TraceID); found {
			if sr, ok := sentRecord.(*traceSentRecord); ok {
				i.Metrics.IncrementCounter("trace_sent_cache_hit")
				i.dealWithSentTrace(sr.keep, sr.rate, sp)
				return
			}
		}
		// trace hasn't already been sent (or this span is really old); let's
		// create a new trace to hold it
		i.Metrics.IncrementCounter("trace_accepted")

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
		ejectedTrace := i.Cache.Set(trace)
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
	}
}

// dealWithSentTrace handles a span that has arrived after the sampling decision
// on the trace has already been made, and it obeys that decision by either
// sending the span immediately or dropping it.
func (i *InMemCollector) dealWithSentTrace(keep bool, sampleRate uint, sp *types.Span) {
	if i.Config.GetIsDryRun() {
		// if dry run mode is enabled, we keep all traces and mark the spans with the sampling decision
		sp.Data["samproxy_kept"] = keep
		if !keep {
			i.Logger.WithField("trace_id", sp.TraceID).Debugf("Sending span that would have been dropped, but dry run mode is enabled")
			i.Transmission.EnqueueSpan(sp)
			return
		}
	}
	if keep {
		i.Logger.WithField("trace_id", sp.TraceID).Debugf("Sending span because of previous decision to send trace")
		sp.SampleRate *= sampleRate
		i.Transmission.EnqueueSpan(sp)
		return
	}
	i.Logger.WithField("trace_id", sp.TraceID).Debugf("Dropping span because of previous decision to drop trace")
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
		i.Logger.
			WithField("trace_id", trace.TraceID).
			WithField("dataset", trace.Dataset).
			Debugf("skipping send because someone else already sent trace to dataset")
		return
	}
	trace.Sent = true

	traceDur := time.Now().Sub(trace.StartTime)
	i.Metrics.Histogram("trace_duration_ms", float64(traceDur.Milliseconds()))
	i.Metrics.Histogram("trace_span_count", float64(len(trace.GetSpans())))

	var sampler sample.Sampler
	var found bool

	if sampler, found = i.datasetSamplers[trace.Dataset]; !found {
		sampler = i.SamplerFactory.GetSamplerImplementationForDataset(trace.Dataset)
		// no dataset sampler found, use default sampler
		if sampler == nil {
			sampler = i.defaultSampler
		}

		if i.datasetSamplers == nil {
			i.datasetSamplers = make(map[string]sample.Sampler)
		}

		// save sampler for later
		i.datasetSamplers[trace.Dataset] = sampler
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
		i.Metrics.IncrementCounter("trace_send_dropped")
		i.Logger.WithField("trace_id", trace.TraceID).WithField("dataset", trace.Dataset).Infof("Dropping trace because of sampling, trace to dataset")
		return
	}
	i.Metrics.IncrementCounter("trace_send_kept")

	// ok, we're not dropping this trace; send all the spans
	if i.Config.GetIsDryRun() && !shouldSend {
		i.Logger.WithField("trace_id", trace.TraceID).WithField("dataset", trace.Dataset).Infof("Trace would have been dropped, but dry run mode is enabled")
	}
	i.Logger.WithField("trace_id", trace.TraceID).WithField("dataset", trace.Dataset).Infof("Sending trace to dataset")
	for _, sp := range trace.GetSpans() {
		if sp.SampleRate < 1 {
			sp.SampleRate = 1
		}
		if i.Config.GetIsDryRun() {
			sp.Data["samproxy_kept"] = shouldSend
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
	// purge the collector of any in-flight traces
	if i.Cache != nil {
		traces := i.Cache.GetAll()
		for _, trace := range traces {
			if trace != nil {
				if !trace.GetSent() {
					i.send(trace)
				}
			}
		}
	}
	if i.Transmission != nil {
		i.Transmission.Flush()
	}
	return nil
}

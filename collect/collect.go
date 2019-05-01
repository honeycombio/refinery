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
	toSend   chan *sendSignal
	reload   chan struct{}
}

// sendSignal is an indicator that it's time to send a trace.
type sendSignal struct {
	trace *types.Trace
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
	}
	c.Start()
	i.Cache = c

	// listen for config reloads
	i.Config.RegisterReloadCallback(i.sendReloadSignal)

	i.Metrics.Register("trace_duration", "histogram")
	i.Metrics.Register("trace_num_spans", "histogram")
	i.Metrics.Register("collector_incoming_queue", "histogram")
	i.Metrics.Register("collector_peer_queue", "histogram")
	i.Metrics.Register("trace_sent_cache_hit", "counter")
	i.Metrics.Register("trace_accepted", "counter")
	i.Metrics.Register("trace_send_kept", "counter")
	i.Metrics.Register("trace_send_dropped", "counter")

	stc, err := lru.New(capacity * 5) // keep 5x ring buffer size
	if err != nil {
		return err
	}
	i.sentTraceCache = stc

	i.incoming = make(chan *types.Span, capacity*3)
	i.toSend = make(chan *sendSignal, capacity)
	// spin up one collector because this is a single threaded collector
	go i.collect()

	return nil
}

// instruct
func (i *InMemCollector) sendReloadSignal() {
	i.reload <- struct{}{}
}

func (i *InMemCollector) reloadConfigs() {
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
	// make sure we send all pending traces when we finish
	defer func() {
		for sig := range i.toSend {
			i.send(sig.trace)
		}
	}()

	peerChanSize := cap(i.fromPeer)

	for {
		// record channel lengths
		i.Metrics.Histogram("collector_tosend_queue", float64(len(i.toSend)))
		i.Metrics.Histogram("collector_incoming_queue", float64(len(i.incoming)))
		i.Metrics.Histogram("collector_peer_queue", float64(len(i.fromPeer)))

		// process traces that are ready to send first to make sure we can clear out
		// any stuck queues that are eligible for clearing
		select {
		case sig, ok := <-i.toSend:
			if ok {
				i.send(sig.trace)
			}
		default:
		}

		// process peer traffic at 2/1 ratio to incoming traffic. By processing peer
		// traffic preferentially we avoid the situation where the cluster essentially
		// deadlocks because peers are waiting to get their events handed off to each
		// other.
		select {
		case sig := <-i.toSend:
			i.send(sig.trace)
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
				continue
			}
		default:
		}

		// ok, the peer queue is low enough, let's wait for new events from anywhere
		select {
		case sig, ok := <-i.toSend:
			if ok {
				i.send(sig.trace)
			}
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
		cancel := make(chan struct{})
		trace = &types.Trace{
			APIHost:       sp.APIHost,
			APIKey:        sp.APIKey,
			Dataset:       sp.Dataset,
			TraceID:       sp.TraceID,
			StartTime:     time.Now(),
			CancelSending: cancel,
		}
		// push this into the cache and if we eject an unsent trace, send it ASAP
		ejectedTrace := i.Cache.Set(trace)
		if ejectedTrace != nil {
			i.sendASAP(ejectedTrace)
		}
		// start up the overall trace timeout
		i.sendAfterTraceTimeout(trace)
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
		i.sendAfterRootDelay(trace)
	}
}

// dealWithSentTrace handles a span that has arrived after the sampling decision
// on the trace has already been made, and it obeys that decision by either
// sending the span immediately or dropping it.
func (i *InMemCollector) dealWithSentTrace(keep bool, sampleRate uint, sp *types.Span) {
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

// sendASAP sends a trace as soon as possible, aka no delay.
func (i *InMemCollector) sendASAP(trace *types.Trace) {
	go i.pauseAndSend(0, trace)
}

// sendAfterTraceTimeout will wait for the trace timeout to expire then send (if
// it has not already been sent). It should be called in a goroutine, not
// synchronously.
func (i *InMemCollector) sendAfterTraceTimeout(trace *types.Trace) {
	traceTimeout, err := i.Config.GetTraceTimeout()
	if err != nil {
		i.Logger.Errorf("failed to get trace timeout. pausing for 60 seconds")
		traceTimeout = 60
	}
	dur := time.Duration(traceTimeout) * time.Second
	go i.pauseAndSend(dur, trace)
}

// if the configuration says "send the trace when no new spans have come in for
// X seconds" this function will cancel all outstanding send timers and start a
// new one. To prevent infinitely postponed traces, there is still the (TODO)
// total number of spans cap and a (TODO) gloabal time since first seen cap.
//
// TODO this is not yet actually implemented, but leaving the function here as a
// reminder that it'd be an interesting config to add.
func (i *InMemCollector) sendAfterIdleTimeout(trace *types.Trace) {
	// cancel all outstanding sending timers
	close(trace.CancelSending)

	// get the configured delay
	spanSeenDelay, err := i.Config.GetSpanSeenDelay()
	if err != nil {
		i.Logger.Errorf("failed to get send delay. pausing for 2 seconds")
		spanSeenDelay = 2
	}

	// make a new cancel sending channel and then wait on it
	trace.CancelSending = make(chan struct{})
	dur := time.Duration(spanSeenDelay) * time.Second
	go i.pauseAndSend(dur, trace)
}

// sendAfterRootDelay waits the SendDelay timeout then registers the trace to be
// sent.
func (i *InMemCollector) sendAfterRootDelay(trace *types.Trace) {
	sendDelay, err := i.Config.GetSendDelay()
	if err != nil {
		i.Logger.Errorf("failed to get send delay. pausing for 2 seconds")
		sendDelay = 2
	}
	dur := time.Duration(sendDelay) * time.Second
	go i.pauseAndSend(dur, trace)
}

// pauseAndSend will block for pause time and then send the trace.
func (i *InMemCollector) pauseAndSend(pause time.Duration, trace *types.Trace) {
	select {
	case <-time.After(pause):
		// TODO fix FinishTime to be the time of the last span + its duration rather
		// than whenever the timer goes off.
		trace.FinishTime = time.Now()
		// close the channel so all other timers expire
		close(trace.CancelSending)
		i.Logger.
			WithField("trace_id", trace.TraceID).
			WithField("pause_dur", pause).
			Debugf("pauseAndSend wait finished; sending trace.")
		i.toSend <- &sendSignal{trace}

	case <-trace.CancelSending:
		// CancelSending channel is closed, meaning someone else sent the trace.
		// Let's stop waiting and bail.
	}
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

	// we're sending this trace, bump the counter
	i.Metrics.IncrementCounter("trace_sent")
	i.Metrics.Histogram("trace_span_count", float64(len(trace.GetSpans())))

	traceDur := float64(trace.FinishTime.Sub(trace.StartTime) / time.Millisecond)
	i.Metrics.Histogram("trace_duration_ms", traceDur)

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

	// if we're supposed to drop this trace, then we're done.
	if !shouldSend {
		i.Metrics.IncrementCounter("trace_send_dropped")
		i.Logger.WithField("trace_id", trace.TraceID).WithField("dataset", trace.Dataset).Infof("Dropping trace because of sampling, trace to dataset")
		return
	}
	i.Metrics.IncrementCounter("trace_send_kept")

	// ok, we're not dropping this trace; send all the spans
	i.Logger.WithField("trace_id", trace.TraceID).WithField("dataset", trace.Dataset).Infof("Sending trace to dataset")
	for _, sp := range trace.GetSpans() {
		if sp.SampleRate < 1 {
			sp.SampleRate = 1
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

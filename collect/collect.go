package collect

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
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

type InMemCollector struct {
	Config         config.Config          `inject:""`
	Logger         logger.Logger          `inject:""`
	Transmission   transmit.Transmission  `inject:"upstreamTransmission"`
	Metrics        metrics.Metrics        `inject:""`
	SamplerFactory *sample.SamplerFactory `inject:""`

	cacheLock       sync.Mutex
	Cache           cache.Cache
	datasetSamplers map[string]sample.Sampler
	dsLock          sync.Mutex
	defaultSampler  sample.Sampler

	sentTraceCache *lru.Cache

	incoming chan *types.Span
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
	i.Config.RegisterReloadCallback(i.reloadConfigs)

	i.Metrics.Register("trace_duration", "histogram")
	i.Metrics.Register("trace_num_spans", "histogram")
	i.Metrics.Register("trace_sent_cache_hit", "counter")
	i.Metrics.Register("trace_accepted", "counter")
	i.Metrics.Register("trace_send_kept", "counter")
	i.Metrics.Register("trace_send_dropped", "counter")

	stc, err := lru.New(capacity * 5) // keep 5x ring buffer size
	if err != nil {
		return err
	}
	i.sentTraceCache = stc

	// spin up 8 cancellable span accumulators (because c5.2xl have 8 CPUs)
	i.incoming = make(chan *types.Span, capacity)
	for iter := 0; iter < 8; iter++ {
		go i.collect()
	}

	return nil
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

// collect handles all spans that have been added to the incoming channel
func (i *InMemCollector) collect() {
	for sp := range i.incoming {
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
			ctx, cancel := context.WithCancel(context.Background())
			trace = &types.Trace{
				APIHost:       sp.APIHost,
				APIKey:        sp.APIKey,
				Dataset:       sp.Dataset,
				TraceID:       sp.TraceID,
				StartTime:     time.Now(),
				CancelSending: cancel,
			}
			// push this into the cache and if we eject an unsent trace, send it.
			ejectedTrace := i.Cache.Set(trace)
			if ejectedTrace != nil {
				go i.send(ejectedTrace)
			}
			go i.timeoutThenSend(ctx, trace)
		}
		err := trace.AddSpan(sp)
		if err == types.TraceAlreadySent {
			i.dealWithSentTrace(trace.KeepSample, trace.SampleRate, sp)
			return
		}

		// if this is a root span, send the trace
		if isRootSpan(sp) {
			go i.pauseAndSend(trace)
		}
	}
}

//
func (i *InMemCollector) dealWithSentTrace(keep bool, sampleRate uint, sp *types.Span) {
	if keep {
		i.Logger.WithField("trace_id", sp.TraceID).Debugf("Sending span because of previous decision to send trace")
		sp.SampleRate *= sampleRate
		i.Transmission.EnqueueSpan(sp)
		return
	}
	i.Logger.WithField("trace_id", sp.TraceID).Debugf("Dropping span because of previous decision to drop trace")
	return
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

// timeoutThenSend will wait for the trace timeout to expire then send (if it
// has not already been sent). It should be called in a goroutine, not
// synchronously.
func (i *InMemCollector) timeoutThenSend(ctx context.Context, trace *types.Trace) {
	traceTimeout, err := i.Config.GetTraceTimeout()
	if err != nil {
		i.Logger.Errorf("failed to get trace timeout. pausing for 60 seconds")
		traceTimeout = 60
	}
	timeout := time.NewTimer(time.Duration(traceTimeout) * time.Second)
	select {
	case <-timeout.C:
		i.Logger.WithField("trace_id", trace.TraceID).Debugf("trace timeout for trace")
		trace.FinishTime = time.Now()
		i.send(trace)
	case <-ctx.Done():
		// someone canceled our timeout. just bail
		return
	}
}

// pauseAndSend will block for a short time then send. It should be called in a
// goroutine, not synchronously.
func (i *InMemCollector) pauseAndSend(trace *types.Trace) {
	if trace.GetSent() == true {
		// someone else already sent this trace, we can just bail.
		return
	}
	trace.FinishTime = time.Now()
	sendDelay, err := i.Config.GetSendDelay()
	if err != nil {
		i.Logger.Errorf("failed to get send delay. pausing for 2 seconds")
		sendDelay = 2
	}
	time.Sleep(time.Duration(sendDelay) * time.Second)
	i.send(trace)
}

func (i *InMemCollector) send(trace *types.Trace) {
	trace.SendSampleLock.Lock()
	defer trace.SendSampleLock.Unlock()

	if trace.Sent == true {
		// someone else already sent this. Return doing nothing.
		i.Logger.WithField("trace_id", trace.TraceID).WithField("dataset", trace.Dataset).Debugf("skipping send because someone else already sent trace to dataset")
		return
	}

	// we're sending this trace, bump the counter
	i.Metrics.IncrementCounter("trace_sent")
	i.Metrics.Histogram("trace_span_count", float64(len(trace.GetSpans())))

	// hey, we're sending the trace! we should cancel the timeout goroutine.
	trace.CancelSending()

	traceDur := float64(trace.FinishTime.Sub(trace.StartTime) / time.Millisecond)
	i.Metrics.Histogram("trace_duration_ms", traceDur)

	var sampler sample.Sampler
	var found bool

	i.dsLock.Lock()
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
	i.dsLock.Unlock()

	// make sampling decision and update the trace
	rate, shouldSend := sampler.GetSampleRate(trace)
	trace.SampleRate = rate
	trace.KeepSample = shouldSend
	trace.Sent = true

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

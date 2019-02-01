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
	Transmission   transmit.Transmission  `inject:""`
	Metrics        metrics.Metrics        `inject:""`
	SamplerFactory *sample.SamplerFactory `inject:""`

	cacheLock       sync.Mutex
	Cache           cache.Cache
	datasetSamplers map[string]sample.Sampler
	defaultSampler  sample.Sampler

	// sentTraceCache holds the record of our past sampling decisions
	sentTraceCache *lru.Cache

	// traceTimeouts holds traces until they've timed out and are ready to send
	traceTimeouts chan *types.Trace
	cancelers     []context.CancelFunc
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
	i.Metrics.Register("trace_timeout_buffer_overrun", "counter")

	stc, err := lru.New(capacity * 5) // keep 5x ring buffer size
	if err != nil {
		return err
	}
	i.sentTraceCache = stc

	// make the trace timout channel.  We really don't want to block or drop
	// traces from this channel so let's make it really big and be careful to
	// scream if we overrun it.
	i.traceTimeouts = make(chan *types.Trace, capacity*10)
	// pass in a cancellable context so we can flush all the traces in the
	// channel on shutdown
	ctx, cancel := context.WithCancel(context.Background())
	i.cancelers = append(i.cancelers, cancel)

	// start up the goroutine that will consume from the trace timeout channel
	go i.handleTimedOutTraces(ctx)

	return nil
}

func (i *InMemCollector) reloadConfigs() {
	imcConfig := &imcConfig{}
	err := i.Config.GetOtherConfig("InMemCollector", imcConfig)
	if err != nil {
		i.Logger.Errorf("Failed to reload InMemCollector section when reloading configs:", err)
	}
	capacity := imcConfig.CacheCapacity

	if existingCache, ok := i.Cache.(*cache.DefaultInMemCache); ok {
		if capacity != existingCache.GetCacheSize() {
			i.Logger.Debugf("refreshing the cache because it changed size from %d to %d", existingCache.GetCacheSize(), capacity)
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
		i.Logger.Errorf("skipping reloading the cache on config reload because it's not an in-memory cache")
	}
	// TODO add resizing the LRU sent trace cache on config reload
}

func (i *InMemCollector) AddSpan(sp *types.Span) {
	trace := i.Cache.Get(sp.TraceID)
	if trace == nil {
		// if the trace has already been sent, just pass along the span
		if sentRecord, found := i.sentTraceCache.Get(sp.TraceID); found {
			if sr, ok := sentRecord.(*traceSentRecord); ok {
				// TODO add a metric saying we pulled this trace from the sent cache
				i.dealWithSentTrace(sr.keep, sr.rate, sp)
				return
			}
		}
		// trace hasn't already been sent (or this span is really old); let's
		// create a new trace to hold it
		trace = &types.Trace{
			APIHost:   sp.APIHost,
			APIKey:    sp.APIKey,
			Dataset:   sp.Dataset,
			TraceID:   sp.TraceID,
			StartTime: time.Now(),
		}
		// push this into the cache and if we eject an unsent trace, send it.
		// This might happen before the 60sec timeout
		ejectedTrace := i.Cache.Set(trace)
		if ejectedTrace != nil {
			go i.send(ejectedTrace)
		}
		// add the trace to the timeout channel for eventual sending
		select {
		case i.traceTimeouts <- trace:
		default:
			// this is bad - we filled the timeout channel. This trace will
			// never timeout; it's only opportunity for getting sent is
			// overrunning the cache buffer.
			i.Metrics.IncrementCounter("trace_timeout_buffer_overrun")
			i.Logger.Errorf("trace ID %s overflowed timeout channel", trace.TraceID)
		}
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

//
func (i *InMemCollector) dealWithSentTrace(keep bool, sampleRate uint, sp *types.Span) {
	if keep {
		i.Logger.Debugf("Sending span because of previous decision to send trace %s", sp.TraceID)
		sp.SampleRate *= sampleRate
		i.Transmission.EnqueueSpan(sp)
		return
	}
	i.Logger.Debugf("Dropping span because of previous decision to drop trace %s", sp.TraceID)
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

// handleTimedOutTraces reads from the channel of potentially old traces. It
// considers each one and sends it if it's more than 60sec old. They're inserted
// in arrival order, so if it's less than 60sec old, the function sleeps until
// it is (since everything that will come after it is necessarily newer). It
// sends anything that needs to be sent and discards those already sent. This is
// the method by which traces will get sent eventually even if no root span ever
// arrives.
func (i *InMemCollector) handleTimedOutTraces(ctx context.Context) {
	done := ctx.Done()
	traceTimeout, err := i.Config.GetTraceTimeout()
	if err != nil {
		i.Logger.Errorf("failed to get trace timeout. pausing for 60 seconds")
		traceTimeout = 60
	}
	for {
		select {
		case <-done:
			return
		case tr := <-i.traceTimeouts:
			// if we've already sent the trace, carry on.
			if tr.GetSent() {
				i.Logger.Debugf("queued trace already sent")
				continue
			}
			// if the trace's start time plus 60sec is before now, we've timed out.
			if tr.StartTime.Add(time.Duration(traceTimeout) * time.Second).Before(time.Now()) {
				i.Logger.Debugf("trace timeout for trace ID %s", tr.TraceID)
				go i.pauseAndSend(tr)
				continue
			}
			// the trace hasn't been sent, but it also hasn't timed out we
			// should sleep until it's timeout then send it. If it's already
			// been sent by then, sending will return with a noop.
			i.Logger.Debugf("got trhough queue")
			timeTillExpire := tr.StartTime.Add(time.Duration(traceTimeout) * time.Second).Sub(time.Now())
			if timeTillExpire < 0 {
				timeTillExpire = 0
			}
			time.Sleep(timeTillExpire)
			if !tr.GetSent() { // send if not already sent
				i.Logger.Debugf("queued trace sent after sleep")
				go i.pauseAndSend(tr)
			}
		}
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
		i.Logger.Debugf("skipping send because someone else already sent, trace ID %s to dataset %s", trace.TraceID, trace.Dataset)
		return
	}

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
	trace.Sent = true

	// record this decision in the sent record LRU for future spans
	sentRecord := traceSentRecord{
		keep: shouldSend,
		rate: rate,
	}
	i.sentTraceCache.Add(trace.TraceID, &sentRecord)

	// if we're supposed to drop this trace, then we're done.
	if !shouldSend {
		i.Logger.Infof("Dropping trace because of sampling, trace ID %s to dataset %s", trace.TraceID, trace.Dataset)
		return
	}

	// ok, we're not dropping this trace; send all the spans
	i.Logger.Infof("Sending trace ID %s to dataset %s", trace.TraceID, trace.Dataset)
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
	// purge the collector of any in-flight traces
	traces := i.Cache.GetAll()
	for _, trace := range traces {
		if !trace.GetSent() {
			i.send(trace)
		}
	}
	// cancel all the contexts that need canceling
	for _, cancel := range i.cancelers {
		cancel()
	}
	i.Transmission.Flush()
	return nil
}

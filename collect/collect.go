package collect

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

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
}

type imcConfig struct {
	CacheCapacity int
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
				fmt.Printf(".")
				c.Set(trace)
			}
			i.Cache = c
		} else {
			i.Logger.Debugf("skipping reloading the cache on config reload because it hasn't changed capacity")
		}
	} else {
		i.Logger.Errorf("skipping reloading the cache on config reload because it's not an in-memory cache")
	}
}

func (i *InMemCollector) AddSpan(sp *types.Span) {
	i.Logger.Debugf("accepted span for collection: %+v", sp.Data)
	trace := i.Cache.Get(sp.TraceID)
	if trace == nil {
		ctx, cancel := context.WithCancel(context.Background())
		spans := make([]*types.Span, 0, 1)
		spans = append(spans, sp)
		trace = &types.Trace{
			APIHost:       sp.APIHost,
			APIKey:        sp.APIKey,
			Dataset:       sp.Dataset,
			TraceID:       sp.TraceID,
			Spans:         spans,
			StartTime:     time.Now(),
			CancelSending: cancel,
		}
		i.Cache.Set(trace)
		go i.timeoutThenSend(ctx, trace)
	} else {
		// we found a trace; add this span to it
		trace.Spans = append(trace.Spans, sp)
	}

	// if this trace has already gotten sent (aka we're a straggler), skip the
	// rest and obey the existing sample / send decision
	if trace.GetSent() {
		if trace.KeepSample {
			i.Logger.Debugf("Obeying previous decision to send trace %s", sp.TraceID)
			sp.SampleRate *= trace.SampleRate
			i.Transmission.EnqueueSpan(sp)
			return
		}
		i.Logger.Debugf("Obeying previous decision to drop trace %s", sp.TraceID)
		return
	}

	// if this is a root span, send the trace
	if isRootSpan(sp) {
		i.Logger.Debugf("found root span; sending trace: %+v", sp.Data)
		go i.pauseAndSend(trace)
	}
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
		i.Logger.Debugf("trace timeout for trace ID %s", trace.TraceID)
		trace.FinishTime = time.Now()
		i.send(trace)
	case <-ctx.Done():
		i.Logger.Debugf("trace timeout cancelled for trace ID %s", trace.TraceID)
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
		i.Logger.Debugf("skipping send because someone else already sent, trace ID %s", trace.TraceID)
		return
	}

	traceDur := float64(trace.FinishTime.Sub(trace.StartTime) / time.Millisecond)
	i.Metrics.Histogram("trace_duration_ms", traceDur)

	// hey, we're sending the trace! we should cancel the timeout goroutine.
	defer trace.CancelSending()

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

	// if we're supposed to drop this trace, then we're done.
	if !shouldSend {
		i.Logger.Debugf("Dropping trace because of sampling, trace ID %s", trace.TraceID)
		return
	}

	// ok, we're not dropping this trace; send all the spans
	i.Logger.Debugf("Handing off trace spans to transmission, trace ID %s", trace.TraceID)
	for _, sp := range trace.Spans {
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
	i.Transmission.Flush()
	return nil
}

package collect

import (
	"runtime"
	"strconv"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/stretchr/testify/assert"

	"github.com/honeycombio/samproxy/collect/cache"
	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/metrics"
	"github.com/honeycombio/samproxy/sample"
	"github.com/honeycombio/samproxy/transmit"
	"github.com/honeycombio/samproxy/types"
)

// TestAddRootSpan tests that adding a root span winds up with a trace object in
// the cache and that that trace gets sent
func TestAddRootSpan(t *testing.T) {
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	conf := &config.MockConfig{
		GetSendDelayVal:    0,
		GetTraceTimeoutVal: 60 * time.Second,
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		SendTickerVal:      2 * time.Millisecond,
	}
	coll := &InMemCollector{
		Config:       conf,
		Logger:       &logger.NullLogger{},
		Transmission: transmission,
		Metrics:      &metrics.NullMetrics{},
		SamplerFactory: &sample.SamplerFactory{
			Config: conf,
			Logger: &logger.NullLogger{},
		},
	}

	c := &cache.DefaultInMemCache{
		Config: cache.CacheConfig{
			CacheCapacity: 3,
		},
		Metrics: &metrics.NullMetrics{},
		Logger:  &logger.NullLogger{},
	}
	err := c.Start()
	assert.NoError(t, err, "in-mem cache should start")

	coll.cache = c
	stc, err := lru.New(15)
	assert.NoError(t, err, "lru cache should start")
	coll.sentTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	defer coll.Stop()

	var traceID1 = "mytrace"
	var traceID2 = "mytraess"

	span := &types.Span{
		TraceID: traceID1,
		Event: types.Event{
			Dataset: "aoeu",
		},
	}
	coll.AddSpan(span)

	time.Sleep(conf.SendTickerVal * 2)
	// adding one span with no parent ID should:
	// * create the trace in the cache
	// * send the trace
	// * remove the trace from the cache
	assert.Nil(t, coll.getFromCache(traceID1), "after sending the span, it should be removed from the cache")
	transmission.Mux.RLock()
	assert.Equal(t, 1, len(transmission.Events), "adding a root span should send the span")
	assert.Equal(t, "aoeu", transmission.Events[0].Dataset, "sending a root span should immediately send that span via transmission")
	transmission.Mux.RUnlock()

	span = &types.Span{
		TraceID: traceID2,
		Event: types.Event{
			Dataset: "aoeu",
		},
	}
	coll.AddSpanFromPeer(span)
	time.Sleep(conf.SendTickerVal * 2)
	// adding one span with no parent ID should:
	// * create the trace in the cache
	// * send the trace
	// * remove the trace from the cache
	assert.Nil(t, coll.getFromCache(traceID1), "after sending the span, it should be removed from the cache")
	transmission.Mux.RLock()
	assert.Equal(t, 2, len(transmission.Events), "adding another root span should send the span")
	assert.Equal(t, "aoeu", transmission.Events[1].Dataset, "sending a root span should immediately send that span via transmission")
	transmission.Mux.RUnlock()
}

// TestAddSpan tests that adding a span winds up with a trace object in the
// cache
func TestAddSpan(t *testing.T) {
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	conf := &config.MockConfig{
		GetSendDelayVal:    0,
		GetTraceTimeoutVal: 60 * time.Second,
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		SendTickerVal:      2 * time.Millisecond,
	}
	coll := &InMemCollector{
		Config:       conf,
		Logger:       &logger.NullLogger{},
		Transmission: transmission,
		Metrics:      &metrics.NullMetrics{},
		SamplerFactory: &sample.SamplerFactory{
			Config: conf,
			Logger: &logger.NullLogger{},
		},
	}
	c := &cache.DefaultInMemCache{
		Config: cache.CacheConfig{
			CacheCapacity: 3,
		},
		Metrics: &metrics.NullMetrics{},
		Logger:  &logger.NullLogger{},
	}
	c.Start()
	coll.cache = c
	stc, err := lru.New(15)
	assert.NoError(t, err, "lru cache should start")
	coll.sentTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	defer coll.Stop()

	var traceID = "mytrace"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: map[string]interface{}{
				"trace.parent_id": "unused",
			},
		},
	}
	coll.AddSpanFromPeer(span)
	time.Sleep(conf.SendTickerVal * 2)
	assert.Equal(t, traceID, coll.getFromCache(traceID).TraceID, "after adding the span, we should have a trace in the cache with the right trace ID")
	assert.Equal(t, 0, len(transmission.Events), "adding a non-root span should not yet send the span")
	// ok now let's add the root span and verify that both got sent
	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    map[string]interface{}{},
		},
	}
	coll.AddSpan(rootSpan)
	time.Sleep(conf.SendTickerVal * 2)
	assert.Nil(t, coll.getFromCache(traceID), "after adding a leaf and root span, it should be removed from the cache")
	transmission.Mux.RLock()
	assert.Equal(t, 2, len(transmission.Events), "adding a root span should send all spans in the trace")
	transmission.Mux.RUnlock()
}

// TestDryRunMode tests that all traces are sent, regardless of sampling decision, and that the
// sampling decision is marked on each span in the trace
func TestDryRunMode(t *testing.T) {
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	conf := &config.MockConfig{
		GetSendDelayVal:    0,
		GetTraceTimeoutVal: 60 * time.Second,
		GetSamplerTypeVal: &config.DeterministicSamplerConfig{
			SampleRate: 10,
		},
		SendTickerVal: 2 * time.Millisecond,
		DryRun:        true,
	}
	samplerFactory := &sample.SamplerFactory{
		Config: conf,
		Logger: &logger.NullLogger{},
	}
	sampler := samplerFactory.GetSamplerImplementationForDataset("test")
	coll := &InMemCollector{
		Config:         conf,
		Logger:         &logger.NullLogger{},
		Transmission:   transmission,
		Metrics:        &metrics.NullMetrics{},
		SamplerFactory: samplerFactory,
	}
	c := &cache.DefaultInMemCache{
		Config: cache.CacheConfig{
			CacheCapacity: 3,
		},
		Metrics: &metrics.NullMetrics{},
		Logger:  &logger.NullLogger{},
	}
	err := c.Start()
	assert.NoError(t, err, "in-mem cache should start")
	coll.cache = c
	stc, err := lru.New(15)
	assert.NoError(t, err, "lru cache should start")
	coll.sentTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	defer coll.Stop()

	var traceID1 = "abc123"
	var traceID2 = "def456"
	var traceID3 = "ghi789"
	// sampling decisions based on trace ID
	_, keepTraceID1 := sampler.GetSampleRate(&types.Trace{TraceID: traceID1})
	// would be dropped if dry run mode was not enabled
	assert.False(t, keepTraceID1)
	_, keepTraceID2 := sampler.GetSampleRate(&types.Trace{TraceID: traceID2})
	assert.True(t, keepTraceID2)
	_, keepTraceID3 := sampler.GetSampleRate(&types.Trace{TraceID: traceID3})
	// would be dropped if dry run mode was not enabled
	assert.False(t, keepTraceID3)

	span := &types.Span{
		TraceID: traceID1,
		Event: types.Event{
			Data: map[string]interface{}{},
		},
	}
	coll.AddSpan(span)
	time.Sleep(conf.SendTickerVal * 2)
	// adding one span with no parent ID should:
	// * create the trace in the cache
	// * send the trace
	// * remove the trace from the cache
	assert.Nil(t, coll.getFromCache(traceID1), "after sending the span, it should be removed from the cache")
	transmission.Mux.RLock()
	assert.Equal(t, 1, len(transmission.Events), "adding a root span should send the span")
	assert.Equal(t, keepTraceID1, transmission.Events[0].Data["samproxy_kept"], "field should match sampling decision for its trace ID")
	transmission.Mux.RUnlock()

	// add a non-root span, create the trace in the cache
	span = &types.Span{
		TraceID: traceID2,
		Event: types.Event{
			Dataset: "aoeu",
			Data: map[string]interface{}{
				"trace.parent_id": "unused",
			},
		},
	}
	coll.AddSpanFromPeer(span)
	time.Sleep(conf.SendTickerVal * 2)
	assert.Equal(t, traceID2, coll.getFromCache(traceID2).TraceID, "after adding the span, we should have a trace in the cache with the right trace ID")

	span = &types.Span{
		TraceID: traceID2,
		Event: types.Event{
			Data: map[string]interface{}{},
		},
	}
	coll.AddSpanFromPeer(span)
	time.Sleep(conf.SendTickerVal * 2)
	// adding root span to send the trace
	transmission.Mux.RLock()
	assert.Equal(t, 3, len(transmission.Events), "adding another root span should send the span")
	// both spans should be marked with the sampling decision
	assert.Equal(t, keepTraceID2, transmission.Events[1].Data["samproxy_kept"], "field should match sampling decision for its trace ID")
	assert.Equal(t, keepTraceID2, transmission.Events[2].Data["samproxy_kept"], "field should match sampling decision for its trace ID")
	transmission.Mux.RUnlock()

	span = &types.Span{
		TraceID: traceID3,
		Event: types.Event{
			Data: map[string]interface{}{},
		},
	}
	coll.AddSpan(span)
	time.Sleep(conf.SendTickerVal * 2)
	// adding one span with no parent ID should:
	// * create the trace in the cache
	// * send the trace
	// * remove the trace from the cache
	assert.Nil(t, coll.getFromCache(traceID3), "after sending the span, it should be removed from the cache")
	transmission.Mux.RLock()
	assert.Equal(t, 4, len(transmission.Events), "adding a root span should send the span")
	assert.Equal(t, keepTraceID3, transmission.Events[3].Data["samproxy_kept"], "field should match sampling decision for its trace ID")
	transmission.Mux.RUnlock()
}

func TestSampleConfigReload(t *testing.T) {
	transmission := &transmit.MockTransmission{}

	transmission.Start()

	conf := &config.MockConfig{
		GetSendDelayVal:                      0,
		GetTraceTimeoutVal:                   10 * time.Millisecond,
		GetSamplerTypeVal:                    &config.DeterministicSamplerConfig{SampleRate: 1},
		SendTickerVal:                        2 * time.Millisecond,
		GetInMemoryCollectorCacheCapacityVal: config.InMemoryCollectorCacheCapacity{CacheCapacity: 10},
	}

	coll := &InMemCollector{
		Config:       conf,
		Logger:       &logger.NullLogger{},
		Transmission: transmission,
		Metrics:      &metrics.NullMetrics{},
		SamplerFactory: &sample.SamplerFactory{
			Config: conf,
			Logger: &logger.NullLogger{},
		},
	}

	err := coll.Start()
	assert.NoError(t, err)
	defer coll.Stop()

	dataset := "aoeu"

	span := &types.Span{
		TraceID: "1",
		Event: types.Event{
			Dataset: dataset,
		},
	}

	coll.AddSpan(span)

	assert.Eventually(t, func() bool {
		coll.mutex.Lock()
		defer coll.mutex.Unlock()

		_, ok := coll.datasetSamplers[dataset]
		return ok
	}, conf.GetTraceTimeoutVal*2, conf.SendTickerVal)

	conf.ReloadConfig()

	assert.Eventually(t, func() bool {
		coll.mutex.Lock()
		defer coll.mutex.Unlock()

		_, ok := coll.datasetSamplers[dataset]
		return !ok
	}, conf.GetTraceTimeoutVal*2, conf.SendTickerVal)

	span = &types.Span{
		TraceID: "2",
		Event: types.Event{
			Dataset: dataset,
		},
	}

	coll.AddSpan(span)

	assert.Eventually(t, func() bool {
		coll.mutex.Lock()
		defer coll.mutex.Unlock()

		_, ok := coll.datasetSamplers[dataset]
		return ok
	}, conf.GetTraceTimeoutVal*2, conf.SendTickerVal)
}

func TestMaxAlloc(t *testing.T) {
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	conf := &config.MockConfig{
		GetSendDelayVal:    0,
		GetTraceTimeoutVal: 10 * time.Minute,
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		SendTickerVal:      2 * time.Millisecond,
	}
	coll := &InMemCollector{
		Config:       conf,
		Logger:       &logger.NullLogger{},
		Transmission: transmission,
		Metrics:      &metrics.NullMetrics{},
		SamplerFactory: &sample.SamplerFactory{
			Config: conf,
			Logger: &logger.NullLogger{},
		},
	}
	c := &cache.DefaultInMemCache{
		Config: cache.CacheConfig{
			CacheCapacity: 1000,
		},
		Metrics: &metrics.NullMetrics{},
		Logger:  &logger.NullLogger{},
	}
	c.Start()
	coll.cache = c
	stc, err := lru.New(15)
	assert.NoError(t, err, "lru cache should start")
	coll.sentTraceCache = stc

	coll.incoming = make(chan *types.Span, 1000)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	defer coll.Stop()

	for i := 0; i < 500; i++ {
		span := &types.Span{
			TraceID: strconv.Itoa(i),
			Event: types.Event{
				Dataset: "aoeu",
				Data: map[string]interface{}{
					"trace.parent_id": "unused",
					"id":              i,
				},
			},
		}
		coll.AddSpan(span)
	}

	for len(coll.incoming) > 0 {
		time.Sleep(conf.SendTickerVal)
	}

	// Now there should be 500 traces in the cache.
	// Set MaxAlloc, which should cause cache evictions.
	coll.mutex.Lock()
	assert.Equal(t, 500, len(coll.cache.GetAll()))

	// We only want to induce a single downsize, so set MaxAlloc just below
	// our current post-GC alloc.
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	conf.GetInMemoryCollectorCacheCapacityVal.MaxAlloc = mem.Alloc - 1

	coll.mutex.Unlock()

	var traces []*types.Trace
	for {
		coll.mutex.Lock()
		traces = coll.cache.GetAll()
		if len(traces) < 500 {
			break
		}
		coll.mutex.Unlock()

		time.Sleep(conf.SendTickerVal)
	}

	assert.Equal(t, 450, len(traces), "should have shrunk cache to 90% of previous size")
	for i, trace := range traces {
		assert.False(t, trace.Sent)
		assert.Equal(t, strconv.Itoa(i+50), trace.TraceID)
	}
	coll.mutex.Unlock()

	// We discarded the first 50 spans, and sent them.
	transmission.Mux.Lock()
	assert.Equal(t, 50, len(transmission.Events), "should have sent 10% of traces")
	for i, ev := range transmission.Events {
		assert.Equal(t, i, ev.Data["id"])
	}

	transmission.Mux.Unlock()

}

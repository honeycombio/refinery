package collect

import (
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/facebookgo/inject"
	"github.com/stretchr/testify/assert"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"
)

const legacyAPIKey = "c9945edf5d245834089a1bd6cc9ad01e"

func newCache() (cache.TraceSentCache, error) {
	cfg := config.SampleCacheConfig{
		KeptSize:          100,
		DroppedSize:       100,
		SizeCheckInterval: config.Duration(1 * time.Second),
	}

	return cache.NewCuckooSentCache(cfg, &metrics.NullMetrics{})
}

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
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
	}
	coll := &InMemCollector{
		Config:       conf,
		Logger:       &logger.NullLogger{},
		Transmission: transmission,
		Metrics:      &metrics.NullMetrics{},
		StressRelief: &MockStressReliever{},
		SamplerFactory: &sample.SamplerFactory{
			Config: conf,
			Logger: &logger.NullLogger{},
		},
	}

	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

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
			APIKey:  legacyAPIKey,
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
			APIKey:  legacyAPIKey,
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

// #490, SampleRate getting stomped could cause confusion if sampling was
// happening upstream of refinery. Writing down what got sent to refinery
// will help people figure out what is going on.
func TestOriginalSampleRateIsNotedInMetaField(t *testing.T) {
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	conf := &config.MockConfig{
		GetSendDelayVal:    0,
		GetTraceTimeoutVal: 60 * time.Second,
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 2},
		SendTickerVal:      2 * time.Millisecond,
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
	}
	coll := &InMemCollector{
		Config:       conf,
		Logger:       &logger.NullLogger{},
		Transmission: transmission,
		Metrics:      &metrics.NullMetrics{},
		StressRelief: &MockStressReliever{},
		SamplerFactory: &sample.SamplerFactory{
			Config: conf,
			Logger: &logger.NullLogger{},
		},
	}

	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	defer coll.Stop()

	// Spin until a sample gets triggered
	sendAttemptCount := 0
	for getEventsLength(transmission) < 1 || sendAttemptCount > 10 {
		sendAttemptCount++
		span := &types.Span{
			TraceID: fmt.Sprintf("trace-%v", sendAttemptCount),
			Event: types.Event{
				Dataset:    "aoeu",
				APIKey:     legacyAPIKey,
				SampleRate: 50,
				Data:       make(map[string]interface{}),
			},
		}
		coll.AddSpan(span)
		time.Sleep(conf.SendTickerVal * 2)
	}

	transmission.Mux.RLock()
	assert.Greater(t, len(transmission.Events), 0, "should be some events transmitted")
	assert.Equal(t, uint(50), transmission.Events[0].Data["meta.refinery.original_sample_rate"],
		"metadata should be populated with original sample rate")
	transmission.Mux.RUnlock()

	span := &types.Span{
		TraceID: fmt.Sprintf("trace-%v", 1000),
		Event: types.Event{
			Dataset:    "aoeu",
			APIKey:     legacyAPIKey,
			SampleRate: 0,
			Data:       make(map[string]interface{}),
		},
	}

	coll.AddSpan(span)

	time.Sleep(conf.SendTickerVal * 2)

	transmission.Mux.RLock()
	assert.Equal(t, 2, len(transmission.Events), "should be some events transmitted")
	assert.Nil(t, transmission.Events[1].Data["meta.refinery.original_sample_rate"],
		"metadata should not be populated when zero")
	transmission.Mux.RUnlock()
}

// HoneyComb treats a missing or 0 SampleRate the same as 1, but
// behaves better/more consistently if the SampleRate is explicitly
// set instead of inferred
func TestTransmittedSpansShouldHaveASampleRateOfAtLeastOne(t *testing.T) {
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	conf := &config.MockConfig{
		GetSendDelayVal:    0,
		GetTraceTimeoutVal: 60 * time.Second,
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		SendTickerVal:      2 * time.Millisecond,
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
	}
	coll := &InMemCollector{
		Config:       conf,
		Logger:       &logger.NullLogger{},
		Transmission: transmission,
		Metrics:      &metrics.NullMetrics{},
		StressRelief: &MockStressReliever{},
		SamplerFactory: &sample.SamplerFactory{
			Config: conf,
			Logger: &logger.NullLogger{},
		},
	}

	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	defer coll.Stop()

	span := &types.Span{
		TraceID: fmt.Sprintf("trace-%v", 1),
		Event: types.Event{
			Dataset:    "aoeu",
			APIKey:     legacyAPIKey,
			SampleRate: 0, // This should get lifted to 1
			Data:       make(map[string]interface{}),
		},
	}

	coll.AddSpan(span)

	time.Sleep(conf.SendTickerVal * 2)

	transmission.Mux.RLock()
	assert.Equal(t, 1, len(transmission.Events), "should be some events transmitted")
	assert.Equal(t, uint(1), transmission.Events[0].SampleRate,
		"SampleRate should be reset to one after starting at zero")
	transmission.Mux.RUnlock()
}

func getEventsLength(transmission *transmit.MockTransmission) int {
	transmission.Mux.RLock()
	defer transmission.Mux.RUnlock()

	return len(transmission.Events)
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
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
	}
	coll := &InMemCollector{
		Config:       conf,
		Logger:       &logger.NullLogger{},
		Transmission: transmission,
		Metrics:      &metrics.NullMetrics{},
		StressRelief: &MockStressReliever{},
		SamplerFactory: &sample.SamplerFactory{
			Config: conf,
			Logger: &logger.NullLogger{},
		},
	}
	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

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
			APIKey: legacyAPIKey,
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
			APIKey:  legacyAPIKey,
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
		SendTickerVal:      20 * time.Millisecond,
		DryRun:             true,
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
	}
	samplerFactory := &sample.SamplerFactory{
		Config: conf,
		Logger: &logger.NullLogger{},
	}
	sampler := samplerFactory.GetSamplerImplementationForKey("test", true)
	coll := &InMemCollector{
		Config:         conf,
		Logger:         &logger.NullLogger{},
		Transmission:   transmission,
		Metrics:        &metrics.NullMetrics{},
		StressRelief:   &MockStressReliever{},
		SamplerFactory: samplerFactory,
	}
	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	defer coll.Stop()

	var traceID1 = "abc123"
	var traceID2 = "def456"
	var traceID3 = "ghi789"
	// sampling decisions based on trace ID
	sampleRate1, keepTraceID1, _, _ := sampler.GetSampleRate(&types.Trace{TraceID: traceID1})
	// would be dropped if dry run mode was not enabled
	assert.False(t, keepTraceID1)
	assert.Equal(t, uint(10), sampleRate1)
	sampleRate2, keepTraceID2, _, _ := sampler.GetSampleRate(&types.Trace{TraceID: traceID2})
	assert.True(t, keepTraceID2)
	assert.Equal(t, uint(10), sampleRate2)
	sampleRate3, keepTraceID3, _, _ := sampler.GetSampleRate(&types.Trace{TraceID: traceID3})
	// would be dropped if dry run mode was not enabled
	assert.False(t, keepTraceID3)
	assert.Equal(t, uint(10), sampleRate3)

	span := &types.Span{
		TraceID: traceID1,
		Event: types.Event{
			Data:   map[string]interface{}{},
			APIKey: legacyAPIKey,
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
	assert.Equal(t, keepTraceID1, transmission.Events[0].Data[config.DryRunFieldName], "config.DryRunFieldName should match sampling decision for its trace ID")
	transmission.Mux.RUnlock()

	// add a non-root span, create the trace in the cache
	span = &types.Span{
		TraceID: traceID2,
		Event: types.Event{
			Dataset: "aoeu",
			Data: map[string]interface{}{
				"trace.parent_id": "unused",
			},
			APIKey: legacyAPIKey,
		},
	}
	coll.AddSpanFromPeer(span)
	time.Sleep(conf.SendTickerVal * 2)
	assert.Equal(t, traceID2, coll.getFromCache(traceID2).TraceID, "after adding the span, we should have a trace in the cache with the right trace ID")

	span = &types.Span{
		TraceID: traceID2,
		Event: types.Event{
			Data:   map[string]interface{}{},
			APIKey: legacyAPIKey,
		},
	}
	coll.AddSpanFromPeer(span)
	time.Sleep(conf.SendTickerVal * 2)
	// adding root span to send the trace
	transmission.Mux.RLock()
	assert.Equal(t, 3, len(transmission.Events), "adding another root span should send the span")
	// both spans should be marked with the sampling decision
	assert.Equal(t, keepTraceID2, transmission.Events[1].Data[config.DryRunFieldName], "config.DryRunFieldName should match sampling decision for its trace ID")
	assert.Equal(t, keepTraceID2, transmission.Events[2].Data[config.DryRunFieldName], "config.DryRunFieldName should match sampling decision for its trace ID")
	// check that meta value associated with dry run mode is properly applied
	assert.Equal(t, uint(10), transmission.Events[1].Data["meta.dryrun.sample_rate"])
	// check expected sampleRate against span data
	assert.Equal(t, sampleRate1, transmission.Events[0].Data["meta.dryrun.sample_rate"])
	assert.Equal(t, sampleRate2, transmission.Events[1].Data["meta.dryrun.sample_rate"])
	transmission.Mux.RUnlock()

	span = &types.Span{
		TraceID: traceID3,
		Event: types.Event{
			Data:   map[string]interface{}{},
			APIKey: legacyAPIKey,
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
	assert.Equal(t, keepTraceID3, transmission.Events[3].Data[config.DryRunFieldName], "field should match sampling decision for its trace ID")
	transmission.Mux.RUnlock()
}

func TestCacheSizeReload(t *testing.T) {
	transmission := &transmit.MockTransmission{}
	transmission.Start()

	conf := &config.MockConfig{
		GetSendDelayVal:    0,
		GetTraceTimeoutVal: 10 * time.Minute,
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		SendTickerVal:      2 * time.Millisecond,
		GetCollectionConfigVal: config.CollectionConfig{
			CacheCapacity: 1,
		},
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
	}

	coll := &InMemCollector{
		Config:       conf,
		Logger:       &logger.NullLogger{},
		Transmission: transmission,
		Metrics:      &metrics.NullMetrics{},
		StressRelief: &MockStressReliever{},
		SamplerFactory: &sample.SamplerFactory{
			Config: conf,
			Logger: &logger.NullLogger{},
		},
	}

	err := coll.Start()
	assert.NoError(t, err)
	defer coll.Stop()

	event := types.Event{
		Dataset: "dataset",
		Data: map[string]interface{}{
			"trace.parent_id": "1",
		},
		APIKey: legacyAPIKey,
	}

	err = coll.AddSpan(&types.Span{TraceID: "1", Event: event})
	assert.NoError(t, err)
	err = coll.AddSpan(&types.Span{TraceID: "2", Event: event})
	assert.NoError(t, err)

	expectedEvents := 1
	wait := 1 * time.Second
	check := func() bool {
		transmission.Mux.RLock()
		defer transmission.Mux.RUnlock()

		return len(transmission.Events) == expectedEvents
	}
	assert.Eventually(t, check, 60*wait, wait, "expected one trace evicted and sent")

	conf.Mux.Lock()
	conf.GetCollectionConfigVal.CacheCapacity = 2
	conf.Mux.Unlock()
	conf.ReloadConfig()

	assert.Eventually(t, func() bool {
		coll.mutex.RLock()
		defer coll.mutex.RUnlock()

		return coll.cache.(*cache.DefaultInMemCache).GetCacheSize() == 2
	}, 60*wait, wait, "cache size to change")

	err = coll.AddSpan(&types.Span{TraceID: "3", Event: event})
	assert.NoError(t, err)
	time.Sleep(5 * conf.SendTickerVal)
	assert.True(t, check(), "expected no more traces evicted and sent")

	conf.Mux.Lock()
	conf.GetCollectionConfigVal.CacheCapacity = 1
	conf.Mux.Unlock()
	conf.ReloadConfig()

	expectedEvents = 2
	assert.Eventually(t, check, 60*wait, wait, "expected another trace evicted and sent")
}

func TestSampleConfigReload(t *testing.T) {
	transmission := &transmit.MockTransmission{}

	transmission.Start()

	conf := &config.MockConfig{
		GetSendDelayVal:        0,
		GetTraceTimeoutVal:     60 * time.Second,
		GetSamplerTypeVal:      &config.DeterministicSamplerConfig{SampleRate: 1},
		SendTickerVal:          2 * time.Millisecond,
		ParentIdFieldNames:     []string{"trace.parent_id", "parentId"},
		GetCollectionConfigVal: config.CollectionConfig{CacheCapacity: 10},
		SampleCache: config.SampleCacheConfig{
			KeptSize:          100,
			DroppedSize:       100,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
	}

	coll := &InMemCollector{
		Config:       conf,
		Logger:       &logger.NullLogger{},
		Transmission: transmission,
		Metrics:      &metrics.NullMetrics{},
		StressRelief: &MockStressReliever{},
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
			APIKey:  legacyAPIKey,
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
			APIKey:  legacyAPIKey,
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

func TestStableMaxAlloc(t *testing.T) {
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	conf := &config.MockConfig{
		GetSendDelayVal:    0,
		GetTraceTimeoutVal: 10 * time.Minute,
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		SendTickerVal:      2 * time.Millisecond,
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
	}
	coll := &InMemCollector{
		Config:       conf,
		Logger:       &logger.NullLogger{},
		Transmission: transmission,
		Metrics:      &metrics.NullMetrics{},
		StressRelief: &MockStressReliever{},
		SamplerFactory: &sample.SamplerFactory{
			Config: conf,
			Logger: &logger.NullLogger{},
		},
	}
	spandata := make([]map[string]interface{}, 500)
	for i := 0; i < 500; i++ {
		spandata[i] = map[string]interface{}{
			"trace.parent_id": "unused",
			"id":              i,
			"str1":            strings.Repeat("abc", rand.Intn(100)+1),
			"str2":            strings.Repeat("def", rand.Intn(100)+1),
		}
	}

	c := cache.NewInMemCache(1000, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

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
				Data:    spandata[i],
				APIKey:  legacyAPIKey,
			},
		}
		coll.AddSpan(span)
	}

	for len(coll.incoming) > 0 {
		time.Sleep(conf.SendTickerVal)
	}

	// Now there should be 500 traces in the cache.
	coll.mutex.Lock()
	assert.Equal(t, 500, len(coll.cache.GetAll()))

	// We want to induce an eviction event, so set MaxAlloc a bit below
	// our current post-GC alloc.
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	// Set MaxAlloc, which should cause cache evictions.
	conf.GetCollectionConfigVal.MaxAlloc = config.MemorySize(mem.Alloc * 99 / 100)

	coll.mutex.Unlock()

	// wait for the cache to take some action
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

	assert.Equal(t, 1000, coll.cache.(*cache.DefaultInMemCache).GetCacheSize(), "cache size shouldn't change")

	tracesLeft := len(traces)
	assert.Less(t, tracesLeft, 480, "should have sent some traces")
	assert.Greater(t, tracesLeft, 100, "should have NOT sent some traces")
	coll.mutex.Unlock()

	// We discarded the most costly spans, and sent them.
	transmission.Mux.Lock()
	assert.Equal(t, 500-len(traces), len(transmission.Events), "should have sent traces that weren't kept")

	transmission.Mux.Unlock()
}

func TestAddSpanNoBlock(t *testing.T) {
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	conf := &config.MockConfig{
		GetSendDelayVal:    0,
		GetTraceTimeoutVal: 10 * time.Minute,
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{},
		SendTickerVal:      2 * time.Millisecond,
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
	}
	coll := &InMemCollector{
		Config:       conf,
		Logger:       &logger.NullLogger{},
		Transmission: transmission,
		Metrics:      &metrics.NullMetrics{},
		StressRelief: &MockStressReliever{},
		SamplerFactory: &sample.SamplerFactory{
			Config: conf,
			Logger: &logger.NullLogger{},
		},
	}
	c := cache.NewInMemCache(10, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 3)
	coll.fromPeer = make(chan *types.Span, 3)
	coll.datasetSamplers = make(map[string]sample.Sampler)

	// Don't start collect(), so the queues are never drained
	span := &types.Span{
		TraceID: "1",
		Event: types.Event{
			Dataset: "aoeu",
			APIKey:  legacyAPIKey,
		},
	}

	for i := 0; i < 3; i++ {
		err := coll.AddSpan(span)
		assert.NoError(t, err)
		err = coll.AddSpanFromPeer(span)
		assert.NoError(t, err)
	}

	err = coll.AddSpan(span)
	assert.Error(t, err)
	err = coll.AddSpanFromPeer(span)
	assert.Error(t, err)
}

func TestDependencyInjection(t *testing.T) {
	var g inject.Graph
	err := g.Provide(
		&inject.Object{Value: &InMemCollector{}},
		&inject.Object{Value: &config.MockConfig{}},
		&inject.Object{Value: &logger.NullLogger{}},
		&inject.Object{Value: &transmit.MockTransmission{}, Name: "upstreamTransmission"},
		&inject.Object{Value: &metrics.NullMetrics{}, Name: "genericMetrics"},
		&inject.Object{Value: &sample.SamplerFactory{}},
		&inject.Object{Value: &MockStressReliever{}, Name: "stressRelief"},
	)
	if err != nil {
		t.Error(err)
	}
	if err := g.Populate(); err != nil {
		t.Error(err)
	}
}

// TestAddSpanCount tests that adding a root span winds up with a trace object in
// the cache and that that trace gets span count added to it
func TestAddSpanCount(t *testing.T) {
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	conf := &config.MockConfig{
		GetSendDelayVal:    0,
		GetTraceTimeoutVal: 60 * time.Second,
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		SendTickerVal:      2 * time.Millisecond,
		AddSpanCountToRoot: true,
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
	}
	coll := &InMemCollector{
		Config:       conf,
		Logger:       &logger.NullLogger{},
		Transmission: transmission,
		Metrics:      &metrics.NullMetrics{},
		StressRelief: &MockStressReliever{},
		SamplerFactory: &sample.SamplerFactory{
			Config: conf,
			Logger: &logger.NullLogger{},
		},
	}
	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

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
			APIKey: legacyAPIKey,
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
			APIKey:  legacyAPIKey,
		},
	}
	coll.AddSpan(rootSpan)
	time.Sleep(conf.SendTickerVal * 2)
	assert.Nil(t, coll.getFromCache(traceID), "after adding a leaf and root span, it should be removed from the cache")
	transmission.Mux.RLock()
	assert.Equal(t, 2, len(transmission.Events), "adding a root span should send all spans in the trace")
	assert.Equal(t, nil, transmission.Events[0].Data["meta.span_count"], "child span metadata should NOT be populated with span count")
	assert.Equal(t, int64(2), transmission.Events[1].Data["meta.span_count"], "root span metadata should be populated with span count")
	transmission.Mux.RUnlock()
}

// TestLateRootGetsSpanCount tests that the root span gets decorated with the right span count
// even if the trace had already been sent
func TestLateRootGetsSpanCount(t *testing.T) {
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	conf := &config.MockConfig{
		GetSendDelayVal:      0,
		GetTraceTimeoutVal:   5 * time.Millisecond,
		GetSamplerTypeVal:    &config.DeterministicSamplerConfig{SampleRate: 1},
		SendTickerVal:        2 * time.Millisecond,
		AddSpanCountToRoot:   true,
		ParentIdFieldNames:   []string{"trace.parent_id", "parentId"},
		AddRuleReasonToTrace: true,
	}
	coll := &InMemCollector{
		Config:       conf,
		Logger:       &logger.NullLogger{},
		Transmission: transmission,
		Metrics:      &metrics.NullMetrics{},
		StressRelief: &MockStressReliever{},
		SamplerFactory: &sample.SamplerFactory{
			Config: conf,
			Logger: &logger.NullLogger{},
		},
	}
	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

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
			APIKey: legacyAPIKey,
		},
	}
	coll.AddSpanFromPeer(span)
	time.Sleep(conf.SendTickerVal * 10)

	trace := coll.getFromCache(traceID)
	assert.Nil(t, trace, "trace should have been sent although the root span hasn't arrived")
	assert.Equal(t, 1, len(transmission.Events), "adding a non-root span and waiting should send the span")
	// now we add the root span and verify that both got sent and that the root span had the span count
	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    map[string]interface{}{},
			APIKey:  legacyAPIKey,
		},
	}
	coll.AddSpan(rootSpan)
	time.Sleep(conf.SendTickerVal * 2)
	assert.Nil(t, coll.getFromCache(traceID), "after adding a leaf and root span, it should be removed from the cache")
	transmission.Mux.RLock()
	assert.Equal(t, 2, len(transmission.Events), "adding a root span should send all spans in the trace")
	assert.Equal(t, nil, transmission.Events[0].Data["meta.span_count"], "child span metadata should NOT be populated with span count")
	assert.Equal(t, int64(2), transmission.Events[1].Data["meta.span_count"], "root span metadata should be populated with span count")
	assert.Equal(t, "late", transmission.Events[1].Data["meta.refinery.reason"], "late spans should have meta.refinery.reason set to late.")
	transmission.Mux.RUnlock()
}

// TestLateRootNotDecorated tests that spans do not get decorated with 'meta.refinery.reason' meta field
// if the AddRuleReasonToTrace attribute not set in config
func TestLateSpanNotDecorated(t *testing.T) {
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	conf := &config.MockConfig{
		GetSendDelayVal:    0,
		GetTraceTimeoutVal: 5 * time.Minute,
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		SendTickerVal:      2 * time.Millisecond,
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
	}
	coll := &InMemCollector{
		Config:       conf,
		Logger:       &logger.NullLogger{},
		Transmission: transmission,
		Metrics:      &metrics.NullMetrics{},
		StressRelief: &MockStressReliever{},
		SamplerFactory: &sample.SamplerFactory{
			Config: conf,
			Logger: &logger.NullLogger{},
		},
	}
	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	defer coll.Stop()

	var traceID = "traceABC"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: map[string]interface{}{
				"trace.parent_id": "unused",
			},
			APIKey: legacyAPIKey,
		},
	}
	coll.AddSpanFromPeer(span)
	time.Sleep(conf.SendTickerVal * 2)

	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    map[string]interface{}{},
			APIKey:  legacyAPIKey,
		},
	}
	coll.AddSpan(rootSpan)
	time.Sleep(conf.SendTickerVal * 2)
	transmission.Mux.RLock()
	assert.Equal(t, 2, len(transmission.Events), "adding a root span should send all spans in the trace")
	assert.Equal(t, nil, transmission.Events[1].Data["meta.refinery.reason"], "late span should not have meta.refinery.reason set to late")
	transmission.Mux.RUnlock()
}

func TestAddAdditionalAttributes(t *testing.T) {
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	conf := &config.MockConfig{
		GetSendDelayVal:    0,
		GetTraceTimeoutVal: 60 * time.Second,
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		SendTickerVal:      2 * time.Millisecond,
		AdditionalAttributes: map[string]string{
			"name":  "foo",
			"other": "bar",
		},
	}
	coll := &InMemCollector{
		Config:       conf,
		Logger:       &logger.NullLogger{},
		Transmission: transmission,
		Metrics:      &metrics.NullMetrics{},
		StressRelief: &MockStressReliever{},
		SamplerFactory: &sample.SamplerFactory{
			Config: conf,
			Logger: &logger.NullLogger{},
		},
	}
	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	defer coll.Stop()

	var traceID = "trace123"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: map[string]interface{}{
				"trace.parent_id": "unused",
			},
			APIKey: legacyAPIKey,
		},
	}
	coll.AddSpanFromPeer(span)
	time.Sleep(conf.SendTickerVal * 2)

	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    map[string]interface{}{},
			APIKey:  legacyAPIKey,
		},
	}
	coll.AddSpan(rootSpan)
	time.Sleep(conf.SendTickerVal * 5)
	transmission.Mux.RLock()
	assert.Equal(t, 2, len(transmission.Events), "should be some events transmitted")
	assert.Equal(t, "foo", transmission.Events[0].Data["name"], "new attribute should appear in data")
	assert.Equal(t, "bar", transmission.Events[0].Data["other"], "new attribute should appear in data")
	transmission.Mux.RUnlock()

}

// TestStressReliefDecorateHostname tests that the span gets decorated with hostname if
// StressReliefMode is active
func TestStressReliefDecorateHostname(t *testing.T) {
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	conf := &config.MockConfig{
		GetSendDelayVal:    0,
		GetTraceTimeoutVal: 5 * time.Minute,
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		SendTickerVal:      2 * time.Millisecond,
		ParentIdFieldNames: []string{"trace.parent_id", "parentId"},
		StressRelief: config.StressReliefConfig{
			Mode:              "monitor",
			ActivationLevel:   75,
			DeactivationLevel: 25,
			SamplingRate:      100,
		},
	}
	coll := &InMemCollector{
		Config:       conf,
		Logger:       &logger.NullLogger{},
		Transmission: transmission,
		Metrics:      &metrics.NullMetrics{},
		StressRelief: &MockStressReliever{},
		SamplerFactory: &sample.SamplerFactory{
			Config: conf,
			Logger: &logger.NullLogger{},
		},
		hostname: "host123",
	}
	c := cache.NewInMemCache(3, &metrics.NullMetrics{}, &logger.NullLogger{})
	coll.cache = c
	stc, err := newCache()
	assert.NoError(t, err, "lru cache should start")
	coll.sampleTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()
	defer coll.Stop()

	var traceID = "traceABC"

	span := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data: map[string]interface{}{
				"trace.parent_id": "unused",
			},
			APIKey: legacyAPIKey,
		},
	}
	coll.AddSpanFromPeer(span)
	time.Sleep(conf.SendTickerVal * 2)

	rootSpan := &types.Span{
		TraceID: traceID,
		Event: types.Event{
			Dataset: "aoeu",
			Data:    map[string]interface{}{},
			APIKey:  legacyAPIKey,
		},
	}
	coll.AddSpan(rootSpan)
	time.Sleep(conf.SendTickerVal * 2)
	transmission.Mux.RLock()
	assert.Equal(t, 2, len(transmission.Events), "adding a root span should send all spans in the trace")
	assert.Equal(t, "host123", transmission.Events[1].Data["meta.refinery.local_hostname"])
	transmission.Mux.RUnlock()

}

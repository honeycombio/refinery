package collect

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/honeycombio/samproxy/collect/cache"
	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/metrics"
	"github.com/honeycombio/samproxy/sample"
	"github.com/honeycombio/samproxy/transmit"
	"github.com/honeycombio/samproxy/types"
	"github.com/stretchr/testify/assert"
)

func BenchmarkCollect(b *testing.B) {
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	conf := &config.MockConfig{
		GetSendDelayVal:          0,
		GetTraceTimeoutVal:       60 * time.Second,
		GetDefaultSamplerTypeVal: "DeterministicSampler",
		SendTickerVal:            2 * time.Millisecond,
	}
	coll := &InMemCollector{
		Config:         conf,
		Logger:         &logger.NullLogger{},
		Transmission:   transmission,
		defaultSampler: &sample.DeterministicSampler{},
		Metrics:        &metrics.NullMetrics{},
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
	assert.NoError(b, err, "in-mem cache should start")
	coll.Cache = c
	stc, err := lru.New(15)
	assert.NoError(b, err, "lru cache should start")
	coll.sentTraceCache = stc

	coll.incoming = make(chan *types.Span, 5)
	coll.fromPeer = make(chan *types.Span, 5)
	go coll.collect()

	for n := 0; n < b.N; n++ {
		span := &types.Span{
			TraceID: fmt.Sprintf("%f", rand.Float64()),
			Event: types.Event{
				Dataset: "aoeu",
			},
		}
		coll.AddSpan(span)

		span = &types.Span{
			TraceID: fmt.Sprintf("%f", rand.Float64()),
			Event: types.Event{
				Dataset: "aoeu",
			},
		}
		coll.AddSpanFromPeer(span)
	}

	wg := &sync.WaitGroup{} // wait until we get b.N number of spans out the other side

	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			// transmission.Mux.RLock()
			if len(transmission.Events) == (b.N * 2) {
				// transmission.Mux.RUnlock()
				return
			}
			// transmission.Mux.RUnlock()
		}
	}()

	wg.Wait()
}

package collect

import (
	"math/rand"
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

func BenchmarkCollect(b *testing.B) {
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	conf := &config.MockConfig{
		GetSendDelayVal:    0,
		GetTraceTimeoutVal: 60 * time.Second,
		GetSamplerTypeVal:  "DeterministicSampler",
		SendTickerVal:      2 * time.Millisecond,
	}

	log := &logger.LogrusLogger{}
	log.SetLevel("warn")
	log.Start()

	metric := &metrics.MockMetrics{}
	metric.Start()

	coll := &InMemCollector{
		Config:       conf,
		Logger:       log,
		Transmission: transmission,
		Metrics:      metric,
		SamplerFactory: &sample.SamplerFactory{
			Config: conf,
			Logger: log,
		},
	}
	c := &cache.DefaultInMemCache{
		Config: cache.CacheConfig{
			CacheCapacity: 3,
		},
		Metrics: metric,
		Logger:  log,
	}
	err := c.Start()
	assert.NoError(b, err, "in-mem cache should start")
	coll.cache = c
	stc, err := lru.New(15)
	assert.NoError(b, err, "lru cache should start")
	coll.sentTraceCache = stc

	coll.incoming = make(chan *types.Span, 500)
	coll.fromPeer = make(chan *types.Span, 500)
	coll.datasetSamplers = make(map[string]sample.Sampler)
	go coll.collect()

	// wait until we get n number of spans out the other side
	wait := func(n int) {
		for {
			transmission.Mux.RLock()
			count := len(transmission.Events)
			transmission.Mux.RUnlock()

			if count >= n {
				break
			}
			time.Sleep(100 * time.Microsecond)
		}
	}

	b.Run("AddSpan", func(b *testing.B) {
		transmission.Flush()
		for n := 0; n < b.N; n++ {
			span := &types.Span{
				TraceID: strconv.Itoa(rand.Int()),
				Event: types.Event{
					Dataset: "aoeu",
				},
			}
			coll.AddSpan(span)
		}
		wait(b.N)
	})

	b.Run("AddSpanFromPeer", func(b *testing.B) {
		transmission.Flush()
		for n := 0; n < b.N; n++ {
			span := &types.Span{
				TraceID: strconv.Itoa(rand.Int()),
				Event: types.Event{
					Dataset: "aoeu",
				},
			}
			coll.AddSpanFromPeer(span)
		}
		wait(b.N)
	})

}

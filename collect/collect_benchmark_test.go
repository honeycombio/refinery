package collect

import (
	"fmt"
	"math/rand"
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
		GetSendDelayVal:          0,
		GetTraceTimeoutVal:       60 * time.Second,
		GetDefaultSamplerTypeVal: "DeterministicSampler",
		SendTickerVal:            2 * time.Millisecond,
	}

	log := &logger.LogrusLogger{}
	log.SetLevel("warn")
	log.Start()

	metric := &metrics.MockMetrics{}
	metric.Start()

	coll := &InMemCollector{
		Config:         conf,
		Logger:         log,
		Transmission:   transmission,
		defaultSampler: &sample.DeterministicSampler{},
		Metrics:        metric,
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

	// wait until we get b.N number of spans out the other side
	for {
		transmission.Mux.RLock()
		count := len(transmission.Events)
		transmission.Mux.RUnlock()

		if count == (b.N * 2) {
			break
		}
		time.Sleep(time.Millisecond)
	}
}

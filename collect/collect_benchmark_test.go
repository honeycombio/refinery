// +build all race

package collect

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/stretchr/testify/assert"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"
)

func BenchmarkCollect(b *testing.B) {
	transmission := &transmit.MockTransmission{}
	transmission.Start()
	conf := &config.MockConfig{
		GetSendDelayVal:    0,
		GetTraceTimeoutVal: 60 * time.Second,
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{},
		SendTickerVal:      2 * time.Millisecond,
	}

	log := &logger.LogrusLogger{}
	log.SetLevel("warn")
	log.Start()

	metric := &metrics.MockMetrics{}
	metric.Start()

	stc, err := lru.New(15)
	assert.NoError(b, err, "lru cache should start")

	coll := &InMemCollector{
		Config:       conf,
		Logger:       log,
		Transmission: transmission,
		Metrics:      metric,
		SamplerFactory: &sample.SamplerFactory{
			Config: conf,
			Logger: log,
		},
		BlockOnAddSpan:  true,
		cache:           cache.NewInMemCache(3, metric, log),
		incoming:        make(chan *types.Span, 500),
		fromPeer:        make(chan *types.Span, 500),
		datasetSamplers: make(map[string]sample.Sampler),
		sentTraceCache:  stc,
	}
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

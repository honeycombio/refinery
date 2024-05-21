package cache

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getCache(typ string, clock clockwork.Clock) SpanCache {
	cfg := &config.MockConfig{
		GetCollectionConfigVal: config.CollectionConfig{
			CacheCapacity: 10000,
		},
		GetTraceTimeoutVal: 10 * time.Second,
		GetSendDelayVal:    2 * time.Second,
	}
	switch typ {
	case "basic":
		return &SpanCache_basic{
			Cfg:     cfg,
			Clock:   clock,
			Metrics: &metrics.NullMetrics{},
		}
	default:
		panic("unknown cache type")
	}
}

func TestSpanCache(t *testing.T) {
	for _, typ := range []string{"basic"} {
		c := getCache(typ, clockwork.NewFakeClock())
		t.Run(typ, func(t *testing.T) {

			err := c.(startstop.Starter).Start()
			require.NoError(t, err)

			// test that we can add a span
			span := &types.Span{
				TraceID: "trace1",
				Event: types.Event{
					APIHost: "apihost",
					APIKey:  "apikey",
					Dataset: "dataset",
				},
			}
			err = c.Set(span)
			require.NoError(t, err)

			// test that we can retrieve the span
			trace := c.Get("trace1")
			require.NotNil(t, trace)
			assert.Equal(t, "trace1", trace.TraceID)
			assert.Equal(t, "apihost", trace.APIHost)
			assert.Equal(t, "apikey", trace.APIKey)
			assert.Equal(t, "dataset", trace.Dataset)
			// assert.Equal(t, c.Clock.Now(), trace.ArrivalTime)
			assert.Equal(t, 1, c.Len())

			// test that we can remove the span
			c.Remove("trace1")
			trace = c.Get("trace1")
			require.Nil(t, trace)
			assert.Equal(t, 0, c.Len())
		})
	}
}

func TestGetHighImpact(t *testing.T) {
	for _, typ := range []string{"basic"} {
		c := getCache(typ, clockwork.NewRealClock())
		t.Run(typ, func(t *testing.T) {

			err := c.(startstop.Starter).Start()
			require.NoError(t, err)

			const numIDs = 20
			ids := make([]string, numIDs)
			for i := 0; i < numIDs; i++ {
				ids[i] = genID(32)
			}
			evt := types.Event{
				APIHost: "apihost",
				APIKey:  "apikey",
				Dataset: "dataset",
			}
			for i := 0; i < numIDs; i++ {
				// we want cache impact to be highest first
				evt.Data = map[string]any{"field": genID(30 - i)}
				span := &types.Span{
					TraceID:     ids[i],
					Event:       evt,
					ArrivalTime: c.GetClock().Now(),
				}
				err := c.Set(span)
				require.NoError(t, err)
			}

			// test that we can retrieve the highest-impact span
			traceIDs := c.GetHighImpactTraceIDs(0.1)
			require.Len(t, traceIDs, 2)
			assert.Equal(t, ids[0], traceIDs[0])
			assert.Equal(t, ids[1], traceIDs[1])
		})
	}
}

func TestGetTraceIDs(t *testing.T) {
	for _, typ := range []string{"basic"} {
		c := getCache(typ, clockwork.NewFakeClock())
		t.Run(typ, func(t *testing.T) {

			err := c.(startstop.Starter).Start()
			require.NoError(t, err)

			for i := 0; i < 10; i++ {
				// test that we can add a span
				span := &types.Span{
					TraceID: fmt.Sprintf("trace%d", i),
					ID:      fmt.Sprintf("span%d", i),
					Event: types.Event{
						APIHost: "apihost",
						APIKey:  "apikey",
						Dataset: "dataset",
					},
				}
				err = c.Set(span)
				require.NoError(t, err)
			}

			// test that we can retrieve the traces in batches
			firstBatch := c.GetTraceIDs(5)
			secondBatch := c.GetTraceIDs(5)
			thirdBatch := c.GetTraceIDs(5)
			require.NotEqualValues(t, firstBatch, secondBatch)
			require.NotNil(t, thirdBatch)
		})
	}
}

func TestGetOldest(t *testing.T) {
	for _, typ := range []string{"basic"} {
		fakeClock := clockwork.NewFakeClock()
		c := getCache(typ, fakeClock) // sets up a cache with a 10s timeout and 2s send delay
		t.Run(typ, func(t *testing.T) {

			err := c.(startstop.Starter).Start()
			require.NoError(t, err)

			const numIDs = 10
			ids := make([]string, numIDs)
			for i := 0; i < numIDs; i++ {
				ids[i] = genID(32)
			}
			evt := types.Event{
				APIHost: "apihost",
				APIKey:  "apikey",
				Dataset: "dataset",
			}
			for i := 0; i < numIDs; i++ {
				// we want cache impact to be highest first
				evt.Data = map[string]any{"field": genID(30 - i)}
				span := &types.Span{
					TraceID:     ids[i],
					Event:       evt,
					ArrivalTime: c.GetClock().Now(),
				}
				err := c.Set(span)
				require.NoError(t, err)
				fakeClock.Advance(1 * time.Second)
			}

			// now we have 10 spans, each 1s apart
			// after 2s more, none should be available from GetOldTraceIDs
			fakeClock.Advance(2 * time.Second)
			// give ourselves a little time to process the spans
			fakeClock.Advance(400 * time.Millisecond)
			traceIDs := c.GetOldTraceIDs()
			require.Len(t, traceIDs, 0)

			// After 13s more, the first 2 spans should be available from GetOldTraceIDs
			// (10s timeout + 2s send delay)*2 + 1s
			fakeClock.Advance(13 * time.Second)
			traceIDs = c.GetOldTraceIDs()
			require.Len(t, traceIDs, 2)
			assert.Contains(t, ids, traceIDs[0])
			assert.Contains(t, ids, traceIDs[1])

			// after another 8s, all spans should be available
			fakeClock.Advance(8 * time.Second)
			traceIDs = c.GetOldTraceIDs()
			require.Len(t, traceIDs, 10)
		})
	}
}

func BenchmarkSpanCacheAdd(b *testing.B) {
	for _, typ := range []string{"basic"} {
		c := getCache(typ, clockwork.NewFakeClock())
		b.Run(typ, func(b *testing.B) {

			c.(startstop.Starter).Start()

			ids := make([]string, b.N)
			for i := 0; i < b.N; i++ {
				ids[i] = genID(32)
			}
			evt := types.Event{
				APIHost: "apihost",
				APIKey:  "apikey",
				Dataset: "dataset",
			}
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				span := &types.Span{
					TraceID: ids[i],
					Event:   evt,
				}
				c.Set(span)
			}
		})
	}
}

func BenchmarkSpanCacheGet(b *testing.B) {
	for _, typ := range []string{"basic"} {
		c := getCache(typ, clockwork.NewFakeClock())
		b.Run(typ, func(b *testing.B) {

			c.(startstop.Starter).Start()

			ids := make([]string, b.N)
			for i := 0; i < b.N; i++ {
				ids[i] = genID(32)
			}
			evt := types.Event{
				APIHost: "apihost",
				APIKey:  "apikey",
				Dataset: "dataset",
			}

			for i := 0; i < b.N; i++ {
				span := &types.Span{
					TraceID: ids[i],
					Event:   evt,
				}
				c.Set(span)
			}
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				c.Get(ids[i])
			}
		})
	}
}

func BenchmarkSpanCacheGetTraceIDs(b *testing.B) {
	for _, typ := range []string{"basic"} {
		c := getCache(typ, clockwork.NewFakeClock())
		b.Run(typ, func(b *testing.B) {

			c.(startstop.Starter).Start()

			ids := make([]string, b.N)
			for i := 0; i < b.N; i++ {
				ids[i] = genID(32)
			}
			evt := types.Event{
				APIHost: "apihost",
				APIKey:  "apikey",
				Dataset: "dataset",
			}

			for i := 0; i < b.N; i++ {
				span := &types.Span{
					TraceID: ids[i],
					Event:   evt,
				}
				c.Set(span)
			}
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				c.GetTraceIDs(50)
			}
		})
	}
}

func BenchmarkSpanCacheMixed(b *testing.B) {
	const numIDs = 10000
	for _, typ := range []string{"basic"} {
		c := getCache(typ, clockwork.NewFakeClock())
		b.Run(typ, func(b *testing.B) {

			c.(startstop.Starter).Start()

			ids := make([]string, numIDs)
			for i := 0; i < numIDs; i++ {
				ids[i] = genID(32)
			}
			evt := types.Event{
				APIHost: "apihost",
				APIKey:  "apikey",
				Dataset: "dataset",
			}

			b.ResetTimer()
			// we have numIDs IDs, and we'll iterate and for each
			// ID we'll either:
			// - add a span (80% of the time)
			// - get a trace (10% of the time)
			// - delete a trace (10% of the time)
			for i := 0; i < b.N; i++ {
				switch rand.Intn(10) {
				case 0:
					c.Remove(ids[i%numIDs])
				case 1:
					c.Get(ids[i%numIDs])
				default:
					span := &types.Span{
						TraceID:  ids[i%numIDs],
						DataSize: 100,
						Event:    evt,
					}
					c.Set(span)
				}
			}
		})
	}
}

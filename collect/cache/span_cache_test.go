package cache

import (
	"math/rand"
	"testing"

	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/config"
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
	}
	switch typ {
	case "basic":
		return &spanCache_basic{
			Clock: clock,
		}
	case "complex":
		return &spanCache_complex{
			Cfg:   cfg,
			Clock: clock,
		}
	default:
		panic("unknown cache type")
	}
}

func TestSpanCache(t *testing.T) {
	for _, typ := range []string{"basic", "complex"} {
		c := getCache(typ, clockwork.NewFakeClock())
		t.Run(typ, func(t *testing.T) {

			err := c.(startstop.Starter).Start()
			require.NoError(t, err)
			defer c.(startstop.Stopper).Stop()

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
			assert.Equal(t, "trace1", trace.TraceID)
			assert.Equal(t, "apihost", trace.APIHost)
			assert.Equal(t, "apikey", trace.APIKey)
			assert.Equal(t, "dataset", trace.Dataset)
			// assert.Equal(t, c.Clock.Now(), trace.ArrivalTime)
			assert.Equal(t, 1, c.Len())

			// test that we can remove the span
			c.Remove("trace1")
			c.Get("trace1")
			require.Equal(t, 0, c.Len())
		})
	}
}

func BenchmarkSpanCacheAdd(b *testing.B) {
	for _, typ := range []string{"basic", "complex"} {
		c := getCache(typ, clockwork.NewFakeClock())
		b.Run(typ, func(b *testing.B) {

			c.(startstop.Starter).Start()
			defer c.(startstop.Stopper).Stop()

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
	for _, typ := range []string{"basic", "complex"} {
		c := getCache(typ, clockwork.NewFakeClock())
		b.Run(typ, func(b *testing.B) {

			c.(startstop.Starter).Start()
			defer c.(startstop.Stopper).Stop()

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
	for _, typ := range []string{"basic", "complex"} {
		c := getCache(typ, clockwork.NewFakeClock())
		b.Run(typ, func(b *testing.B) {

			c.(startstop.Starter).Start()
			defer c.(startstop.Stopper).Stop()

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
func BenchmarkSpanCacheGetOldest(b *testing.B) {
	for _, typ := range []string{"basic", "complex"} {
		c := getCache(typ, clockwork.NewRealClock())
		b.Run(typ, func(b *testing.B) {

			c.(startstop.Starter).Start()
			defer c.(startstop.Stopper).Stop()

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
					TraceID:     ids[i],
					Event:       evt,
					ArrivalTime: c.GetClock().Now(),
				}
				c.Set(span)
			}
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				c.GetOldest(.10)
			}
		})
	}
}

func BenchmarkSpanCacheMixed(b *testing.B) {
	const numIDs = 10000
	for _, typ := range []string{"basic", "complex"} {
		c := getCache(typ, clockwork.NewFakeClock())
		b.Run(typ, func(b *testing.B) {

			c.(startstop.Starter).Start()
			defer c.(startstop.Stopper).Stop()

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
						TraceID: ids[i%numIDs],
						Event:   evt,
					}
					c.Set(span)
				}
			}
		})
	}
}

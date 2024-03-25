package cache

import (
	"testing"

	"github.com/honeycombio/refinery/types"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestXxx(t *testing.T) {
	c := spanCache{
		Clock: clockwork.NewFakeClock(),
	}
	err := c.Start()
	require.NoError(t, err)
	defer c.Stop()

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
	trace, err := c.Get("trace1")
	require.NoError(t, err)
	assert.Equal(t, "trace1", trace.TraceID)
	assert.Equal(t, "apihost", trace.APIHost)
	assert.Equal(t, "apikey", trace.APIKey)
	assert.Equal(t, "dataset", trace.Dataset)
	assert.Equal(t, c.Clock.Now(), trace.ArrivalTime)
	assert.Equal(t, 1, c.Len())

	// test that we can remove the span
	err = c.Remove("trace1")
	require.NoError(t, err)
	_, err = c.Get("trace1")
	require.NoError(t, err) // should not return an error
	require.Equal(t, 0, c.Len())
}

func BenchmarkSpanCacheAdd(b *testing.B) {
	c := spanCache{
		Clock: clockwork.NewFakeClock(),
	}
	c.Start()
	defer c.Stop()

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
			TraceID: "",
			Event:   evt,
		}
		c.Set(span)
	}
}

func BenchmarkSpanCacheGet(b *testing.B) {
	c := spanCache{
		Clock: clockwork.NewFakeClock(),
	}
	c.Start()
	defer c.Stop()

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
			TraceID: "",
			Event:   evt,
		}
		c.Set(span)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.Get(ids[i])
	}
}

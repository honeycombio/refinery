package cache

import (
	"fmt"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testTrace struct {
	TraceID    string
	sentReason uint
}

// ensure that we implement KeptTrace
var _ KeptTrace = (*testTrace)(nil)

func (t *testTrace) ID() string {
	return t.TraceID
}

func (t *testTrace) SampleRate() uint {
	return 17
}

func (t *testTrace) DescendantCount() uint32 {
	return 6
}

func (t *testTrace) SpanEventCount() uint32 {
	return 1
}

func (t *testTrace) SpanLinkCount() uint32 {
	return 2
}

func (t *testTrace) SpanCount() uint32 {
	return 3
}

func (t *testTrace) SetSentReason(r uint) {
	t.sentReason = r
}

func (t *testTrace) SentReason() uint {
	return t.sentReason
}

func Test_cuckooSentCache_Record(t *testing.T) {
	cfg := &config.MockConfig{
		SampleCache: config.SampleCacheConfig{
			KeptSize:          1000,
			DroppedSize:       1000,
			SizeCheckInterval: config.Duration(1 * time.Second),
		},
	}

	c := &CuckooSentCache{
		Cfg: cfg,
		Met: &metrics.NullMetrics{},
	}

	err := c.Start()
	assert.NoError(t, err)
	// add some items to the cache
	for i := 0; i < 100; i++ {
		trace := &testTrace{
			TraceID: fmt.Sprintf("trace%02d", i),
		}
		c.Record(trace, i%2 == 1, "because")
	}
	// check that the last item is in the cache
	tr, reason, found := c.Test("trace99")
	require.True(t, found)
	assert.True(t, tr.Kept())
	assert.Equal(t, "because", reason)
	assert.Equal(t, uint(6), tr.DescendantCount())

	// we need to give the dropped traces cache a chance to run or it might not process everything
	time.Sleep(50 * time.Millisecond)
	tr, reason, found = c.Test("trace98")
	require.True(t, found)
	assert.False(t, tr.Kept())
	assert.Equal(t, "", reason)

	_, _, found = c.Test("traceXX")
	assert.False(t, found)
}

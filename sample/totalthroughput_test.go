package sample

import (
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"

	"github.com/stretchr/testify/assert"
)

func TestTotalThroughputAddSampleRateKeyToTrace(t *testing.T) {
	const spanCount = 5

	metrics := metrics.MockMetrics{}
	metrics.Start()

	sampler := &TotalThroughputSampler{
		Config: &config.TotalThroughputSamplerConfig{
			FieldList: []string{"http.status_code"},
		},
		Logger:  &logger.NullLogger{},
		Metrics: &metrics,
		GetClusterSize: func(useClusterSize bool) int {
			return 1
		},
	}

	trace := &types.Trace{}
	for i := 0; i < spanCount; i++ {
		trace.AddSpan(&types.Span{
			Event: types.Event{
				Data: map[string]interface{}{
					"http.status_code": "200",
				},
			},
		})
	}
	sampler.Start()
	sampler.GetSampleRate(trace)

	spans := trace.GetSpans()
	assert.Len(t, spans, spanCount, "should have the same number of spans as input")
}

package sample

import (
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"

	"github.com/stretchr/testify/assert"
)

func TestEMAThroughputAddSampleRateKeyToTrace(t *testing.T) {
	const spanCount = 5

	metrics := metrics.MockMetrics{}
	metrics.Start()

	sampler := &EMAThroughputSampler{
		Config: &config.EMAThroughputSamplerConfig{
			FieldList: []string{"http.status_code", "request.path", "app.team.id", "important_field"},
		},
		Logger:  &logger.NullLogger{},
		Metrics: &metrics,
		GetClusterSize: func(b bool) int {
			return 1
		},
	}

	trace := &types.Trace{}
	for i := 0; i < spanCount; i++ {
		trace.AddSpan(&types.Span{
			Event: types.Event{
				Data: map[string]interface{}{
					"http.status_code": 200,
					"app.team.id":      float64(4),
					"important_field":  true,
					"request.path":     "/{slug}/fun",
				},
			},
		})
	}
	sampler.Start()
	rate, _, reason, key := sampler.GetSampleRate(trace)

	spans := trace.GetSpans()

	assert.Len(t, spans, spanCount, "should have the same number of spans as input")
	assert.Equal(t, uint(10), rate, "sample rate should be 10")
	assert.Equal(t, "emathroughput", reason)
	assert.Equal(t, "4•,200•,true•,/{slug}/fun•,", key)

}

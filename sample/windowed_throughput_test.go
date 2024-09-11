package sample

import (
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"

	"github.com/stretchr/testify/assert"
)

func TestWindowedThroughputAddSampleRateKeyToTrace(t *testing.T) {
	const spanCount = 5

	metrics := metrics.MockMetrics{}
	metrics.Start()

	sampler := &WindowedThroughputSampler{
		Config: &config.WindowedThroughputSamplerConfig{
			FieldList: []string{"http.status_code", "request.path", "app.team.id", "important_field"},
		},
		Logger:  &logger.NullLogger{},
		Metrics: &metrics,
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
	rate, reason, key := sampler.GetSampleRate(trace)

	spans := trace.GetSpans()

	assert.Len(t, spans, spanCount, "should have the same number of spans as input")
	assert.Equal(t, uint(1), rate, "sample rate should be 1")
	assert.Equal(t, "Windowedthroughput", reason)
	assert.Equal(t, "4•,200•,true•,/{slug}/fun•,", key)

}

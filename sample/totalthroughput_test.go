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
	}

	config := &config.MockConfig{}
	trace := &types.Trace{}
	for range spanCount {
		trace.AddSpan(&types.Span{
			Event: types.Event{
				Data: types.NewPayload(config, map[string]any{
					"http.status_code": "200",
				}),
			},
		})
	}
	sampler.Start()
	sampler.GetSampleRate(trace)

	spans := trace.GetSpans()
	assert.Len(t, spans, spanCount, "should have the same number of spans as input")
}

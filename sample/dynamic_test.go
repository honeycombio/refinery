package sample

import (
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"

	"github.com/stretchr/testify/assert"
)

func TestDynamicAddSampleRateKeyToTrace(t *testing.T) {
	const spanCount = 5

	metrics := metrics.MockMetrics{}
	metrics.Start()

	sampler := &DynamicSampler{
		Config: &config.DynamicSamplerConfig{
			FieldList:  []string{"http.status_code", "root.service_name", "root.url"},
			SampleRate: 1,
		},
		Logger:  &logger.NullLogger{},
		Metrics: &metrics,
	}

	trace := &types.Trace{}
	for i := 0; i < spanCount; i++ {
		if i == spanCount-1 {
			trace.RootSpan = &types.Span{
				Event: types.Event{
					Data: map[string]interface{}{
						"http.status_code": "200",
						"service_name":     "test",
					},
				},
			}
		}
		trace.AddSpan(&types.Span{
			Event: types.Event{
				Data: map[string]interface{}{
					"http.status_code": "200",
					"url":              "/test",
				},
			},
		})
	}
	sampler.Start()
	rate, keep, reason, key := sampler.GetSampleRate(trace)

	spans := trace.GetSpans()
	assert.Len(t, spans, spanCount, "should have the same number of spans as input")
	assert.Equal(t, uint(1), rate)
	assert.True(t, keep)
	assert.Equal(t, "dynamic", reason)
	assert.Equal(t, "200•,test,", key)
}

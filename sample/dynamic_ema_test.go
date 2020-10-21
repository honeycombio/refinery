// +build all race

package sample

import (
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"

	"github.com/stretchr/testify/assert"
)

func TestDynamicEMAAddSampleRateKeyToTrace(t *testing.T) {
	const spanCount = 5

	metrics := metrics.MockMetrics{}
	metrics.Start()

	sampler := &EMADynamicSampler{
		Config: &config.EMADynamicSamplerConfig{
			FieldList:                    []string{"http.status_code", "request.path", "app.team.id", "important_field"},
			AddSampleRateKeyToTrace:      true,
			AddSampleRateKeyToTraceField: "meta.key",
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
	sampler.GetSampleRate(trace)

	spans := trace.GetSpans()

	assert.Len(t, spans, spanCount, "should have the same number of spans as input")
	for _, span := range spans {
		assert.Equal(t, span.Event.Data, map[string]interface{}{
			"http.status_code": 200,
			"app.team.id":      float64(4),
			"important_field":  true,
			"request.path":     "/{slug}/fun",
			"meta.key":         "4•,200•,true•,/{slug}/fun•,",
		}, "should add the sampling key to all spans in the trace")
	}
}

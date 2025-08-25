package sample

import (
	"fmt"
	"sync"
	"testing"
	"time"

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
	mockCfg := &config.MockConfig{}
	for i := 0; i < spanCount; i++ {
		if i == spanCount-1 {
			trace.RootSpan = &types.Span{
				Event: types.Event{
					Data: types.NewPayload(mockCfg, map[string]interface{}{
						"http.status_code": "200",
						"service_name":     "test",
					}),
				},
			}
		}
		trace.AddSpan(&types.Span{
			Event: types.Event{
				Data: types.NewPayload(mockCfg, map[string]interface{}{
					"http.status_code": "200",
					"url":              "/test",
				}),
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

// TestDynamicSamplerConcurrency tests that GetSampleRate is safe to call concurrently
func TestDynamicSamplerConcurrency(t *testing.T) {
	testCases := []struct {
		name   string
		config *config.DynamicSamplerConfig
	}{
		{
			name: "basic_config",
			config: &config.DynamicSamplerConfig{
				FieldList:  []string{"service.name", "http.status_code"},
				SampleRate: 2,
			},
		},
		{
			name: "with_trace_length",
			config: &config.DynamicSamplerConfig{
				FieldList:      []string{"service.name", "http.status_code", "root.operation_name"},
				SampleRate:     5,
				UseTraceLength: true,
				MaxKeys:        100,
			},
		},
		{
			name: "with_clear_frequency",
			config: &config.DynamicSamplerConfig{
				FieldList:      []string{"service.name"},
				SampleRate:     3,
				ClearFrequency: config.Duration(10 * time.Second),
				MaxKeys:        200,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metrics := &metrics.MockMetrics{}
			metrics.Start()

			sampler := &DynamicSampler{
				Config:  tc.config,
				Logger:  &logger.NullLogger{},
				Metrics: metrics,
			}

			err := sampler.Start()
			assert.NoError(t, err)

			// Create test traces with different characteristics
			mockCfg := &config.MockConfig{}
			createTrace := func(serviceName, statusCode, operationName string, spanCount int) *types.Trace {
				trace := &types.Trace{}
				for i := 0; i < spanCount; i++ {
					data := map[string]interface{}{
						"service.name":     serviceName,
						"http.status_code": statusCode,
					}
					// Add root-specific fields for the last span (root span)
					if i == spanCount-1 && operationName != "" {
						data["operation_name"] = operationName
					}

					span := &types.Span{
						Event: types.Event{
							Data: types.NewPayload(mockCfg, data),
						},
					}
					if i == spanCount-1 {
						trace.RootSpan = span
					}
					trace.AddSpan(span)
				}
				return trace
			}

			testTraces := []*types.Trace{
				createTrace("service-a", "200", "GET /users", 5),
				createTrace("service-b", "404", "POST /orders", 3),
				createTrace("service-c", "500", "DELETE /items", 7),
			}

			const numGoroutines = 10
			const iterationsPerGoroutine = 50

			var wg sync.WaitGroup

			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(goroutineID int) {
					defer wg.Done()

					for j := 0; j < iterationsPerGoroutine; j++ {
						// Test predefined traces
						for _, trace := range testTraces {
							rate, _, reason, key := sampler.GetSampleRate(trace)
							assert.NotZero(t, rate, "rate should be positive")
							assert.Equal(t, "dynamic", reason)
							assert.NotEmpty(t, key, "key should not be empty")

							// Verify deterministic parts for same trace (rate, reason, key should be same)
							rate2, _, reason2, key2 := sampler.GetSampleRate(trace)
							assert.Equal(t, rate, rate2, "rate should be deterministic")
							assert.Equal(t, reason, reason2, "reason should be deterministic")
							assert.Equal(t, key, key2, "key should be deterministic")
							// Note: keep value can vary due to randomness, so we don't check it
						}

						// Test with random traces
						randomTrace := createTrace(
							fmt.Sprintf("service-%d", goroutineID),
							fmt.Sprintf("%d", 200+(j%3)*100),
							fmt.Sprintf("op-%d", j),
							(j%5)+1,
						)

						rate, _, reason, key := sampler.GetSampleRate(randomTrace)
						assert.NotZero(t, rate)
						assert.Equal(t, "dynamic", reason)
						assert.NotEmpty(t, key)

						// Test trace length functionality
						if sampler.Config.UseTraceLength {
							// With UseTraceLength, different span counts should affect the key
							shortTrace := createTrace(fmt.Sprintf("service-%d", goroutineID), "200", "short-op", 1)
							longTrace := createTrace(fmt.Sprintf("service-%d", goroutineID), "200", "long-op", 10)

							_, _, _, shortKey := sampler.GetSampleRate(shortTrace)
							_, _, _, longKey := sampler.GetSampleRate(longTrace)

							// Keys should be different due to span count difference
							assert.NotEqual(t, shortKey, longKey, "UseTraceLength should make keys different for different span counts")
						}
					}
				}(i)
			}

			wg.Wait()
		})
	}
}

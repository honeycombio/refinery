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

func TestDynamicEMAAddSampleRateKeyToTrace(t *testing.T) {
	const spanCount = 5

	metrics := metrics.MockMetrics{}
	metrics.Start()

	sampler := &EMADynamicSampler{
		Config: &config.EMADynamicSamplerConfig{
			FieldList: []string{"http.status_code", "request.path", "app.team.id", "important_field"},
		},
		Logger:  &logger.NullLogger{},
		Metrics: &metrics,
	}

	trace := &types.Trace{}
	mockCfg := &config.MockConfig{}
	for i := 0; i < spanCount; i++ {
		trace.AddSpan(&types.Span{
			Event: types.Event{
				Data: types.NewPayload(mockCfg, map[string]interface{}{
					"http.status_code": 200,
					"app.team.id":      float64(4),
					"important_field":  true,
					"request.path":     "/{slug}/fun",
				}),
			},
		})
	}
	sampler.Start()
	rate, _, reason, key := sampler.GetSampleRate(trace)

	spans := trace.GetSpans()

	assert.Len(t, spans, spanCount, "should have the same number of spans as input")
	assert.Equal(t, uint(10), rate, "sample rate should be 10")
	assert.Equal(t, "emadynamic", reason)
	assert.Equal(t, "4•,200•,true•,/{slug}/fun•,", key)

}

// TestEMADynamicSamplerConcurrency tests that GetSampleRate is safe to call concurrently
func TestEMADynamicSamplerConcurrency(t *testing.T) {
	testCases := []struct {
		name   string
		config *config.EMADynamicSamplerConfig
	}{
		{
			name: "basic_config",
			config: &config.EMADynamicSamplerConfig{
				FieldList:      []string{"service.name", "http.status_code"},
				GoalSampleRate: 5,
			},
		},
		{
			name: "with_trace_length_and_burst_detection",
			config: &config.EMADynamicSamplerConfig{
				FieldList:           []string{"service.name", "operation.name", "root.user_id"},
				GoalSampleRate:      10,
				UseTraceLength:      true,
				MaxKeys:             150,
				Weight:              0.3,
				BurstMultiple:       2.5,
				BurstDetectionDelay: 3,
			},
		},
		{
			name: "with_adjustment_interval_and_ageout",
			config: &config.EMADynamicSamplerConfig{
				FieldList:          []string{"team.id", "service.name"},
				GoalSampleRate:     20,
				AdjustmentInterval: config.Duration(5 * time.Second),
				Weight:             0.7,
				AgeOutValue:        0.02,
				MaxKeys:            75,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metrics := &metrics.MockMetrics{}
			metrics.Start()

			sampler := &EMADynamicSampler{
				Config:  tc.config,
				Logger:  &logger.NullLogger{},
				Metrics: metrics,
			}

			err := sampler.Start()
			assert.NoError(t, err)

			// Create test traces with different characteristics
			mockCfg := &config.MockConfig{}
			createTrace := func(serviceName, statusCode, operationName, userID string, teamID int, spanCount int) *types.Trace {
				trace := &types.Trace{}
				for i := 0; i < spanCount; i++ {
					data := map[string]interface{}{
						"service.name":     serviceName,
						"http.status_code": statusCode,
						"operation.name":   operationName,
						"team.id":          teamID,
					}
					// Add root-specific fields for the root span
					if i == spanCount-1 && userID != "" {
						data["user_id"] = userID
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
				createTrace("user-service", "200", "get_profile", "user123", 1, 5),
				createTrace("order-service", "404", "create_order", "user456", 2, 3),
				createTrace("payment-service", "500", "process_payment", "user789", 1, 8),
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
							assert.Equal(t, "emadynamic", reason)
							assert.NotEmpty(t, key, "key should not be empty")

							// Verify deterministic parts for same trace
							rate2, _, reason2, key2 := sampler.GetSampleRate(trace)
							assert.Equal(t, rate, rate2, "rate should be deterministic")
							assert.Equal(t, reason, reason2, "reason should be deterministic")
							assert.Equal(t, key, key2, "key should be deterministic")
						}

						// Test with random traces
						randomTrace := createTrace(
							fmt.Sprintf("service-%d", goroutineID),
							fmt.Sprintf("%d", 200+(j%4)*100),
							fmt.Sprintf("op-%d", j),
							fmt.Sprintf("user-%d-%d", goroutineID, j),
							goroutineID%3+1,
							(j%6)+1,
						)

						rate, _, reason, key := sampler.GetSampleRate(randomTrace)
						assert.NotZero(t, rate)
						assert.Equal(t, "emadynamic", reason)
						assert.NotEmpty(t, key)

						// Test trace length functionality
						if sampler.Config.UseTraceLength {
							shortTrace := createTrace(fmt.Sprintf("svc-%d", goroutineID), "200", "short-op", "user123", 1, 1)
							longTrace := createTrace(fmt.Sprintf("svc-%d", goroutineID), "200", "long-op", "user123", 1, 15)

							_, _, _, shortKey := sampler.GetSampleRate(shortTrace)
							_, _, _, longKey := sampler.GetSampleRate(longTrace)

							assert.NotEqual(t, shortKey, longKey, "UseTraceLength should make keys different for different span counts")
						}
					}
				}(i)
			}

			wg.Wait()
		})
	}
}

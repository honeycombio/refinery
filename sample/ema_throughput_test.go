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
	}

	mockCfg := &config.MockConfig{}
	trace := &types.Trace{}
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
	assert.Equal(t, "emathroughput", reason)
	assert.Equal(t, "4•,200•,true•,/{slug}/fun•,", key)

}

// TestEMAThroughputSamplerConcurrency tests that GetSampleRate is safe to call concurrently
func TestEMAThroughputSamplerConcurrency(t *testing.T) {
	testCases := []struct {
		name   string
		config *config.EMAThroughputSamplerConfig
	}{
		{
			name: "basic_config",
			config: &config.EMAThroughputSamplerConfig{
				FieldList:            []string{"service.name", "http.status_code"},
				GoalThroughputPerSec: 100,
				InitialSampleRate:    10,
			},
		},
		{
			name: "with_cluster_size_and_trace_length",
			config: &config.EMAThroughputSamplerConfig{
				FieldList:            []string{"service.name", "operation.name", "root.user_id"},
				GoalThroughputPerSec: 500,
				InitialSampleRate:    5,
				UseClusterSize:       true,
				UseTraceLength:       true,
				MaxKeys:              200,
				Weight:               0.25,
				BurstMultiple:        3.0,
				BurstDetectionDelay:  5,
			},
		},
		{
			name: "with_adjustment_interval_and_ageout",
			config: &config.EMAThroughputSamplerConfig{
				FieldList:            []string{"team.id", "service.name"},
				GoalThroughputPerSec: 1000,
				InitialSampleRate:    20,
				AdjustmentInterval:   config.Duration(3 * time.Second),
				Weight:               0.6,
				AgeOutValue:          0.01,
				MaxKeys:              100,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metrics := &metrics.MockMetrics{}
			metrics.Start()

			sampler := &EMAThroughputSampler{
				Config:  tc.config,
				Logger:  &logger.NullLogger{},
				Metrics: metrics,
			}

			err := sampler.Start()
			assert.NoError(t, err)
			defer sampler.Stop()

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
				createTrace("api-service", "200", "get_user", "user123", 1, 4),
				createTrace("payment-service", "201", "create_payment", "user456", 2, 6),
				createTrace("order-service", "500", "update_order", "user789", 1, 2),
			}

			const numGoroutines = 10
			const iterationsPerGoroutine = 50

			var wg sync.WaitGroup

			// Test concurrent GetSampleRate calls
			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(goroutineID int) {
					defer wg.Done()

					for j := 0; j < iterationsPerGoroutine; j++ {
						// Test predefined traces
						for _, trace := range testTraces {
							rate, _, reason, key := sampler.GetSampleRate(trace)
							assert.NotZero(t, rate, "rate should be positive")
							assert.Equal(t, "emathroughput", reason)
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
							(j%7)+1,
						)

						rate, _, reason, key := sampler.GetSampleRate(randomTrace)
						assert.NotZero(t, rate)
						assert.Equal(t, "emathroughput", reason)
						assert.NotEmpty(t, key)

						// Test trace length functionality if enabled
						if sampler.Config.UseTraceLength {
							shortTrace := createTrace(fmt.Sprintf("svc-%d", goroutineID), "200", "short", "user123", 1, 1)
							longTrace := createTrace(fmt.Sprintf("svc-%d", goroutineID), "200", "long", "user123", 1, 12)

							_, _, _, shortKey := sampler.GetSampleRate(shortTrace)
							_, _, _, longKey := sampler.GetSampleRate(longTrace)

							assert.NotEqual(t, shortKey, longKey, "UseTraceLength should make keys different for different span counts")
						}
					}
				}(i)
			}

			// Test concurrent SetClusterSize calls if UseClusterSize is enabled
			if tc.config.UseClusterSize {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 20; i++ {
						clusterSize := (i % 5) + 1 // Cluster sizes 1-5
						sampler.SetClusterSize(clusterSize)
						time.Sleep(time.Millisecond) // Small delay to allow interleaving
					}
				}()
			}

			wg.Wait()
		})
	}
}

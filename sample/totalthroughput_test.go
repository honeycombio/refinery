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
	for i := 0; i < spanCount; i++ {
		trace.AddSpan(&types.Span{
			Event: types.Event{
				Data: types.NewPayload(config, map[string]interface{}{
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

// TestTotalThroughputSamplerConcurrency tests that GetSampleRate is safe to call concurrently
func TestTotalThroughputSamplerConcurrency(t *testing.T) {
	testCases := []struct {
		name   string
		config *config.TotalThroughputSamplerConfig
	}{
		{
			name: "basic_config",
			config: &config.TotalThroughputSamplerConfig{
				FieldList:            []string{"service.name", "http.status_code"},
				GoalThroughputPerSec: 100,
			},
		},
		{
			name: "with_cluster_size_and_trace_length",
			config: &config.TotalThroughputSamplerConfig{
				FieldList:            []string{"service.name", "operation.name", "root.user_id"},
				GoalThroughputPerSec: 250,
				UseClusterSize:       true,
				UseTraceLength:       true,
				MaxKeys:              200,
			},
		},
		{
			name: "with_clear_frequency",
			config: &config.TotalThroughputSamplerConfig{
				FieldList:            []string{"team.id", "service.name"},
				GoalThroughputPerSec: 150,
				ClearFrequency:       config.Duration(15 * time.Second),
				MaxKeys:              100,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metrics := &metrics.MockMetrics{}
			metrics.Start()

			sampler := &TotalThroughputSampler{
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
				createTrace("auth-service", "200", "login", "user123", 1, 3),
				createTrace("billing-service", "201", "create_invoice", "user456", 2, 5),
				createTrace("notify-service", "500", "send_email", "user789", 1, 2),
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
							assert.Equal(t, "totalthroughput", reason)
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
						assert.Equal(t, "totalthroughput", reason)
						assert.NotEmpty(t, key)

						// Test trace length functionality if enabled
						if sampler.Config.UseTraceLength {
							shortTrace := createTrace(fmt.Sprintf("svc-%d", goroutineID), "200", "short", "user123", 1, 1)
							longTrace := createTrace(fmt.Sprintf("svc-%d", goroutineID), "200", "long", "user123", 1, 10)

							_, _, _, shortKey := sampler.GetSampleRate(shortTrace)
							_, _, _, longKey := sampler.GetSampleRate(longTrace)

							assert.NotEqual(t, shortKey, longKey, "UseTraceLength should make keys different for different span counts")
						}
					}
				}(i)
			}

			// Cluster size management is now handled by the SamplerFactory

			wg.Wait()
		})
	}
}

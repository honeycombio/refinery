package agent

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestUsageTracker_Add(t *testing.T) {
	tests := []struct {
		name           string
		signalsAndVals []struct {
			signal usageSignal
			val    float64
		}
		expectedLength int
		expectedData   map[usageSignal]float64
	}{
		{
			name: "Add usage data",
			signalsAndVals: []struct {
				signal usageSignal
				val    float64
			}{
				{signal: signal_traces, val: 1},
				{signal: signal_traces, val: 2},
				{signal: signal_logs, val: 3},
				{signal: signal_logs, val: 4},
			},
			expectedLength: 2,
			expectedData: map[usageSignal]float64{
				signal_traces: 2,
				signal_logs:   4,
			},
		},
		{
			name: "Add 0 usage data",
			signalsAndVals: []struct {
				signal usageSignal
				val    float64
			}{
				{signal: signal_traces, val: 0},
				{signal: signal_logs, val: 0},
			},
			expectedData:   make(map[usageSignal]float64),
			expectedLength: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := newUsageTracker()

			for _, sv := range tt.signalsAndVals {
				tracker.Add(sv.signal, sv.val)
			}

			assert.Equal(t, tt.expectedData, tracker.lastUsageData)
			assert.Len(t, tracker.currentDataPoints, tt.expectedLength)
			assert.Equal(t, tt.expectedData, tracker.currentDataPoints)
		})
	}
}

func TestUsageTracker_NewReport(t *testing.T) {
	tests := []struct {
		name           string
		setup          func(tracker *usageTracker)
		expectedError  error
		expectedReport func(now time.Time) pmetric.Metrics
	}{
		{
			name: "Generate usage report",
			setup: func(tracker *usageTracker) {
				tracker.Add(signal_traces, 1)
				tracker.Add(signal_traces, 2)
				tracker.Add(signal_logs, 2)
			},
			expectedError: nil,
			expectedReport: func(now time.Time) pmetric.Metrics {
				metrics := map[usageSignal]float64{
					signal_traces: 2,
					signal_logs:   2,
				}
				return newOTLPResourceMetricsPayload(metrics, now)
			},
		},
		{
			name: "Generate usage report with no data",
			setup: func(tracker *usageTracker) {
				// No data added
			},
			expectedError: errNoData,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := newUsageTracker()
			now := time.Now()

			tt.setup(tracker)

			report, err := tracker.NewReport("my-service", "1.0.0", "my-hostname", now)
			if tt.expectedError != nil {
				require.Error(t, err)
				require.Nil(t, report)
				assert.Equal(t, tt.expectedError, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, report)
				if tt.expectedReport != nil {
					unmarshaler := &pmetric.JSONUnmarshaler{}
					m, err := unmarshaler.UnmarshalMetrics(report)
					require.NoError(t, err)
					require.NoError(t, pmetrictest.CompareMetrics(tt.expectedReport(now), m, pmetrictest.IgnoreMetricDataPointsOrder()))
					assert.Empty(t, tracker.currentDataPoints)
					assert.NotEmpty(t, tracker.lastDataPoints)
				}
			}

			tracker.completeSend()
			assert.Empty(t, tracker.lastDataPoints)
		})
	}
}

func newOTLPResourceMetricsPayload(metrics map[usageSignal]float64, now time.Time) pmetric.Metrics {
	var dataPoints []map[string]any
	for signal, value := range metrics {
		dataPoints = append(dataPoints, map[string]any{
			"attributes":   []map[string]any{{"key": "signal", "value": map[string]string{"stringValue": string(signal)}}},
			"timeUnixNano": fmt.Sprintf("%d", now.UnixNano()),
			"asInt":        fmt.Sprintf("%d", int(value)),
		})
	}

	payload := map[string]any{
		"resourceMetrics": []map[string]any{
			{
				"resource": map[string]any{
					"attributes": []map[string]any{
						{"key": "service.name", "value": map[string]string{"stringValue": "my-service"}},
						{"key": "service.version", "value": map[string]string{"stringValue": "1.0.0"}},
						{"key": "host.name", "value": map[string]string{"stringValue": "my-hostname"}},
					},
				},
				"scopeMetrics": []map[string]any{
					{
						"metrics": []map[string]any{
							{
								"name": "bytes_received",
								"sum": map[string]any{
									"aggregationTemporality": 1,
									"dataPoints":             dataPoints,
								},
							},
						},
						"scope": map[string]any{},
					},
				},
			},
		},
	}
	bytes, _ := json.Marshal(payload)
	unmarshaler := &pmetric.JSONUnmarshaler{}
	m, _ := unmarshaler.UnmarshalMetrics(bytes)
	return m
}

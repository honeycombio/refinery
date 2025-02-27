package agent

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUsageTracker_Add(t *testing.T) {
	tests := []struct {
		name                 string
		usageData            []usage
		expectedLength       int
		expectedData         []usage
		expectedLasUsageData map[usageSignal]usage
	}{
		{
			name: "Add usage data",
			usageData: []usage{
				{signal: signal_traces, val: 1},
				{signal: signal_traces, val: 2},
				{signal: signal_logs, val: 3},
				{signal: signal_logs, val: 4},
			},
			expectedLength: 4,
			expectedData: []usage{
				{signal: signal_traces, val: 1},
				{signal: signal_traces, val: 1},
				{signal: signal_logs, val: 3},
				{signal: signal_logs, val: 1},
			},
			expectedLasUsageData: map[usageSignal]usage{
				signal_traces: {signal: signal_traces, val: 2},
				signal_logs:   {signal: signal_logs, val: 4},
			},
		},
		{
			name: "Add 0 usage data",
			usageData: []usage{
				{signal: signal_traces, val: 0},
				{signal: signal_logs, val: 0},
			},
			expectedLength: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := newUsageTracker()
			now := time.Now()

			for _, data := range tt.usageData {
				data.timestamp = now
				tracker.Add(data)
			}

			if tt.expectedLasUsageData != nil {
				assert.Equal(t, tt.expectedLasUsageData[signal_traces], tracker.lastUsageData[signal_traces])
				assert.Equal(t, tt.expectedLasUsageData[signal_logs], tracker.lastUsageData[signal_logs])
			}
			assert.Len(t, tracker.currentDataPoints, tt.expectedLength)
			for i, expected := range tt.expectedData {
				assert.Equal(t, expected.signal, tracker.currentDataPoints[i].signal)
				assert.Equal(t, expected.val, tracker.currentDataPoints[i].val)
				assert.Equal(t, now, tracker.currentDataPoints[i].timestamp, time.Second)
			}
		})
	}
}
func TestUsageTracker_NewReport(t *testing.T) {
	tests := []struct {
		name           string
		setup          func(tracker *usageTracker, now time.Time)
		expectedError  error
		expectedReport func(now time.Time) string
	}{
		{
			name: "Generate usage report",
			setup: func(tracker *usageTracker, now time.Time) {
				tracker.Add(newTraceCumulativeUsage(1, now))
				tracker.Add(newLogCumulativeUsage(2, now))
			},
			expectedError: nil,
			expectedReport: func(now time.Time) string {
				data := newOTLPResourceMetricsPayload([]usage{
					{signal: signal_traces, val: 1, timestamp: now},
					{signal: signal_logs, val: 2, timestamp: now},
				})
				return string(data)
			},
		},
		{
			name: "Generate usage report with no data",
			setup: func(tracker *usageTracker, now time.Time) {
				// No data added
			},
			expectedError:  errNoData,
			expectedReport: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := newUsageTracker()
			now := time.Now()

			tt.setup(tracker, now)

			report, err := tracker.NewReport("my-service", "1.0.0", "my-hostname")
			if tt.expectedError != nil {
				require.Error(t, err)
				require.Nil(t, report)
				assert.Equal(t, tt.expectedError, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, report)
				if tt.expectedReport != nil {
					assert.JSONEq(t, tt.expectedReport(now), string(report))
					assert.Empty(t, tracker.currentDataPoints)
					assert.NotEmpty(t, tracker.lastDataPoints)
				}
			}

			tracker.completeSend()
			assert.Empty(t, tracker.lastDataPoints)
		})
	}
}

func newOTLPResourceMetricsPayload(data []usage) []byte {
	var dataPoints []map[string]interface{}
	for _, d := range data {
		dataPoints = append(dataPoints, map[string]interface{}{
			"attributes":   []map[string]interface{}{{"key": "signal", "value": map[string]string{"stringValue": string(d.signal)}}},
			"timeUnixNano": fmt.Sprintf("%d", d.timestamp.UnixNano()),
			"asInt":        fmt.Sprintf("%d", int(d.val)),
		})
	}

	payload := map[string]interface{}{
		"resourceMetrics": []map[string]interface{}{
			{
				"resource": map[string]interface{}{
					"attributes": []map[string]interface{}{
						{"key": "service.name", "value": map[string]string{"stringValue": "my-service"}},
						{"key": "service.version", "value": map[string]string{"stringValue": "1.0.0"}},
						{"key": "host.name", "value": map[string]string{"stringValue": "my-hostname"}},
					},
				},
				"scopeMetrics": []map[string]interface{}{
					{
						"metrics": []map[string]interface{}{
							{
								"name": "bytes_received",
								"sum": map[string]interface{}{
									"aggregationTemporality": 1,
									"dataPoints":             dataPoints,
								},
							},
						},
						"scope": map[string]interface{}{},
					},
				},
			},
		},
	}
	bytes, _ := json.Marshal(payload)
	return bytes
}

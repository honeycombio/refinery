package agent

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/jonboulle/clockwork"
	"github.com/open-telemetry/opamp-go/client"
	"github.com/open-telemetry/opamp-go/client/types"
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestAgentOnMessage_RemoteConfig(t *testing.T) {
	var reloadCalled int
	cfg := &config.MockConfig{
		GetLoggerLevelVal:  config.InfoLevel,
		GetSamplerTypeVal:  "FakeSamplerType",
		GetSamplerTypeName: "FakeSamplerName",
	}
	cfg.Callbacks = []config.ConfigReloadCallback{
		func(configHash, ruleCfgHash string) {
			reloadCalled++
		},
	}
	agent := NewAgent(Logger{Logger: &logger.NullLogger{}}, "1.0.0", cfg, &metrics.NullMetrics{}, &health.Health{})
	defer agent.Stop(context.Background())

	testcases := []struct {
		name                string
		configMap           map[string]*protobufs.AgentConfigFile
		configHash          []byte
		expectedReloadCount int
		status              protobufs.RemoteConfigStatuses
	}{
		{
			name:                "empty config map",
			configMap:           map[string]*protobufs.AgentConfigFile{},
			configHash:          []byte{0},
			expectedReloadCount: 0,
		},
		{
			name: "new refinery config from remote config",
			configMap: map[string]*protobufs.AgentConfigFile{
				"refinery_config": {
					Body:        []byte(`{"Logger":{"Level":"debug"}}`),
					ContentType: "text/yaml",
				},
			},
			configHash:          []byte{1},
			expectedReloadCount: 1,
			status:              protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED,
		},
		{
			name: "new refinery rules from remote config",
			configMap: map[string]*protobufs.AgentConfigFile{
				"refinery_config": {
					Body:        []byte(`{"Logger":{"Level":"debug"}}`),
					ContentType: "text/yaml",
				},
				"refinery_rules": {
					Body: []byte(`{"rules":[{"name":"test","type":"fake"]}`),
				},
			},
			configHash:          []byte{2},
			expectedReloadCount: 2,
			status:              protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED,
		},
		{
			name: "same remote config should not cause reload",
			configMap: map[string]*protobufs.AgentConfigFile{
				"refinery_config": {
					Body:        []byte(`{"Logger":{"Level":"debug"}}`),
					ContentType: "text/yaml",
				},
				"refinery_rules": {
					Body: []byte(`{"rules":[{"name":"test","type":"fake"]}`),
				},
			},
			configHash:          []byte{2},
			expectedReloadCount: 2,
			status:              protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED,
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			agent.onMessage(context.Background(), &types.MessageData{
				RemoteConfig: &protobufs.AgentRemoteConfig{
					Config: &protobufs.AgentConfigMap{
						ConfigMap: tc.configMap,
					},
					ConfigHash: tc.configHash,
				},
			})
			require.Eventually(t, func() bool {
				return tc.expectedReloadCount == reloadCalled
			}, 1*time.Second, 100*time.Millisecond, fmt.Sprintf("unexpected reload count %d", reloadCalled))

			require.Equal(t, tc.status, agent.remoteConfigStatus.GetStatus(), fmt.Sprintf("unexpected status %s", agent.remoteConfigStatus.GetStatus()))
			if tc.status == protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED {
				assert.Equal(t, tc.configHash, agent.remoteConfigStatus.GetLastRemoteConfigHash())
				assert.Equal(t, tc.configHash, agent.remoteConfig.GetConfigHash())
				assert.Equal(t, tc.configMap, agent.remoteConfig.GetConfig().GetConfigMap())
			}
		})
	}

}

func TestHealthCheck(t *testing.T) {
	healthReporter := &health.MockHealthReporter{}
	agent := NewAgent(Logger{Logger: &logger.NullLogger{}}, "1.0.0", &config.MockConfig{}, &metrics.NullMetrics{}, healthReporter)
	defer agent.Stop(context.Background())

	// health check should start with false
	require.False(t, agent.calculateHealth().Healthy)

	// health check should be false if Refinery is not ready
	healthReporter.SetAlive(true)
	require.False(t, agent.calculateHealth().Healthy)

	// health check should be true if both alive and ready are true
	healthReporter.SetReady(true)
	require.True(t, agent.calculateHealth().Healthy)
}

func TestAgentUsageReport(t *testing.T) {
	mockClient := &MockOpAMPClient{}
	ctx, cancel := context.WithCancel(context.Background())
	clock := clockwork.NewFakeClock()
	agent := &Agent{
		ctx:             ctx,
		cancel:          cancel,
		logger:          Logger{&logger.NullLogger{}},
		agentType:       serviceName,
		agentVersion:    "1.0.0",
		opampClient:     mockClient,
		effectiveConfig: &config.MockConfig{},
		metrics:         &metrics.NullMetrics{},
		health:          &health.MockHealthReporter{},
		usageTracker:    newUsageTracker(),
		clock:           clock,
	}
	agent.createAgentIdentity()
	agent.hostname = "my-hostname"
	defer cancel()

	isSent := make(chan struct{})
	close(isSent)
	mockClient.On("SendCustomMessage", mock.Anything).Return(isSent, nil)

	agent.usageTracker.Add(signal_traces, 1)
	agent.usageTracker.Add(signal_logs, 2)
	agent.usageTracker.Add(signal_traces, 3)
	agent.usageTracker.Add(signal_logs, 4)

	err := agent.sendUsageReport()
	require.NoError(t, err)

	// Format the timestamp to match the expected format in the payload
	timeUnixNano := clock.Now()

	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	resourceAttrs := rm.Resource().Attributes()
	resourceAttrs.PutStr("service.name", "refinery")
	resourceAttrs.PutStr("service.version", "1.0.0")
	resourceAttrs.PutStr("host.name", "my-hostname")
	sm := rm.ScopeMetrics().AppendEmpty()
	ms := sm.Metrics().AppendEmpty()
	ms.SetName("bytes_received")
	sum := ms.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	d1 := sum.DataPoints().AppendEmpty()
	d1.Attributes().PutStr("signal", "traces")
	d1.SetIntValue(3)
	d1.SetTimestamp(pcommon.NewTimestampFromTime(timeUnixNano))
	d2 := sum.DataPoints().AppendEmpty()
	d2.Attributes().PutStr("signal", "logs")
	d2.SetIntValue(4)
	d2.SetTimestamp(pcommon.NewTimestampFromTime(timeUnixNano))

	matcher := mock.MatchedBy(func(payload *protobufs.CustomMessage) bool {
		if payload.Capability != sendAgentTelemetryCapability {
			return false
		}

		unmarshaler := &pmetric.JSONUnmarshaler{}
		m, err := unmarshaler.UnmarshalMetrics(payload.Data)
		require.NoError(t, err)

		return pmetrictest.CompareMetrics(metrics, m, pmetrictest.IgnoreMetricDataPointsOrder()) == nil
	})
	// Assert that the mock client was called with the expected custom message
	mockClient.AssertCalled(t, "SendCustomMessage", matcher)

}

// MockOpAMPClient is a mock implementation of the OpAMPClient interface
type MockOpAMPClient struct {
	mock.Mock
	client.OpAMPClient
}

func (m *MockOpAMPClient) SendCustomMessage(msg *protobufs.CustomMessage) (chan struct{}, error) {
	args := m.Called(msg)
	return args.Get(0).(chan struct{}), args.Error(1)
}

func (m *MockOpAMPClient) SetHealth(health *protobufs.ComponentHealth) error {
	return nil
}

func (m *MockOpAMPClient) Stop(ctx context.Context) error {
	return nil
}

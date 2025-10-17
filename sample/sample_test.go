package sample

import (
	"os"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/facebookgo/inject"
	dynsampler "github.com/honeycombio/dynsampler-go"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// These helper functions are copied from config/config_test.go
func getConfig(args []string) (config.Config, error) {
	opts, err := config.NewCmdEnvOptions(args)
	if err != nil {
		return nil, err
	}
	return config.NewConfig(opts)
}

// creates two temporary yaml files from the strings passed in and returns their filenames
func createTempConfigs(t testing.TB, configBody, rulesBody string) (string, string) {
	tmpDir := t.TempDir()

	configFile, err := os.CreateTemp(tmpDir, "cfg_*.yaml")
	assert.NoError(t, err)

	_, err = configFile.WriteString(configBody)
	assert.NoError(t, err)
	configFile.Close()

	rulesFile, err := os.CreateTemp(tmpDir, "rules_*.yaml")
	assert.NoError(t, err)

	_, err = rulesFile.WriteString(rulesBody)
	assert.NoError(t, err)
	rulesFile.Close()

	return configFile.Name(), rulesFile.Name()
}

// This sets up a map by breaking the key at / (note that the one in config uses .)
func setMap(m map[string]any, key string, value any) {
	if strings.Contains(key, "/") {
		parts := strings.Split(key, "/")
		if _, ok := m[parts[0]]; !ok {
			m[parts[0]] = make(map[string]any)
		}
		setMap(m[parts[0]].(map[string]any), strings.Join(parts[1:], "/"), value)
		return
	}
	m[key] = value
}

func makeYAML(args ...interface{}) string {
	m := make(map[string]any)
	for i := 0; i < len(args); i += 2 {
		setMap(m, args[i].(string), args[i+1])
	}
	b, err := yaml.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func TestDependencyInjection(t *testing.T) {
	var g inject.Graph
	err := g.Provide(
		&inject.Object{Value: &SamplerFactory{}},

		&inject.Object{Value: &config.MockConfig{}},
		&inject.Object{Value: &logger.NullLogger{}},
		&inject.Object{Value: &metrics.NullMetrics{}, Name: "metrics"},
		&inject.Object{Value: peer.NewMockPeers([]string{"foo", "bar"}, "")},
	)
	if err != nil {
		t.Error(err)
	}
	if err := g.Populate(); err != nil {
		t.Error(err)
	}
}

func TestTotalThroughputClusterSize(t *testing.T) {
	cm := makeYAML(
		"General/ConfigurationVersion", 2,
	)
	rm := makeYAML(
		"RulesVersion", 2,
		"Samplers/__default__/DeterministicSampler/SampleRate", 1,
		"Samplers/production/TotalThroughputSampler/GoalThroughputPerSec", 10,
		"Samplers/production/TotalThroughputSampler/UseClusterSize", true,
	)
	cfg, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", cfg, "--rules_config", rules})
	assert.NoError(t, err)

	factory := SamplerFactory{
		Config:  c,
		Logger:  &logger.NullLogger{},
		Metrics: &metrics.NullMetrics{},
		Peers:   peer.NewMockPeers([]string{"foo", "bar"}, ""),
	}
	factory.Start()
	sampler := factory.GetSamplerImplementationForKey("production")
	sampler.Start()
	assert.NotNil(t, sampler)
	impl := sampler.(*TotalThroughputSampler)
	defer impl.dynsampler.Stop()
	assert.Equal(t, 5, impl.dynsampler.GoalThroughputPerSec)
}

func TestEMAThroughputClusterSize(t *testing.T) {
	cm := makeYAML(
		"General/ConfigurationVersion", 2,
	)
	rm := makeYAML(
		"RulesVersion", 2,
		"Samplers/__default__/DeterministicSampler/SampleRate", 1,
		"Samplers/production/EMAThroughputSampler/GoalThroughputPerSec", 10,
		"Samplers/production/EMAThroughputSampler/UseClusterSize", true,
	)
	cfg, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", cfg, "--rules_config", rules})
	assert.NoError(t, err)

	factory := SamplerFactory{
		Config:  c,
		Logger:  &logger.NullLogger{},
		Metrics: &metrics.NullMetrics{},
		Peers:   peer.NewMockPeers([]string{"foo", "bar"}, ""),
	}
	factory.Start()
	sampler := factory.GetSamplerImplementationForKey("production")
	sampler.Start()
	assert.NotNil(t, sampler)
	impl := sampler.(*EMAThroughputSampler)
	defer impl.dynsampler.Stop()
	assert.Equal(t, 5, impl.dynsampler.GoalThroughputPerSec)
}

func TestWindowedThroughputClusterSize(t *testing.T) {
	cm := makeYAML(
		"General/ConfigurationVersion", 2,
	)
	rm := makeYAML(
		"RulesVersion", 2,
		"Samplers/__default__/DeterministicSampler/SampleRate", 1,
		"Samplers/production/WindowedThroughputSampler/GoalThroughputPerSec", 10,
		"Samplers/production/WindowedThroughputSampler/UseClusterSize", true,
	)
	cfg, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", cfg, "--rules_config", rules})
	assert.NoError(t, err)

	factory := SamplerFactory{
		Config:  c,
		Logger:  &logger.NullLogger{},
		Metrics: &metrics.NullMetrics{},
		Peers:   peer.NewMockPeers([]string{"foo", "bar"}, ""),
	}
	factory.Start()
	sampler := factory.GetSamplerImplementationForKey("production")
	sampler.Start()
	assert.NotNil(t, sampler)
	impl := sampler.(*WindowedThroughputSampler)
	defer impl.dynsampler.Stop()
	assert.Equal(t, 5.0, impl.dynsampler.GoalThroughputPerSec)
}

func TestRulesBasedSamplerGetKeyFields(t *testing.T) {
	// Test that RulesBasedSampler correctly aggregates fields from conditions and samplers
	tests := []struct {
		name                  string
		rules                 *config.RulesBasedSamplerConfig
		expectedAll           []string
		expectedNonRootFields []string
	}{
		{
			name: "fields from multiple rules",
			rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Conditions: []*config.RulesBasedSamplerCondition{
							{Field: "service.name"},
							{Field: "root.operation.name"},
						},
					},
					{
						Conditions: []*config.RulesBasedSamplerCondition{
							{Field: "http.status_code"},
							{Field: "root.user.id"},
						},
					},
				},
			},
			expectedAll:           []string{"http.status_code", "operation.name", "service.name", "user.id"},
			expectedNonRootFields: []string{"http.status_code", "service.name"},
		},
		{
			name: "fields from dynamic sampler",
			rules: &config.RulesBasedSamplerConfig{
				Rules: []*config.RulesBasedSamplerRule{
					{
						Conditions: []*config.RulesBasedSamplerCondition{
							{Field: "test"},
						},
						Sampler: &config.RulesBasedDownstreamSampler{
							DynamicSampler: &config.DynamicSamplerConfig{
								FieldList: []string{"service.name", "root.operation.name"},
							},
						},
					},
				},
			},
			expectedAll:           []string{"operation.name", "service.name", "test"},
			expectedNonRootFields: []string{"service.name", "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sampler := &RulesBasedSampler{
				Config:  tt.rules,
				Logger:  &logger.NullLogger{},
				Metrics: &metrics.NullMetrics{},
			}
			sampler.Start()

			allFields, nonRootFields := sampler.GetKeyFields()
			// Sort for comparison since order may vary
			sortedFields := slices.Clone(allFields)
			sort.Strings(sortedFields)
			assert.Equal(t, tt.expectedAll, sortedFields, "all fields should match expected")

			sortedFields = slices.Clone(nonRootFields)
			sort.Strings(sortedFields)
			assert.Equal(t, tt.expectedNonRootFields, sortedFields, "non-root fields should match expected")
		})
	}
}

func TestDynsamplerMetricsRecorder_RegisterMetrics(t *testing.T) {
	t.Run("registers internal dynsampler metrics", func(t *testing.T) {
		mockMetrics := &metrics.MockMetrics{}
		mockMetrics.Start()
		defer mockMetrics.Stop()
		mockSampler := &MockSampler{}

		internalMetrics := map[string]int64{
			"test_gauge_metric":  100,
			"test_counter_count": 50,
			"test_another_gauge": 200,
		}
		mockSampler.On("GetMetrics", "test_").Return(internalMetrics)

		recorder := &dynsamplerMetricsRecorder{
			prefix: "test",
			met:    mockMetrics,
		}

		recorder.RegisterMetrics(mockSampler)

		// Verify internal state
		assert.Equal(t, "test_", recorder.dynPrefix)
		assert.Len(t, recorder.lastMetrics, 3)

		assert.Equal(t, internalDysamplerMetric{
			metricType: metrics.Gauge,
			val:        100,
		}, recorder.lastMetrics["test_gauge_metric"])

		assert.Equal(t, internalDysamplerMetric{
			metricType: metrics.Counter,
			val:        50,
		}, recorder.lastMetrics["test_counter_count"])

		// Check metric names mapping
		assert.Equal(t, "test_num_dropped", recorder.metricNames.numDropped)
		assert.Equal(t, "test_num_kept", recorder.metricNames.numKept)
		assert.Equal(t, "test_sample_rate", recorder.metricNames.sampleRate)
		assert.Equal(t, "test_sampler_key_cardinality", recorder.metricNames.samplerKeyCardinality)

		mockSampler.AssertExpectations(t)
	})

	t.Run("handles empty metrics from sampler", func(t *testing.T) {
		mockMetrics := &metrics.MockMetrics{}
		mockMetrics.Start()
		mockSampler := &MockSampler{}

		mockSampler.On("GetMetrics", "empty_").Return(map[string]int64{})

		recorder := &dynsamplerMetricsRecorder{
			prefix:      "empty",
			lastMetrics: make(map[string]internalDysamplerMetric),
			met:         mockMetrics,
		}

		recorder.RegisterMetrics(mockSampler)

		assert.Equal(t, "empty_", recorder.dynPrefix)
		assert.Len(t, recorder.lastMetrics, 0)

		mockSampler.AssertExpectations(t)
	})
}

func TestDynsamplerMetricsRecorder_RecordMetrics(t *testing.T) {
	mockMetrics := &metrics.MockMetrics{}
	mockMetrics.Start()
	mockSampler := &MockSampler{}

	recorder := &dynsamplerMetricsRecorder{
		prefix: "test",
		met:    mockMetrics,
	}

	initialMetrics := map[string]int64{
		"test_requests_count":     100,
		"test_errors_count":       10,
		"test_active_connections": 25,
		"test_memory_usage":       1024,
	}
	mockSampler.On("GetMetrics", "test_").Return(initialMetrics).Once()
	recorder.RegisterMetrics(mockSampler)

	assert.Equal(t, int64(0), mockMetrics.CounterIncrements["test_requests_count"])
	assert.Equal(t, int64(0), mockMetrics.CounterIncrements["test_errors_count"])

	assert.Equal(t, int64(0), mockMetrics.CounterIncrements["test_num_kept"])
	assert.Equal(t, int64(0), mockMetrics.CounterIncrements["test_num_dropped"])
	assert.Len(t, mockMetrics.Histograms["test_sampler_key_cardinality"], 0)
	assert.Len(t, mockMetrics.Histograms["test_sample_rate"], 0)
	assert.Equal(t, float64(0), mockMetrics.GaugeRecords["test_active_connections"])
	assert.Equal(t, float64(0), mockMetrics.GaugeRecords["test_memory_usage"])

	mockSampler.AssertExpectations(t)

	updatedMetrics := map[string]int64{
		"test_requests_count":     150,
		"test_errors_count":       15,
		"test_active_connections": 55,
		"test_memory_usage":       2048,
	}

	mockSampler.On("GetMetrics", "test_").Return(updatedMetrics).Once()

	recorder.RecordMetrics(mockSampler, true, 20, 30)

	// Verify delta counts were recorded
	assert.Equal(t, int64(50), mockMetrics.CounterIncrements["test_requests_count"]) // 150 - 100
	assert.Equal(t, int64(5), mockMetrics.CounterIncrements["test_errors_count"])    // 15 - 10

	// Verify sampler metrics
	assert.Equal(t, int64(1), mockMetrics.CounterIncrements["test_num_kept"])
	assert.Equal(t, int64(0), mockMetrics.CounterIncrements["test_num_dropped"])
	assert.Len(t, mockMetrics.Histograms["test_sampler_key_cardinality"], 1)
	assert.Equal(t, mockMetrics.Histograms["test_sampler_key_cardinality"][0], float64(30))
	assert.Len(t, mockMetrics.Histograms["test_sample_rate"], 1)
	assert.Equal(t, mockMetrics.Histograms["test_sample_rate"][0], float64(20))

	// Verify direct gauge values (not deltas)
	assert.Equal(t, float64(55), mockMetrics.GaugeRecords["test_active_connections"])
	assert.Equal(t, float64(2048), mockMetrics.GaugeRecords["test_memory_usage"])

	mockSampler.AssertExpectations(t)
}

var _ dynsampler.Sampler = (*MockSampler)(nil)

// MockSampler implements dynsampler.Sampler for testing
type MockSampler struct {
	mock.Mock
}

func (m *MockSampler) GetMetrics(prefix string) map[string]int64 {
	args := m.Called(prefix)
	return args.Get(0).(map[string]int64)
}

func (m *MockSampler) GetSampleRate(key string) int {
	args := m.Called(key)
	return args.Get(0).(int)
}

func (m *MockSampler) GetSampleRateMulti(key string, val int) int {
	args := m.Called(key, val)
	return args.Get(0).(int)
}

func (m *MockSampler) Start() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockSampler) Stop() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockSampler) LoadState(state []byte) error {
	return nil
}

func (m *MockSampler) SaveState() ([]byte, error) {
	return nil, nil
}

// TestPeerMockWithCallback is a test peer implementation that supports triggering callbacks
type TestPeerMockWithCallback struct {
	peers    []string
	callback func()
}

func (p *TestPeerMockWithCallback) GetPeers() ([]string, error) {
	return p.peers, nil
}

func (p *TestPeerMockWithCallback) GetInstanceID() (string, error) {
	return "test-instance", nil
}

func (p *TestPeerMockWithCallback) RegisterUpdatedPeersCallback(callback func()) {
	p.callback = callback
}

func (p *TestPeerMockWithCallback) Ready() error {
	return nil
}

func (p *TestPeerMockWithCallback) Start() error {
	return nil
}

func (p *TestPeerMockWithCallback) TriggerUpdate() {
	if p.callback != nil {
		p.callback()
	}
}

func TestDifferentDatasetsShouldNotShareDynsampler(t *testing.T) {
	// This test demonstrates the bug where different datasets with identical
	// sampler configs incorrectly share the same dynsampler instance
	cm := makeYAML(
		"General/ConfigurationVersion", 2,
	)
	rm := makeYAML(
		"RulesVersion", 2,
		"Samplers/__default__/DeterministicSampler/SampleRate", 1,
		"Samplers/production/TotalThroughputSampler/GoalThroughputPerSec", 10,
		"Samplers/production/TotalThroughputSampler/UseClusterSize", true,
		"Samplers/production/TotalThroughputSampler/FieldList", []string{"service.name"},
		"Samplers/dogfood/TotalThroughputSampler/GoalThroughputPerSec", 10,
		"Samplers/dogfood/TotalThroughputSampler/UseClusterSize", true,
		"Samplers/dogfood/TotalThroughputSampler/FieldList", []string{"service.name"},
	)
	cfg, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", cfg, "--rules_config", rules})
	require.NoError(t, err)

	factory := SamplerFactory{
		Config:  c,
		Logger:  &logger.NullLogger{},
		Metrics: &metrics.NullMetrics{},
		Peers:   peer.NewMockPeers([]string{"foo", "bar"}, ""),
	}
	factory.Start()
	defer factory.Stop()

	// Get samplers for both datasets
	prodSampler := factory.GetSamplerImplementationForKey("production")
	dogfoodSampler := factory.GetSamplerImplementationForKey("dogfood")

	assert.NotNil(t, prodSampler)
	assert.NotNil(t, dogfoodSampler)

	prodImpl := prodSampler.(*TotalThroughputSampler)
	dogfoodImpl := dogfoodSampler.(*TotalThroughputSampler)

	assert.NotEqual(t, prodImpl.dynsampler, dogfoodImpl.dynsampler)
	assert.Equal(t, prodImpl.dynsampler.GoalThroughputPerSec, dogfoodImpl.dynsampler.GoalThroughputPerSec)
}

// TestClusterSizeUpdatesSamplers verifies that the SamplerFactory properly handles dynamic peer updates
// and their impact on throughput-based sampling behavior.
func TestClusterSizeUpdatesSamplers(t *testing.T) {
	// Create a test peer manager that we can control
	testPeers := peer.NewMockPeers([]string{"peer1"}, "")

	// Create a SamplerFactory directly for testing the cluster size behavior
	cm := makeYAML(
		"General/ConfigurationVersion", 2,
	)
	rm := makeYAML(
		"RulesVersion", 2,
		"Samplers/__default__/DeterministicSampler/SampleRate", 1,
		"Samplers/production/TotalThroughputSampler/GoalThroughputPerSec", 100,
		"Samplers/production/TotalThroughputSampler/UseClusterSize", true,
		"Samplers/production/TotalThroughputSampler/FieldList", []string{"service.name"},
	)
	cfg, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", cfg, "--rules_config", rules})
	assert.NoError(t, err)

	// Create and start the SamplerFactory
	factory := SamplerFactory{
		Config:  c,
		Logger:  &logger.NullLogger{},
		Metrics: &metrics.NullMetrics{},
		Peers:   testPeers,
	}

	err = factory.Start()
	require.NoError(t, err)

	// Create a sampler to verify initial throughput
	sampler := factory.GetSamplerImplementationForKey("production")
	require.NotNil(t, sampler)

	// It should be a TotalThroughputSampler
	throughputSampler, ok := sampler.(*TotalThroughputSampler)
	require.True(t, ok, "Expected TotalThroughputSampler")

	// With 1 peer, throughput should be 100 (full original value)
	assert.Equal(t, 100, throughputSampler.dynsampler.GoalThroughputPerSec)

	// Add a second peer
	testPeers.UpdatePeers([]string{"peer1", "peer2"})

	// Throughput should now be 100/2 = 50
	assert.Eventually(t, func() bool {
		return throughputSampler.dynsampler.GoalThroughputPerSec == 50
	}, 2*time.Second, 50*time.Millisecond, "Throughput should be updated to 50 with 2 peers")

	// Add a third peer
	testPeers.UpdatePeers([]string{"peer1", "peer2", "peer3"})

	// Throughput should now be 100/3 = 33 (rounded down to at least 1)
	assert.Eventually(t, func() bool {
		return throughputSampler.dynsampler.GoalThroughputPerSec == 33
	}, 2*time.Second, 50*time.Millisecond, "Throughput should be updated to 33 with 3 peers")

	// Remove a peer (back to 2)
	testPeers.UpdatePeers([]string{"peer1", "peer2"})

	// Throughput should be back to 100/2 = 50
	assert.Eventually(t, func() bool {
		return throughputSampler.dynsampler.GoalThroughputPerSec == 50
	}, 2*time.Second, 50*time.Millisecond, "Throughput should be updated back to 50 with 2 peers")

	// Create another sampler instance with the same key to verify cluster size behavior is consistent
	sampler2 := factory.GetSamplerImplementationForKey("production")
	require.NotNil(t, sampler2)

	throughputSampler2, ok := sampler2.(*TotalThroughputSampler)
	require.True(t, ok, "Expected TotalThroughputSampler")

	// Should have the same dynsampler instance due to shared dynsampler architecture
	assert.Same(t, throughputSampler.dynsampler, throughputSampler2.dynsampler, "Should share the same dynsampler instance for the same key")

	// Verify that creating a new sampler for a different dataset also gets the updated throughput
	// But first add a rule for it
	cm2 := makeYAML(
		"General/ConfigurationVersion", 2,
	)
	rm2 := makeYAML(
		"RulesVersion", 2,
		"Samplers/__default__/DeterministicSampler/SampleRate", 1,
		"Samplers/production/TotalThroughputSampler/GoalThroughputPerSec", 100,
		"Samplers/production/TotalThroughputSampler/UseClusterSize", true,
		"Samplers/production/TotalThroughputSampler/FieldList", []string{"service.name"},
		"Samplers/test2/TotalThroughputSampler/GoalThroughputPerSec", 200,
		"Samplers/test2/TotalThroughputSampler/UseClusterSize", true,
		"Samplers/test2/TotalThroughputSampler/FieldList", []string{"service.name"},
	)
	cfg2, rules2 := createTempConfigs(t, cm2, rm2)
	c2, err := getConfig([]string{"--no-validate", "--config", cfg2, "--rules_config", rules2})
	assert.NoError(t, err)

	// Create another factory with different config to test different throughput values
	factory2 := SamplerFactory{
		Config:  c2,
		Logger:  &logger.NullLogger{},
		Metrics: &metrics.NullMetrics{},
		Peers:   testPeers, // Same peers with 2 peers
	}
	err = factory2.Start()
	require.NoError(t, err)

	sampler3 := factory2.GetSamplerImplementationForKey("test2")
	require.NotNil(t, sampler3)

	throughputSampler3, ok := sampler3.(*TotalThroughputSampler)
	require.True(t, ok, "Expected TotalThroughputSampler")

	// This sampler should have different base throughput (200) but same cluster division (200/2=100)
	assert.Eventually(t, func() bool {
		return throughputSampler3.dynsampler.GoalThroughputPerSec == 100
	}, 2*time.Second, 50*time.Millisecond, "New sampler with 200 throughput should have 100 with 2 peers")

	// Cleanup dynsampler instances to prevent goroutine leaks
	throughputSampler.dynsampler.Stop()
	throughputSampler3.dynsampler.Stop()
}

func BenchmarkGetSamplerImplementation(b *testing.B) {
	cm := makeYAML(
		"General/ConfigurationVersion", 2,
	)
	rm := makeYAML(
		"RulesVersion", 2,
		"Samplers/__default__/DeterministicSampler/SampleRate", 1,
		"Samplers/test/DynamicSampler/SampleRate", 1,
		"Samplers/test2/EMADynamicSampler/SampleRate", 1,
		"Samplers/test3/WindowedThroughputSampler/SampleRate", 1,
		"Samplers/test4/TotalThroughputSampler/SampleRate", 1,
		"Samplers/test5/EMAThroughputSampler/SampleRate", 1,
	)
	cfg, rules := createTempConfigs(b, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", cfg, "--rules_config", rules})
	assert.NoError(b, err)
	mockPeers := peer.NewMockPeers([]string{"foo", "bar"}, "")
	mockPeers.Start()

	factory := SamplerFactory{
		Config:  c,
		Logger:  &logger.NullLogger{},
		Metrics: &metrics.NullMetrics{},
		Peers:   mockPeers,
	}
	factory.Start()

	// Define the keys to distribute evenly
	keys := []string{"default", "test", "test2", "test3", "test4", "test5"}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 5 {
				factory.updatePeerCounts()
				i++
				continue
			}
			// Evenly distribute among the 6 keys
			key := keys[i%len(keys)]

			_ = factory.GetSamplerImplementationForKey(key)
			i++
		}
	})
}

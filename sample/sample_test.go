package sample

import (
	"os"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/facebookgo/inject"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/stretchr/testify/assert"
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
func createTempConfigs(t *testing.T, configBody, rulesBody string) (string, string) {
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
		&inject.Object{Value: &metrics.NullMetrics{}, Name: "genericMetrics"},
		&inject.Object{Value: &peer.MockPeers{Peers: []string{"foo", "bar"}}},
	)
	if err != nil {
		t.Error(err)
	}
	if err := g.Populate(); err != nil {
		t.Error(err)
	}
}

func TestDatasetPrefix(t *testing.T) {
	cm := makeYAML(
		"General/ConfigurationVersion", 2,
		"General/DatasetPrefix", "dataset",
	)
	rm := makeYAML(
		"RulesVersion", 2,
		"Samplers/__default__/DeterministicSampler/SampleRate", 1,
		"Samplers/production/DeterministicSampler/SampleRate", 10,
		"Samplers/dataset.production/DeterministicSampler/SampleRate", 20,
	)
	cfg, rules := createTempConfigs(t, cm, rm)
	c, err := getConfig([]string{"--no-validate", "--config", cfg, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, "dataset", c.GetDatasetPrefix())

	factory := SamplerFactory{Config: c, Logger: &logger.NullLogger{}, Metrics: &metrics.NullMetrics{}}
	factory.Start()

	defaultSampler := &DeterministicSampler{
		Config: &config.DeterministicSamplerConfig{SampleRate: 1},
		Logger: &logger.NullLogger{},
	}
	defaultSampler.Start()

	envSampler := &DeterministicSampler{
		Config: &config.DeterministicSamplerConfig{SampleRate: 10},
		Logger: &logger.NullLogger{},
	}
	envSampler.Start()

	datasetSampler := &DeterministicSampler{
		Config: &config.DeterministicSamplerConfig{SampleRate: 20},
		Logger: &logger.NullLogger{},
	}
	datasetSampler.Start()

	assert.Equal(t, defaultSampler, factory.GetSamplerImplementationForKey("unknown", false))
	assert.Equal(t, defaultSampler, factory.GetSamplerImplementationForKey("unknown", true))
	assert.Equal(t, envSampler, factory.GetSamplerImplementationForKey("production", false))
	assert.Equal(t, datasetSampler, factory.GetSamplerImplementationForKey("production", true))
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
		Peers:   &peer.MockPeers{Peers: []string{"foo", "bar"}},
	}
	factory.Start()
	sampler := factory.GetSamplerImplementationForKey("production", false)
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
		Peers:   &peer.MockPeers{Peers: []string{"foo", "bar"}},
	}
	factory.Start()
	sampler := factory.GetSamplerImplementationForKey("production", false)
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
		Peers:   &peer.MockPeers{Peers: []string{"foo", "bar"}},
	}
	factory.Start()
	sampler := factory.GetSamplerImplementationForKey("production", false)
	sampler.Start()
	assert.NotNil(t, sampler)
	impl := sampler.(*WindowedThroughputSampler)
	defer impl.dynsampler.Stop()
	assert.Equal(t, 5.0, impl.dynsampler.GoalThroughputPerSec)
}

func TestGetKeyFields(t *testing.T) {
	tests := []struct {
		name                  string
		input                 []string
		expectedAll           []string
		expectedNonRootFields []string
	}{
		{
			name:                  "empty slice",
			input:                 []string{},
			expectedAll:           nil,
			expectedNonRootFields: nil,
		},
		{
			name:                  "nil input",
			input:                 nil,
			expectedAll:           nil,
			expectedNonRootFields: nil,
		},
		{
			name:                  "no root fields",
			input:                 []string{"service.name", "operation.name", "duration_ms"},
			expectedAll:           []string{"service.name", "operation.name", "duration_ms"},
			expectedNonRootFields: []string{"service.name", "operation.name", "duration_ms"},
		},
		{
			name:                  "only root fields",
			input:                 []string{"root.service.name", "root.operation.name", "root.duration_ms"},
			expectedAll:           []string{"service.name", "operation.name", "duration_ms"},
			expectedNonRootFields: []string{},
		},
		{
			name:                  "mixed root and non-root fields",
			input:                 []string{"service.name", "root.operation.name", "duration_ms", "root.user.id"},
			expectedAll:           []string{"operation.name", "user.id", "service.name", "duration_ms"},
			expectedNonRootFields: []string{"service.name", "duration_ms"},
		},
		{
			name:                  "duplicate fields with and without root prefix",
			input:                 []string{"service.name", "root.service.name", "operation.name"},
			expectedAll:           []string{"service.name", "service.name", "operation.name"},
			expectedNonRootFields: []string{"service.name", "operation.name"},
		},
		{
			name:                  "fields with dots in names",
			input:                 []string{"root.http.request.method", "http.response.status", "root.db.query.time"},
			expectedAll:           []string{"http.request.method", "db.query.time", "http.response.status"},
			expectedNonRootFields: []string{"http.response.status"},
		},
		{
			name:                  "single non-root field",
			input:                 []string{"test"},
			expectedAll:           []string{"test"},
			expectedNonRootFields: []string{"test"},
		},
		{
			name:                  "single root field",
			input:                 []string{"root.test"},
			expectedAll:           []string{"test"},
			expectedNonRootFields: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allFields, nonrootFields := getKeyFields(tt.input)

			assert.Equal(t, tt.expectedAll, allFields, "All fields should have root prefix stripped and be combined")
			assert.Equal(t, tt.expectedNonRootFields, nonrootFields, "Non-root fields should match expected non-root fields")
		})
	}
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

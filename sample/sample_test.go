package sample

import (
	"os"
	"strings"
	"testing"

	"github.com/facebookgo/inject"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

// These helper functions are copied from config/config_test.go
func getConfig(args []string) (config.Config, error) {
	opts, err := config.NewCmdEnvOptions(args)
	if err != nil {
		return nil, err
	}
	return config.NewConfig(opts, func(err error) {})
}

// creates two temporary yaml files from the strings passed in and returns their filenames
func createTempConfigs(t *testing.T, configBody, rulesBody string) (string, string) {
	tmpDir, err := os.MkdirTemp("", "")
	assert.NoError(t, err)

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
	for _, tc := range []struct {
		name               string
		apiKey             string
		dataset            string
		environment        string
		expectedSamplerKey string
	}{
		{"legacy", "c9945edf5d245834089a1bd6cc9ad01e", "dataset", "environment", "prefix.dataset"},
		{"non-legacy", "d245834089a1bd6cc9ad01e", "dataset", "environment", "environment"},
	} {
		trace := &types.Trace{APIKey: tc.apiKey, Dataset: tc.dataset}
		trace.AddSpan(&types.Span{Event: types.Event{Environment: tc.environment}})

		assert.Equal(t, tc.expectedSamplerKey, trace.GetSamplerSelector("prefix"))
	}
}

func TestTotalThroughputClusterSize(t *testing.T) {
	t.Skip()
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
	defer os.Remove(rules)
	defer os.Remove(cfg)
	c, err := getConfig([]string{"--no-validate", "--config", cfg, "--rules_config", rules})
	assert.NoError(t, err)

	factory := SamplerFactory{
		Config:  c,
		Logger:  &logger.NullLogger{},
		Metrics: &metrics.NullMetrics{},
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
	t.Skip()
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
	defer os.Remove(rules)
	defer os.Remove(cfg)
	c, err := getConfig([]string{"--no-validate", "--config", cfg, "--rules_config", rules})
	assert.NoError(t, err)

	factory := SamplerFactory{
		Config:  c,
		Logger:  &logger.NullLogger{},
		Metrics: &metrics.NullMetrics{},
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
	t.Skip()
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
	defer os.Remove(rules)
	defer os.Remove(cfg)
	c, err := getConfig([]string{"--no-validate", "--config", cfg, "--rules_config", rules})
	assert.NoError(t, err)

	factory := SamplerFactory{
		Config:  c,
		Logger:  &logger.NullLogger{},
		Metrics: &metrics.NullMetrics{},
	}
	factory.Start()
	sampler := factory.GetSamplerImplementationForKey("production")
	sampler.Start()
	assert.NotNil(t, sampler)
	impl := sampler.(*WindowedThroughputSampler)
	defer impl.dynsampler.Stop()
	assert.Equal(t, 5.0, impl.dynsampler.GoalThroughputPerSec)
}

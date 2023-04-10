package sample

import (
	"os"
	"testing"

	"github.com/facebookgo/inject"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/stretchr/testify/assert"
)

// These two helper functions are copied from config/config_test.go
func getConfig(args []string) (config.Config, error) {
	opts, err := config.NewCmdEnvOptions(args)
	if err != nil {
		return nil, err
	}
	return config.NewConfig(opts, func(err error) {})
}

// creates two temporary toml files from the strings passed in and returns their filenames
func createTempConfigs(t *testing.T, configBody string, rulesBody string) (string, string) {
	tmpDir, err := os.MkdirTemp("", "")
	assert.NoError(t, err)

	configFile, err := os.CreateTemp(tmpDir, "cfg_*.toml")
	assert.NoError(t, err)

	if configBody != "" {
		_, err = configFile.WriteString(configBody)
		assert.NoError(t, err)
	}
	configFile.Close()

	rulesFile, err := os.CreateTemp(tmpDir, "rules_*.toml")
	assert.NoError(t, err)

	if rulesBody != "" {
		_, err = rulesFile.WriteString(rulesBody)
		assert.NoError(t, err)
	}
	rulesFile.Close()

	return configFile.Name(), rulesFile.Name()
}

func TestDependencyInjection(t *testing.T) {
	var g inject.Graph
	err := g.Provide(
		&inject.Object{Value: &SamplerFactory{}},

		&inject.Object{Value: &config.MockConfig{}},
		&inject.Object{Value: &logger.NullLogger{}},
		&inject.Object{Value: &metrics.NullMetrics{}, Name: "genericMetrics"},
	)
	if err != nil {
		t.Error(err)
	}
	if err := g.Populate(); err != nil {
		t.Error(err)
	}
}

func TestDatasetPrefix(t *testing.T) {
	cfg, rules := createTempConfigs(t, `
	DatasetPrefix = "dataset"

	[InMemCollector]
		CacheCapacity=1000

	[HoneycombMetrics]
		MetricsHoneycombAPI="http://honeycomb.io"
		MetricsAPIKey="1234"
		MetricsDataset="testDatasetName"
		MetricsReportingInterval=3

	[HoneycombLogger]
		LoggerHoneycombAPI="http://honeycomb.io"
		LoggerAPIKey="1234"
		LoggerDataset="loggerDataset"
	`, `
	Sampler = "DeterministicSampler"
	SampleRate = 1

	[production]
		Sampler = "DeterministicSampler"
		SampleRate = 10

	["dataset.production"]
		Sampler = "DeterministicSampler"
		SampleRate = 20
	`)
	defer os.Remove(rules)
	defer os.Remove(cfg)
	c, err := getConfig([]string{"--config", cfg, "--rules_config", rules})
	assert.NoError(t, err)

	assert.Equal(t, "dataset", c.GetDatasetPrefix())

	factory := SamplerFactory{Config: c, Logger: &logger.NullLogger{}, Metrics: &metrics.NullMetrics{}}

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

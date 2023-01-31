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
	tmpDir, err := os.MkdirTemp("", "")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	configFile, err := os.CreateTemp(tmpDir, "*.toml")
	assert.NoError(t, err)

	_, err = configFile.Write([]byte(`
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
	`))
	assert.NoError(t, err)
	configFile.Close()

	rulesFile, err := os.CreateTemp(tmpDir, "*.toml")
	assert.NoError(t, err)

	_, err = rulesFile.Write([]byte(`
	Sampler = "DeterministicSampler"
	SampleRate = 1

	[production]
		Sampler = "DeterministicSampler"
		SampleRate = 10

	[dataset.production]
		Sampler = "DeterministicSampler"
		SampleRate = 20
	`))
	assert.NoError(t, err)
	rulesFile.Close()

	c, err := config.NewConfig(configFile.Name(), rulesFile.Name(), func(err error) {})
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

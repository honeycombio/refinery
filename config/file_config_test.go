package config

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestDefaultHoneycombLoggerConfig(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	rulesFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.NoError(t, err)

	configFile, err := ioutil.TempFile(tmpDir, "*.toml")
	assert.NoError(t, err)

	dummy := []byte(`
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
	`)

	_, err = configFile.Write(dummy)
	assert.NoError(t, err)
	configFile.Close()

	c, err := NewConfig(configFile.Name(), rulesFile.Name(), func(err error) {})

	assert.NoError(t, err)

	loggerConfig, err := c.GetHoneycombLoggerConfig()

	assert.NoError(t, err)

	assert.Equal(t, false, loggerConfig.LoggerSamplerEnabled)
	assert.Equal(t, 5, loggerConfig.LoggerSamplerThroughput)
}

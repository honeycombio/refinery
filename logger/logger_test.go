package logger

import (
	"os"
	"testing"

	"github.com/honeycombio/refinery/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOTelLoggerStartWithConfigFile(t *testing.T) {
	yaml := `file_format: "0.3"
logger_provider:
  processors:
    - simple:
        exporter:
          console: {}
resource:
  attributes:
    - name: service.name
      value: test-refinery
    - name: deployment.environment
      value: test
`
	f, err := os.CreateTemp(t.TempDir(), "otelconfig-*.yaml")
	require.NoError(t, err)
	_, err = f.WriteString(yaml)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	cfg := &config.MockConfig{
		GetLoggerLevelVal: config.WarnLevel,
		GetOTelLoggerConfigVal: config.OTelLoggerConfig{
			ConfigFile: f.Name(),
		},
	}
	l := &OTelLogger{Config: cfg, Version: "test"}
	require.NoError(t, l.Start())
	assert.NotNil(t, l.logEmitter)
	require.NoError(t, l.Stop())
}

func TestHoneycombLoggerRespectsLogLevelAfterStart(t *testing.T) {
	cfg := &config.MockConfig{
		GetLoggerLevelVal:           config.WarnLevel,
		GetHoneycombLoggerConfigVal: config.HoneycombLoggerConfig{},
	}
	hcLogger := &HoneycombLogger{
		Config:       cfg,
		level:        config.WarnLevel,
		loggerConfig: config.HoneycombLoggerConfig{},
	}

	assert.Equal(t, config.WarnLevel, hcLogger.level)
	err := hcLogger.Start()
	assert.Nil(t, err)
	assert.Equal(t, config.WarnLevel, hcLogger.level)
}

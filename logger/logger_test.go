package logger

import (
	"testing"

	"github.com/honeycombio/refinery/config"

	"github.com/stretchr/testify/assert"
)

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

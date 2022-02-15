// +build all race

package logger

import (
	"testing"

	"github.com/honeycombio/refinery/config"

	"github.com/stretchr/testify/assert"
)

func TestHoneycombLoggerRespectsLogLevelAfterStart(t *testing.T) {
	cfg := &config.MockConfig{GetHoneycombLoggerConfigVal: config.HoneycombLoggerConfig{}}
	hcLogger := &HoneycombLogger{
		Config:       cfg,
		loggerConfig: config.HoneycombLoggerConfig{Level: WarnLevel},
	}

	assert.Equal(t, WarnLevel, hcLogger.loggerConfig.Level)
	err := hcLogger.Start()
	assert.Nil(t, err)
	assert.Equal(t, WarnLevel, hcLogger.loggerConfig.Level)
}

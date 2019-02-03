package logger

import (
	"testing"

	"github.com/honeycombio/samproxy/config"

	"github.com/stretchr/testify/assert"
)

func TestHoneycombLoggerRespectsLogLevelAfterStart(t *testing.T) {
	cfg := &config.MockConfig{GetOtherConfigVal: `{}`}
	hcLogger := &HoneycombLogger{
		Config:       cfg,
		loggerConfig: HoneycombLoggerConfig{level: WarnLevel},
	}

	assert.Equal(t, WarnLevel, hcLogger.loggerConfig.level)
	err := hcLogger.Start()
	assert.Nil(t, err)
	assert.Equal(t, WarnLevel, hcLogger.loggerConfig.level)
}

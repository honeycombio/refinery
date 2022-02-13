//go:build all || race
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

func TestConfigReloadRaciness(t *testing.T) {
	cfg := &config.MockConfig{}
	hcLogger := &HoneycombLogger{
		Config: cfg,
	}

	err := hcLogger.Start()

	assert.NoError(t, err)
	assert.Equal(t, 1, len(cfg.Callbacks))

	// imitate concurrent config reload
	for i := 0; i < 10; i++ {
		go func(j int) {
			cfg.Callbacks[0]()
		}(i)

		go func(j int) {
			cfg.Callbacks[0]()
		}(i)
		hcLogger.Debug().WithField("1", "2")
		hcLogger.Info().WithString("3", "4")
		hcLogger.Error().WithFields(map[string]interface{}{"5": "6"})
		hcLogger.Info().Logf("hi")
		err = hcLogger.SetLevel("INFO")
		assert.NoError(t, err)
	}

}

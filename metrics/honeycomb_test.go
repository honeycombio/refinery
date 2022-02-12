//go:build all || race
// +build all race

package metrics

import (
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/stretchr/testify/assert"
)

func TestConfigReloadRaciness(t *testing.T) {
	mockConfig := config.MockConfig{}
	p := &HoneycombMetrics{
		Logger:        &logger.NullLogger{},
		Config:        &mockConfig,
		reportingFreq: 1, // 1 second
	}

	err := p.Start()

	assert.NoError(t, err)
	assert.Equal(t, 1, len(mockConfig.Callbacks))

	// imitate concurrent config reload
	for i := 0; i < 10; i++ {
		go func(j int) {
			mockConfig.Callbacks[0]()
		}(i)

		go func(j int) {
			mockConfig.Callbacks[0]()
		}(i)

		// sleep long enough that we report metrics
		time.Sleep(1100 * time.Millisecond)
	}
}

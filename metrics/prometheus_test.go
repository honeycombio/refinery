// +build all race

package metrics

import (
	"fmt"
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/stretchr/testify/assert"
)

func TestMultipleRegistrations(t *testing.T) {
	p := &PromMetrics{
		Logger: &logger.MockLogger{},
		Config: &config.MockConfig{},
	}

	err := p.Start()

	assert.NoError(t, err)

	p.Register("test", "counter")

	p.Register("test", "counter")
}

func TestRaciness(t *testing.T) {
	p := &PromMetrics{
		Logger: &logger.MockLogger{},
		Config: &config.MockConfig{},
	}

	err := p.Start()

	assert.NoError(t, err)

	p.Register("race", "counter")

	// this loop modifying the metric registry and reading it to increment
	// a counter should not trigger a race condition
	for i := 0; i < 50; i++ {
		go func(j int) {
			metricName := fmt.Sprintf("metric%d", j)
			p.Register(metricName, "counter")
		}(i)

		go func(j int) {
			p.Increment("race")
		}(i)
	}
}

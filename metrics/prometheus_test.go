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

	p.Register(Metadata{
		Name: "test",
		Type: Counter,
	})

	p.Register(Metadata{
		Name: "test",
		Type: Counter,
	})
}

func TestRaciness(t *testing.T) {
	p := &PromMetrics{
		Logger: &logger.MockLogger{},
		Config: &config.MockConfig{},
	}

	err := p.Start()

	assert.NoError(t, err)

	p.Register(Metadata{
		Name: "race",
		Type: Counter,
	})

	// this loop modifying the metric registry and reading it to increment
	// a counter should not trigger a race condition
	for i := 0; i < 50; i++ {
		go func(j int) {
			metricName := fmt.Sprintf("metric%d", j)
			p.Register(Metadata{
				Name: metricName,
				Type: Counter,
			})
		}(i)

		go func(j int) {
			p.Increment("race")
		}(i)
	}
}

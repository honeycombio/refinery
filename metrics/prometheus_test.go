package metrics

import (
	"fmt"
	"sync"
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Prometheus_MultipleRegistrations(t *testing.T) {
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

func Test_Prometheus_Raciness(t *testing.T) {
	p := &PromMetrics{
		Logger: &logger.MockLogger{},
		Config: &config.MockConfig{},
	}

	err := p.Start()
	require.NoError(t, err)

	p.Register(Metadata{
		Name: "race",
		Type: Counter,
	})

	var wg sync.WaitGroup
	loopLength := 50

	// this loop modifying the metric registry and reading it to increment
	// a counter should not trigger a race condition
	for i := 0; i < loopLength; i++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			metricName := fmt.Sprintf("metric%d", j)
			p.Register(Metadata{
				Name: metricName,
				Type: Counter,
			})
		}(i)

		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			p.Increment("race")
		}(i)
	}

	wg.Wait()
	assert.Len(t, p.metrics, loopLength+1)
}

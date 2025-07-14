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

func Test_OTelMetrics_MultipleRegistrations(t *testing.T) {
	o := &OTelMetrics{
		Logger: &logger.MockLogger{},
		Config: &config.MockConfig{},
	}

	err := o.Start()
	defer o.Stop()

	require.NoError(t, err)

	o.Register(Metadata{
		Name: "test",
		Type: Counter,
	})

	o.Register(Metadata{
		Name: "test",
		Type: Counter,
	})
}

func Test_OTelMetrics_Raciness(t *testing.T) {
	o := &OTelMetrics{
		Logger: &logger.MockLogger{},
		Config: &config.MockConfig{},
	}

	err := o.Start()
	defer o.Stop()

	require.NoError(t, err)

	o.Register(Metadata{
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
			o.Register(Metadata{
				Name: metricName,
				Type: Counter,
			})
		}(i)

		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			o.Increment("race")
		}(i)
	}

	wg.Wait()
	assert.Len(t, o.counters, loopLength+1)
}

func Benchmark_OTelMetrics_ConcurrentAccess(b *testing.B) {
	o := &OTelMetrics{
		Logger: &logger.NullLogger{},
		Config: &config.MockConfig{},
	}
	err := o.Start()
	defer o.Stop()
	require.NoError(b, err)

	o.Register(Metadata{Name: "test_counter", Type: Counter})
	o.Register(Metadata{Name: "test_gauge", Type: Gauge})
	o.Register(Metadata{Name: "test_histogram", Type: Histogram})
	o.Register(Metadata{Name: "test_updown", Type: UpDown})

	b.Run("ConcurrentCounters", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				o.Count("test_counter", 1)
			}
		})
	})

	b.Run("ConcurrentGauges", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				o.Gauge("test_gauge", float64(i))
				i++
			}
		})
	})

	b.Run("ConcurrentHistograms", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				o.Histogram("test_histogram", float64(i))
				i++
			}
		})
	})

	b.Run("ConcurrentMixed", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				switch i % 4 {
				case 0:
					o.Count("test_counter", 1)
				case 1:
					o.Gauge("test_gauge", float64(i))
				case 2:
					o.Histogram("test_histogram", float64(i))
				case 3:
					if i%2 == 0 {
						o.Up("test_updown")
					} else {
						o.Down("test_updown")
					}
				}
				i++
			}
		})
	})
}

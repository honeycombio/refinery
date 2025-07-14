package metrics

import (
	"fmt"
	"sync"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

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

	assert.Contains(t, o.counters, "test")
}

func Test_OTelMetrics_Raciness(t *testing.T) {

	rdr := sdkmetric.NewManualReader()

	o := &OTelMetrics{
		Logger:     &logger.MockLogger{},
		Config:     &config.MockConfig{},
		testReader: rdr,
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

	rm := metricdata.ResourceMetrics{}
	err = rdr.Collect(t.Context(), &rm)
	require.NoError(t, err)
	require.Len(t, rm.ScopeMetrics, 1, "We should have emitted at least one scope's worth of metrics.")
	sm := rm.ScopeMetrics[0]
	require.GreaterOrEqual(t, len(sm.Metrics), loopLength, "We should have created at least as many OTel metrics as we used in the loop.")

	// confirm the counter we incremented in the loop got each increment
	// and didn't get overwritten by concurrent use
	want := metricdata.Metrics{
		Name: "race",
		Data: metricdata.Sum[int64]{
			Temporality: metricdata.CumulativeTemporality,
			IsMonotonic: true,
			DataPoints: []metricdata.DataPoint[int64]{
				{Value: int64(loopLength)},
			},
		},
	}

	var got metricdata.Metrics
	for _, m := range sm.Metrics {
		if m.Name == "race" {
			got = m
		}
	}

	metricdatatest.AssertEqual(t, want, got, metricdatatest.IgnoreTimestamp())
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

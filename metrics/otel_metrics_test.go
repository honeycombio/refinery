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

	// Check that the counter exists in the sync.Map
	_, exists := o.counters.Load("test")
	assert.True(t, exists)
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

	// Count the number of counters in the sync.Map
	counterCount := 0
	o.counters.Range(func(key, value interface{}) bool {
		counterCount++
		return true
	})
	assert.Equal(t, loopLength+1, counterCount)

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

func Test_OTelMetrics_AdditionalAttributes(t *testing.T) {
	rdr := sdkmetric.NewManualReader()

	o := &OTelMetrics{
		Logger: &logger.MockLogger{},
		Config: &config.MockConfig{
			GetOTelMetricsConfigVal: config.OTelMetricsConfig{
				AdditionalAttributes: map[string]string{
					"cluster.id":  "test-cluster-123",
					"environment": "staging",
				},
			},
		},
		testReader: rdr,
	}

	err := o.Start()
	defer o.Stop()
	require.NoError(t, err)

	// Emit a metric so we can collect resource data
	o.Register(Metadata{Name: "test_attr", Type: Counter})
	o.Increment("test_attr")

	rm := metricdata.ResourceMetrics{}
	err = rdr.Collect(t.Context(), &rm)
	require.NoError(t, err)

	// Check that the additional attributes are present as resource attributes
	attrs := rm.Resource.Attributes()
	attrMap := make(map[string]string)
	for _, attr := range attrs {
		attrMap[string(attr.Key)] = attr.Value.AsString()
	}

	assert.Equal(t, "test-cluster-123", attrMap["cluster.id"], "cluster.id resource attribute should be set")
	assert.Equal(t, "staging", attrMap["environment"], "environment resource attribute should be set")
	assert.Equal(t, "refinery", attrMap["service.name"], "service.name should still be present")
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

func Test_OTelMetrics_HistogramBatch(t *testing.T) {
	rdr := sdkmetric.NewManualReader()
	o := &OTelMetrics{
		Logger:     &logger.MockLogger{},
		Config:     &config.MockConfig{},
		testReader: rdr,
	}
	require.NoError(t, o.Start())
	defer o.Stop()

	o.Register(Metadata{Name: "test_hist_batch", Type: Histogram})
	o.Register(Metadata{Name: "test_hist_each", Type: Histogram})

	obs := []float64{0.5, 3, 3, 100, 70000, 5e10}
	for _, v := range obs {
		o.Histogram("test_hist_each", v)
	}
	o.HistogramBatch("test_hist_batch", obs)
	// an empty batch records nothing
	o.HistogramBatch("test_hist_batch", nil)

	rm := metricdata.ResourceMetrics{}
	require.NoError(t, rdr.Collect(t.Context(), &rm))
	points := map[string]metricdata.HistogramDataPoint[float64]{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if h, ok := m.Data.(metricdata.Histogram[float64]); ok {
				require.Len(t, h.DataPoints, 1)
				points[m.Name] = h.DataPoints[0]
			}
		}
	}
	batch, each := points["test_hist_batch"], points["test_hist_each"]
	require.NotZero(t, batch.Count, "the batch histogram recorded nothing")

	// Registering a histogram records an initial 0, so both have it.
	assert.EqualValues(t, len(obs)+1, batch.Count)
	assert.InDelta(t, 5e10+70106.5, batch.Sum, 1e-3)
	assert.Equal(t, each.Count, batch.Count)
	assert.Equal(t, each.Sum, batch.Sum)
	assert.Equal(t, each.BucketCounts, batch.BucketCounts)
	assert.Equal(t, each.Min, batch.Min)
	assert.Equal(t, each.Max, batch.Max)
}

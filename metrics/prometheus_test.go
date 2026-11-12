package metrics

import (
	"fmt"
	"sync"
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testPrometheusMultipleRegistrationsOnce sync.Once

func Test_Prometheus_MultipleRegistrations(t *testing.T) {
	// Unfortunately the prometheus SDK panics if given duplicate metrics
	// registrations, which are global. This means this test will panic if run
	// more than once.
	var doTest bool
	testPrometheusMultipleRegistrationsOnce.Do(func() {
		doTest = true
	})
	if !doTest {
		t.Skip("PromMetrics can only be initialized once")
	}

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

var testPrometheusRacinessOnce sync.Once

func Test_Prometheus_Raciness(t *testing.T) {
	// As above, we can only run this test once.
	var doTest bool
	testPrometheusRacinessOnce.Do(func() {
		doTest = true
	})
	if !doTest {
		t.Skip("PromMetrics can only be initialized once")
	}

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

// histogramSnapshot returns the sample count, sum and cumulative bucket counts
// that the default registry exposes for the named histogram.
func histogramSnapshot(t *testing.T, name string) (count uint64, sum float64, buckets []uint64) {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		h := mf.GetMetric()[0].GetHistogram()
		for _, b := range h.GetBucket() {
			buckets = append(buckets, b.GetCumulativeCount())
		}
		return h.GetSampleCount(), h.GetSampleSum(), buckets
	}
	t.Fatalf("histogram %q is not registered", name)
	return
}

var testPrometheusHistogramBatchOnce sync.Once

func Test_Prometheus_HistogramBatch(t *testing.T) {
	// Register adds to the default registry, which panics on a duplicate name.
	var doTest bool
	testPrometheusHistogramBatchOnce.Do(func() {
		doTest = true
	})
	if !doTest {
		t.Skip("these Prometheus metrics can only be registered once")
	}

	p := &PromMetrics{metrics: make(map[string]interface{})}
	p.Register(Metadata{Name: "test_hist_batch", Type: Histogram})
	p.Register(Metadata{Name: "test_hist_each", Type: Histogram})
	p.Register(Metadata{Name: "test_hist_batch_counter", Type: Counter})

	// These land in different buckets (upper bounds are 1, 4, 16, ... 4^15),
	// with a repeated value and one above the last bucket.
	obs := []float64{0.5, 3, 3, 100, 70000, 5e10}
	for _, v := range obs {
		p.Histogram("test_hist_each", v)
	}
	p.HistogramBatch("test_hist_batch", obs)

	count, sum, buckets := histogramSnapshot(t, "test_hist_batch")
	assert.EqualValues(t, len(obs), count)
	assert.InDelta(t, 5e10+70106.5, sum, 1e-3)

	eachCount, eachSum, eachBuckets := histogramSnapshot(t, "test_hist_each")
	assert.Equal(t, eachCount, count)
	assert.Equal(t, eachSum, sum)
	assert.Equal(t, eachBuckets, buckets)
	// 0.5 is in the first bucket, and 5e10 is in none of them.
	assert.EqualValues(t, 1, buckets[0])
	assert.EqualValues(t, len(obs)-1, buckets[len(buckets)-1])

	// An empty batch records nothing, and an unknown name or a metric of
	// another type is ignored, as with Histogram.
	p.HistogramBatch("test_hist_batch", nil)
	p.HistogramBatch("test_hist_unknown", obs)
	p.HistogramBatch("test_hist_batch_counter", obs)
	count, _, _ = histogramSnapshot(t, "test_hist_batch")
	assert.EqualValues(t, len(obs), count)
}

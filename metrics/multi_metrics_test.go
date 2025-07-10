package metrics

import (
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/stretchr/testify/assert"
)

type dummyLogger struct{}

func (d dummyLogger) Debugf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
	fmt.Println()
}

func (d dummyLogger) Errorf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
	fmt.Println()
}

func getAndStartMultiMetrics(children ...Metrics) (*MultiMetrics, error) {
	mm := NewMultiMetrics()
	for _, child := range children {
		mm.AddChild(child)
	}
	objects := []*inject.Object{
		{Value: "version", Name: "version"},
		{Value: &http.Transport{}, Name: "upstreamTransport"},
		{Value: &http.Transport{}, Name: "peerTransport"},
		{Value: &LegacyMetrics{}, Name: "legacyMetrics"},
		{Value: &PromMetrics{}, Name: "promMetrics"},
		{Value: &OTelMetrics{}, Name: "otelMetrics"},
		{Value: mm, Name: "metrics"},
		{Value: &config.MockConfig{}},
		{Value: &logger.NullLogger{}},
	}
	g := inject.Graph{Logger: dummyLogger{}}
	err := g.Provide(objects...)
	if err != nil {
		return nil, err
	}

	if err := g.Populate(); err != nil {
		fmt.Printf("failed to populate injection graph. error: %+v\n", err)
		return nil, err
	}

	fmt.Println("starting injected dependencies")
	ststLogger := dummyLogger{}

	fmt.Println(g.Objects())

	defer startstop.Stop(g.Objects(), ststLogger)
	if err := startstop.Start(g.Objects(), ststLogger); err != nil {
		fmt.Printf("failed to start injected dependencies. error: %+v\n", err)
		os.Exit(1)
	}

	return mm, err
}

func TestMultiMetrics_Register(t *testing.T) {
	// This shows that a standalone metrics with no children can register and store values
	// that are important to StressRelief.
	mm, err := getAndStartMultiMetrics()
	assert.NoError(t, err)
	mm.Register(Metadata{
		Name: "updown",
		Type: UpDown,
	})
	mm.Register(Metadata{
		Name: "counter",
		Type: Counter,
	})
	mm.Register(Metadata{
		Name: "gauge",
		Type: Gauge,
	})

	mm.Count("counter", 1)
	mm.Up("updown")
	mm.Up("updown")
	mm.Up("updown")
	mm.Down("updown")
	mm.Gauge("gauge", 42)

	// counter should be 1 because it's tracked by StoreMetrics
	val, ok := mm.Get("counter")
	assert.True(t, ok)
	assert.Equal(t, 1, int(val))

	// updown should be 2 because it's tracked by StoreMetrics
	val, ok = mm.Get("updown")
	assert.True(t, ok)
	assert.Equal(t, 2, int(val))

	// gauge should be 42 because it's tracked by StoreMetrics
	val, ok = mm.Get("gauge")
	assert.True(t, ok)
	assert.Equal(t, 42, int(val))

	// non-existent metric should not be ok
	_, ok = mm.Get("non-existent")
	assert.False(t, ok)
}

func BenchmarkConcurrentAccess(b *testing.B) {
	legacyMetrics := &LegacyMetrics{
		Logger: &logger.NullLogger{},
		Config: &config.MockConfig{},
	}
	legacyMetrics.Start()
	defer legacyMetrics.Stop()
	promMetrics := &PromMetrics{
		Logger: &logger.NullLogger{},
		Config: &config.MockConfig{},
	}
	promMetrics.Start()
	otelMetrics := &OTelMetrics{
		Logger: &logger.NullLogger{},
		Config: &config.MockConfig{},
	}
	otelMetrics.Start()
	defer otelMetrics.Stop()

	mm, err := getAndStartMultiMetrics(
		legacyMetrics,
		promMetrics,
		otelMetrics,
	)
	if err != nil {
		b.Fatalf("Failed to setup MultiMetrics: %v", err)
	}

	mm.Register(Metadata{Name: "test_counter", Type: Counter})
	mm.Register(Metadata{Name: "test_gauge", Type: Gauge})
	mm.Register(Metadata{Name: "test_histogram", Type: Histogram})
	mm.Register(Metadata{Name: "test_updown", Type: UpDown})

	b.Run("ConcurrentCounters", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				mm.Count("test_counter", 1)
			}
		})
	})

	b.Run("ConcurrentGauges", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				mm.Gauge("test_gauge", float64(i))
				i++
			}
		})
	})

	b.Run("ConcurrentHistograms", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				mm.Histogram("test_histogram", float64(i))
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
					mm.Count("test_counter", 1)
				case 1:
					mm.Gauge("test_gauge", float64(i))
				case 2:
					mm.Histogram("test_histogram", float64(i))
				case 3:
					if i%2 == 0 {
						mm.Up("test_updown")
					} else {
						mm.Down("test_updown")
					}
				}
				i++
			}
		})
	})
}

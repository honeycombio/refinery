package metrics

import (
	"context"
	"net/url"
	"os"
	"runtime"
	rtmetrics "runtime/metrics"
	"sync"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

var _ MetricsBackend = (*OTelMetrics)(nil)

// OTelMetrics sends metrics to Honeycomb using the OpenTelemetry protocol. One
// particular thing to note is that OTel metrics treats histograms very
// differently than Honeycomb's Legacy metrics. In particular, Legacy metrics
// pre-aggregates histograms and sends columns corresponding to the histogram
// aggregates (e.g. avg, p50, p95, etc.). OTel, on the other hand, sends the raw
// histogram values and lets Honeycomb do the aggregation on ingest. The columns
// in the resulting datasets will not be the same.
//
// To minimize lock contention, this implementation uses sync.Map to reduce lock
// contention for concurrent access to metric instruments.
type OTelMetrics struct {
	Config  config.Config `inject:""`
	Logger  logger.Logger `inject:""`
	Version string        `inject:"version"`

	meter        metric.Meter
	shutdownFunc func(ctx context.Context) error
	testReader   sdkmetric.Reader

	counters         sync.Map // map[string]metric.Int64Counter
	gauges           sync.Map // map[string]metric.Float64Gauge
	histograms       sync.Map // map[string]metric.Float64Histogram
	updowns          sync.Map // map[string]metric.Int64UpDownCounter
	observableGauges sync.Map // map[string]metric.Float64ObservableGauge
}

// Start initializes all metrics or resets all metrics to zero
func (o *OTelMetrics) Start() error {
	cfg := o.Config.GetOTelMetricsConfig()

	// sync.Map doesn't require initialization

	ctx := context.Background()

	// otel can't handle the default endpoint so we have to parse it
	host, err := url.Parse(cfg.APIHost)
	if err != nil {
		o.Logger.Error().WithString("msg", "failed to parse metrics apihost").WithString("apihost", cfg.APIHost)
		return err
	}

	compression := otlpmetrichttp.GzipCompression
	if cfg.Compression == "none" {
		compression = otlpmetrichttp.NoCompression
	}

	options := []otlpmetrichttp.Option{
		otlpmetrichttp.WithEndpoint(host.Host),
		otlpmetrichttp.WithCompression(compression),
		// this is how we tell otel to reset metrics every time they're sent -- for some kinds of metrics.
		// Updown counters should not be reset, nor should gauges.
		// Histograms should definitely be reset.
		// Counters are a bit of a tossup, but we'll reset them because Legacy metrics did.
		otlpmetrichttp.WithTemporalitySelector(func(ik sdkmetric.InstrumentKind) metricdata.Temporality {
			switch ik {
			// These are the ones we care about today. If we add more, we'll need to add them here.
			case sdkmetric.InstrumentKindCounter, sdkmetric.InstrumentKindHistogram:
				return metricdata.DeltaTemporality
			default:
				return metricdata.CumulativeTemporality
			}
		}),
	}
	// Add custom headers from config first, then Honeycomb headers (which take precedence)
	hdrs := make(map[string]string)
	for k, v := range o.Config.GetAdditionalHeaders() {
		hdrs[k] = v
	}
	// Honeycomb headers override any custom headers
	if cfg.APIKey != "" {
		hdrs["x-honeycomb-team"] = cfg.APIKey
	}
	if cfg.Dataset != "" {
		hdrs["x-honeycomb-dataset"] = cfg.Dataset
	}
	if len(hdrs) > 0 {
		options = append(options, otlpmetrichttp.WithHeaders(hdrs))
	}

	if host.Scheme == "http" {
		options = append(options, otlpmetrichttp.WithInsecure())
	}

	exporter, err := otlpmetrichttp.New(ctx, options...)

	if err != nil {
		return err
	}

	// Fetch the hostname once and add it as an attribute to all metrics
	var hostname string
	if hn, err := os.Hostname(); err != nil {
		hostname = "unknown: " + err.Error()
	} else {
		hostname = hn
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(resource.Default().Attributes()...),
		resource.WithAttributes(attribute.KeyValue{Key: "service.name", Value: attribute.StringValue("refinery")}),
		resource.WithAttributes(attribute.KeyValue{Key: "service.version", Value: attribute.StringValue(o.Version)}),
		resource.WithAttributes(attribute.KeyValue{Key: "host.name", Value: attribute.StringValue(hostname)}),
		resource.WithAttributes(attribute.KeyValue{Key: "hostname", Value: attribute.StringValue(hostname)}),
	)

	if err != nil {
		return err
	}

	var reader sdkmetric.Reader
	if o.testReader != nil {
		reader = o.testReader
	} else {
		reader = sdkmetric.NewPeriodicReader(exporter,
			sdkmetric.WithInterval(time.Duration(cfg.ReportingInterval)),
		)
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)
	o.meter = provider.Meter("otelmetrics")
	o.shutdownFunc = provider.Shutdown

	// These metrics are dynamic fields that should always be collected
	name := "num_goroutines"
	var fgo metric.Float64Callback = func(_ context.Context, result metric.Float64Observer) error {
		result.Observe(float64(runtime.NumGoroutine()))
		return nil
	}
	g, err := o.meter.Float64ObservableGauge(name, metric.WithFloat64Callback(fgo))
	if err != nil {
		return err
	}

	o.observableGauges.Store(name, g)

	name = "memory_inuse"
	// This is just reporting the gauge we already track under a different name.
	var fmem metric.Float64Callback = func(_ context.Context, result metric.Float64Observer) error {
		memMetricSample := make([]rtmetrics.Sample, 1)
		memMetricSample[0].Name = RtMetricNameMemory
		rtmetrics.Read(memMetricSample)
		currentAlloc := memMetricSample[0].Value.Uint64()

		result.Observe(float64(currentAlloc))
		return nil
	}
	g, err = o.meter.Float64ObservableGauge(name, metric.WithFloat64Callback(fmem))
	if err != nil {
		return err
	}
	o.observableGauges.Store(name, g)

	startTime := time.Now()
	name = "process_uptime_seconds"
	var fup metric.Float64Callback = func(_ context.Context, result metric.Float64Observer) error {
		result.Observe(float64(time.Since(startTime) / time.Second))
		return nil
	}
	g, err = o.meter.Float64ObservableGauge(name, metric.WithFloat64Callback(fup))
	if err != nil {
		return err
	}
	o.observableGauges.Store(name, g)

	return nil
}

func (o *OTelMetrics) Stop() {
	if o.shutdownFunc != nil {
		o.shutdownFunc(context.Background())
	}
}

// Register creates a new metric with the given metadata
// and initialize it with zero value.
func (o *OTelMetrics) Register(metadata Metadata) {
	switch metadata.Type {
	case Counter:
		_, err := o.getOrInitCounter(metadata)
		if err != nil {
			o.Logger.Error().WithString("name", metadata.Name).Logf("failed to create counter. %s", err.Error())
			return
		}
	case Gauge:
		_, err := o.getOrInitGauge(metadata)
		if err != nil {
			o.Logger.Error().WithString("name", metadata.Name).Logf("failed to create gauge. %s", err.Error())
			return
		}
	case Histogram:
		_, err := o.getOrInitHistogram(metadata)
		if err != nil {
			o.Logger.Error().WithString("name", metadata.Name).Logf("failed to create histogram. %s", err.Error())
			return
		}
	case UpDown:
		_, err := o.getOrInitUpDown(metadata)
		if err != nil {
			o.Logger.Error().WithString("name", metadata.Name).Logf("failed to create updown counter. %s", err.Error())
			return
		}
	default:
		o.Logger.Error().WithString("type", metadata.Type.String()).Logf("unknown metric type")
		return
	}
}

func (o *OTelMetrics) Increment(name string) {
	ctr, err := o.getOrInitCounter(Metadata{
		Name: name,
	})

	if err != nil {
		return
	}

	ctr.Add(context.Background(), 1)
}

func (o *OTelMetrics) Gauge(name string, val float64) {
	g, err := o.getOrInitGauge(Metadata{
		Name: name,
	})
	if err != nil {
		return
	}
	g.Record(context.Background(), val)
}

func (o *OTelMetrics) Count(name string, val int64) {
	ctr, err := o.getOrInitCounter(Metadata{Name: name})
	if err != nil {
		return
	}
	ctr.Add(context.Background(), val)
}

func (o *OTelMetrics) Histogram(name string, val float64) {
	h, err := o.getOrInitHistogram(Metadata{Name: name})
	if err != nil {
		return
	}
	h.Record(context.Background(), val)
}

func (o *OTelMetrics) Up(name string) {
	ud, err := o.getOrInitUpDown(Metadata{Name: name})
	if err != nil {
		return
	}
	ud.Add(context.Background(), 1)
}

func (o *OTelMetrics) Down(name string) {
	ud, err := o.getOrInitUpDown(Metadata{Name: name})
	if err != nil {
		return
	}
	ud.Add(context.Background(), -1)
}

// getOrInitCounter returns a counter metric with the given metadata.
// Uses sync.Map for lock-free concurrent access.
func (o *OTelMetrics) getOrInitCounter(metadata Metadata) (metric.Int64Counter, error) {
	// Fast path: try to load existing counter
	if val, ok := o.counters.Load(metadata.Name); ok {
		return val.(metric.Int64Counter), nil
	}

	// Slow path: create new counter
	ctr, err := o.meter.Int64Counter(metadata.Name,
		metric.WithUnit(string(metadata.Unit)),
		metric.WithDescription(metadata.Description),
	)
	if err != nil {
		return nil, err
	}

	// Give the counter an initial value of 0 so that OTel will send it
	ctr.Add(context.Background(), 0)

	// LoadOrStore ensures only one counter is stored even if multiple goroutines
	// try to create the same counter concurrently
	actual, _ := o.counters.LoadOrStore(metadata.Name, ctr)
	return actual.(metric.Int64Counter), nil
}

// getOrInitGauge returns a gauge metric with the given metadata.
// Uses sync.Map for lock-free concurrent access.
func (o *OTelMetrics) getOrInitGauge(metadata Metadata) (metric.Float64Gauge, error) {
	// Fast path: try to load existing gauge
	if val, ok := o.gauges.Load(metadata.Name); ok {
		return val.(metric.Float64Gauge), nil
	}

	// Slow path: create new gauge
	g, err := o.meter.Float64Gauge(metadata.Name,
		metric.WithUnit(string(metadata.Unit)),
		metric.WithDescription(metadata.Description),
	)
	if err != nil {
		return nil, err
	}

	// LoadOrStore ensures only one gauge is stored even if multiple goroutines
	// try to create the same gauge concurrently
	actual, _ := o.gauges.LoadOrStore(metadata.Name, g)
	return actual.(metric.Float64Gauge), nil
}

// getOrInitHistogram initializes a new histogram metric with the given metadata.
// Uses sync.Map for lock-free concurrent access.
func (o *OTelMetrics) getOrInitHistogram(metadata Metadata) (metric.Float64Histogram, error) {
	// Fast path: try to load existing histogram
	if val, ok := o.histograms.Load(metadata.Name); ok {
		return val.(metric.Float64Histogram), nil
	}

	// Slow path: create new histogram
	h, err := o.meter.Float64Histogram(metadata.Name,
		metric.WithUnit(string(metadata.Unit)),
		metric.WithDescription(metadata.Description),
	)
	if err != nil {
		return nil, err
	}

	h.Record(context.Background(), 0)

	// LoadOrStore ensures only one histogram is stored even if multiple goroutines
	// try to create the same histogram concurrently
	actual, _ := o.histograms.LoadOrStore(metadata.Name, h)
	return actual.(metric.Float64Histogram), nil
}

// getOrInitUpDown initializes a new updown counter metric with the given metadata.
// Uses sync.Map for lock-free concurrent access.
func (o *OTelMetrics) getOrInitUpDown(metadata Metadata) (metric.Int64UpDownCounter, error) {
	// Fast path: try to load existing updown counter
	if val, ok := o.updowns.Load(metadata.Name); ok {
		return val.(metric.Int64UpDownCounter), nil
	}

	// Slow path: create new updown counter
	ud, err := o.meter.Int64UpDownCounter(metadata.Name,
		metric.WithUnit(string(metadata.Unit)),
		metric.WithDescription(metadata.Description),
	)
	if err != nil {
		return nil, err
	}

	ud.Add(context.Background(), 0)

	// LoadOrStore ensures only one updown counter is stored even if multiple goroutines
	// try to create the same updown counter concurrently
	actual, _ := o.updowns.LoadOrStore(metadata.Name, ud)
	return actual.(metric.Int64UpDownCounter), nil
}

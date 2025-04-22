package metrics

import (
	"context"
	"net/url"
	"os"
	"runtime"
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

var _ Metrics = (*OTelMetrics)(nil)

// OTelMetrics sends metrics to Honeycomb using the OpenTelemetry protocol. One
// particular thing to note is that OTel metrics treats histograms very
// differently than Honeycomb's Legacy metrics. In particular, Legacy metrics
// pre-aggregates histograms and sends columns corresponding to the histogram
// aggregates (e.g. avg, p50, p95, etc.). OTel, on the other hand, sends the raw
// histogram values and lets Honeycomb do the aggregation on ingest. The columns
// in the resulting datasets will not be the same.
type OTelMetrics struct {
	Config  config.Config `inject:""`
	Logger  logger.Logger `inject:""`
	Version string        `inject:"version"`

	meter        metric.Meter
	shutdownFunc func(ctx context.Context) error

	counters   map[string]metric.Int64Counter
	gauges     map[string]metric.Float64ObservableGauge
	histograms map[string]metric.Float64Histogram
	updowns    map[string]metric.Int64UpDownCounter

	// values keeps a map of all the non-histogram metrics and their current value
	// so that we can retrieve them with Get()
	values map[string]float64
	lock   sync.RWMutex
}

// Start initializes all metrics or resets all metrics to zero
func (o *OTelMetrics) Start() error {
	cfg := o.Config.GetOTelMetricsConfig()

	o.lock.Lock()
	defer o.lock.Unlock()

	o.counters = make(map[string]metric.Int64Counter)
	o.gauges = make(map[string]metric.Float64ObservableGauge)
	o.histograms = make(map[string]metric.Float64Histogram)
	o.updowns = make(map[string]metric.Int64UpDownCounter)

	o.values = make(map[string]float64)

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
	// if we ever need to add user-specified headers, that would go here
	hdrs := map[string]string{}
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

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(exporter,
				sdkmetric.WithInterval(time.Duration(cfg.ReportingInterval)),
			),
		),
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

	o.gauges[name] = g

	name = "memory_inuse"
	// This is just reporting the gauge we already track under a different name.
	var fmem metric.Float64Callback = func(_ context.Context, result metric.Float64Observer) error {
		// this is an 'ok' value, not an error, so it's safe to ignore it.
		v, _ := o.Get("memory_heap_allocation")
		result.Observe(v)
		return nil
	}
	g, err = o.meter.Float64ObservableGauge(name, metric.WithFloat64Callback(fmem))
	if err != nil {
		return err
	}
	o.gauges[name] = g

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
	o.gauges[name] = g

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
	o.lock.Lock()
	defer o.lock.Unlock()

	switch metadata.Type {
	case Counter:
		_, err := o.initCounter(metadata)
		if err != nil {
			o.Logger.Error().WithString("name", metadata.Name).Logf("failed to create counter. %s", err.Error())
			return
		}
	case Gauge:
		_, err := o.initGauge(metadata)
		if err != nil {
			o.Logger.Error().WithString("name", metadata.Name).Logf("failed to create gauge. %s", err.Error())
			return
		}
	case Histogram:
		_, err := o.initHistogram(metadata)
		if err != nil {
			o.Logger.Error().WithString("name", metadata.Name).Logf("failed to create histogram. %s", err.Error())
			return
		}
	case UpDown:
		_, err := o.initUpDown(metadata)
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
	o.lock.Lock()
	defer o.lock.Unlock()

	var err error
	ctr, ok := o.counters[name]
	if !ok {
		ctr, err = o.initCounter(Metadata{
			Name: name,
		})

		if err != nil {
			return
		}
	}
	ctr.Add(context.Background(), 1)
	o.values[name]++
}

func (o *OTelMetrics) Gauge(name string, val interface{}) {
	o.lock.Lock()
	defer o.lock.Unlock()

	if _, ok := o.gauges[name]; !ok {
		_, err := o.initGauge(Metadata{
			Name: name,
		})

		if err != nil {
			return
		}
	}
	o.values[name] = ConvertNumeric(val)
}

func (o *OTelMetrics) Count(name string, val interface{}) {
	o.lock.Lock()
	defer o.lock.Unlock()

	var err error
	ctr, ok := o.counters[name]
	if !ok {
		ctr, err = o.initCounter(Metadata{Name: name})
		if err != nil {
			return
		}
	}
	f := ConvertNumeric(val)
	ctr.Add(context.Background(), int64(f))
	o.values[name] += f
}

func (o *OTelMetrics) Histogram(name string, val interface{}) {
	o.lock.Lock()
	defer o.lock.Unlock()

	var err error
	h, ok := o.histograms[name]
	if !ok {
		h, err = o.initHistogram(Metadata{Name: name})
		if err != nil {
			return
		}
	}
	f := ConvertNumeric(val)
	h.Record(context.Background(), f)
	o.values[name] += f
}

func (o *OTelMetrics) Up(name string) {
	o.lock.Lock()
	defer o.lock.Unlock()

	var err error
	ud, ok := o.updowns[name]
	if !ok {
		ud, err = o.initUpDown(Metadata{Name: name})
		if err != nil {
			return
		}
	}
	ud.Add(context.Background(), 1)
	o.values[name]++
}

func (o *OTelMetrics) Down(name string) {
	o.lock.Lock()
	defer o.lock.Unlock()

	var err error
	ud, ok := o.updowns[name]
	if !ok {
		ud, err = o.initUpDown(Metadata{Name: name})
		if err != nil {
			return
		}
	}
	ud.Add(context.Background(), -1)
	o.values[name]--
}

func (o *OTelMetrics) Store(name string, value float64) {
	o.lock.Lock()
	defer o.lock.Unlock()

	o.values[name] = value
}

func (o *OTelMetrics) Get(name string) (float64, bool) {
	o.lock.RLock()
	defer o.lock.RUnlock()

	val, ok := o.values[name]
	return val, ok
}

// initCounter initializes a new counter metric with the given metadata
// It should be used while holding the metrics lock.
func (o *OTelMetrics) initCounter(metadata Metadata) (metric.Int64Counter, error) {
	unit := string(metadata.Unit)
	ctr, err := o.meter.Int64Counter(metadata.Name,
		metric.WithUnit(unit),
		metric.WithDescription(metadata.Description),
	)
	if err != nil {
		return nil, err
	}

	// Give the counter an initial value of 0 so that OTel will send it
	ctr.Add(context.Background(), 0)
	o.counters[metadata.Name] = ctr

	return ctr, nil
}

// initGauge initializes a new gauge metric with the given metadata
// It should be used while holding the metrics lock.
func (o *OTelMetrics) initGauge(metadata Metadata) (metric.Float64ObservableGauge, error) {
	unit := string(metadata.Unit)
	var f metric.Float64Callback = func(_ context.Context, result metric.Float64Observer) error {
		// this callback is invoked from outside this function call, so we
		// need to Rlock when we read the values map. We don't know how long
		// Observe() takes, so we make a copy of the value and unlock before
		// calling Observe.
		o.lock.RLock()
		v := o.values[metadata.Name]
		o.lock.RUnlock()

		result.Observe(v)
		return nil
	}
	g, err := o.meter.Float64ObservableGauge(metadata.Name,
		metric.WithUnit(unit),
		metric.WithDescription(metadata.Description),
		metric.WithFloat64Callback(f),
	)
	if err != nil {
		return nil, err
	}

	o.gauges[metadata.Name] = g
	return g, nil
}

// initHistogram initializes a new histogram metric with the given metadata
// It should be used while holding the metrics lock.
func (o *OTelMetrics) initHistogram(metadata Metadata) (metric.Float64Histogram, error) {
	unit := string(metadata.Unit)
	h, err := o.meter.Float64Histogram(metadata.Name,
		metric.WithUnit(unit),
		metric.WithDescription(metadata.Description),
	)
	if err != nil {
		return nil, err
	}
	h.Record(context.Background(), 0)
	o.histograms[metadata.Name] = h

	return h, nil

}

// initUpDown initializes a new updown counter metric with the given metadata
// It should be used while holding the metrics lock.
func (o *OTelMetrics) initUpDown(metadata Metadata) (metric.Int64UpDownCounter, error) {
	unit := string(metadata.Unit)
	ud, err := o.meter.Int64UpDownCounter(metadata.Name,
		metric.WithUnit(unit),
		metric.WithDescription(metadata.Description),
	)
	if err != nil {
		return nil, err
	}
	ud.Add(context.Background(), 0)
	o.updowns[metadata.Name] = ud

	return ud, nil

}

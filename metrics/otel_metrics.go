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

	meter metric.Meter

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

func (o *OTelMetrics) Register(name string, metricType string) {
	o.lock.Lock()
	defer o.lock.Unlock()

	switch metricType {
	case "counter":
		ctr, err := o.meter.Int64Counter(name)
		if err != nil {
			o.Logger.Error().WithString("msg", "failed to create counter").WithString("name", name)
			return
		}
		o.counters[name] = ctr
	case "gauge":
		var f metric.Float64Callback = func(_ context.Context, result metric.Float64Observer) error {
			// this callback is invoked from outside this function call, so we
			// need to Rlock when we read the values map. We don't know how long
			// Observe() takes, so we make a copy of the value and unlock before
			// calling Observe.
			o.lock.RLock()
			v := o.values[name]
			o.lock.RUnlock()

			result.Observe(v)
			return nil
		}
		g, err := o.meter.Float64ObservableGauge(name,
			metric.WithFloat64Callback(f),
		)
		if err != nil {
			o.Logger.Error().WithString("msg", "failed to create gauge").WithString("name", name)
			return
		}
		o.gauges[name] = g
	case "histogram":
		h, err := o.meter.Float64Histogram(name)
		if err != nil {
			o.Logger.Error().WithString("msg", "failed to create histogram").WithString("name", name)
			return
		}
		o.histograms[name] = h
	case "updown":
		ud, err := o.meter.Int64UpDownCounter(name)
		if err != nil {
			o.Logger.Error().WithString("msg", "failed to create updown").WithString("name", name)
			return
		}
		o.updowns[name] = ud
	default:
		o.Logger.Error().WithString("msg", "unknown metric type").WithString("type", metricType)
		return
	}
}

func (o *OTelMetrics) Increment(name string) {
	o.lock.Lock()
	defer o.lock.Unlock()

	if ctr, ok := o.counters[name]; ok {
		ctr.Add(context.Background(), 1)
		o.values[name]++
	}
}

func (o *OTelMetrics) Gauge(name string, val interface{}) {
	o.lock.Lock()
	defer o.lock.Unlock()

	o.values[name] = ConvertNumeric(val)
}

func (o *OTelMetrics) Count(name string, val interface{}) {
	o.lock.Lock()
	defer o.lock.Unlock()

	if ctr, ok := o.counters[name]; ok {
		f := ConvertNumeric(val)
		ctr.Add(context.Background(), int64(f))
		o.values[name] += f
	}
}

func (o *OTelMetrics) Histogram(name string, val interface{}) {
	o.lock.Lock()
	defer o.lock.Unlock()

	if h, ok := o.histograms[name]; ok {
		f := ConvertNumeric(val)
		h.Record(context.Background(), f)
		o.values[name] += f
	}
}

func (o *OTelMetrics) Up(name string) {
	o.lock.Lock()
	defer o.lock.Unlock()

	if ud, ok := o.updowns[name]; ok {
		ud.Add(context.Background(), 1)
		o.values[name]++
	}
}

func (o *OTelMetrics) Down(name string) {
	o.lock.Lock()
	defer o.lock.Unlock()

	if ud, ok := o.updowns[name]; ok {
		ud.Add(context.Background(), -1)
		o.values[name]--
	}
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

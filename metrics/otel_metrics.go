package metrics

import (
	"context"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

// OTelMetrics discards all metrics
type OTelMetrics struct {
	Config  config.Config `inject:""`
	Logger  logger.Logger `inject:""`
	Version string        `inject:"version"`

	meter metric.Meter

	counters   map[string]metric.Int64Counter
	gauges     map[string]metric.Float64ObservableGauge
	histograms map[string]metric.Int64Histogram
	updowns    map[string]metric.Int64UpDownCounter

	// values keeps a map of all the non-histogram metrics and their current value
	// so that we can retrieve them with Get()
	values map[string]float64
	lock   sync.RWMutex
}

// Start initializes all metrics or resets all metrics to zero
func (o *OTelMetrics) Start() {
	cfg := o.Config.GetOTelMetricsConfig()

	ctx := context.Background()

	// otel can't handle the default endpoint so we have to parse it
	host, err := url.Parse(cfg.APIHost)
	if err != nil {
		o.Logger.Error().WithString("msg", "failed to parse metrics apihost").WithString("apihost", cfg.APIHost)
		return
	}

	compression := otlpmetrichttp.GzipCompression
	if cfg.Compression == "none" {
		compression = otlpmetrichttp.NoCompression
	}

	options := []otlpmetrichttp.Option{
		otlpmetrichttp.WithEndpoint(host.Host),
		otlpmetrichttp.WithHeaders(map[string]string{
			"x-honeycomb-team":    cfg.APIKey,
			"x-honeycomb-dataset": cfg.Dataset,
		}),
		otlpmetrichttp.WithCompression(compression),
	}
	if host.Scheme == "http" {
		options = append(options, otlpmetrichttp.WithInsecure())
	}

	exporter, err := otlpmetrichttp.New(ctx, options...)

	if err != nil {
		log.Fatal(err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(resource.Default().Attributes()...),
		resource.WithAttributes(attribute.KeyValue{Key: "service.name", Value: attribute.StringValue("refinery")}),
		resource.WithAttributes(attribute.KeyValue{Key: "service.version", Value: attribute.StringValue(o.Version)}),
	)

	if err != nil {
		log.Fatal(err)
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
}

func (o *OTelMetrics) Register(name string, metricType string) {
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
			o.lock.RLock()
			defer o.lock.RUnlock()
			result.Observe(o.values[name])
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
		h, err := o.meter.Int64Histogram(name)
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

	o.values[name] = val.(float64)
}

func (o *OTelMetrics) Count(name string, val interface{}) {
	o.lock.Lock()
	defer o.lock.Unlock()

	if ctr, ok := o.counters[name]; ok {
		ctr.Add(context.Background(), val.(int64))
		o.values[name] += float64(val.(int64))
	}
}

func (o *OTelMetrics) Histogram(name string, obs interface{}) {
	o.lock.Lock()
	defer o.lock.Unlock()

	if h, ok := o.histograms[name]; ok {
		h.Record(context.Background(), obs.(int64))
		o.values[name] += float64(obs.(int64))
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

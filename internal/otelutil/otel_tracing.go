package otelutil

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/url"
	"strings"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	samplers "go.opentelemetry.io/otel/sdk/trace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// telemetry helpers

func AddException(span trace.Span, err error) {
	span.AddEvent("exception", trace.WithAttributes(
		attribute.KeyValue{Key: "exception.type", Value: attribute.StringValue("error")},
		attribute.KeyValue{Key: "exception.message", Value: attribute.StringValue(err.Error())},
		attribute.KeyValue{Key: "exception.stacktrace", Value: attribute.StringValue("stacktrace")},
		attribute.KeyValue{Key: "exception.escaped", Value: attribute.BoolValue(false)},
	))
}

// addSpanField adds a field to a span, using the appropriate method for the type of the value.
func AddSpanField(span trace.Span, key string, value interface{}) {
	span.SetAttributes(Attributes(map[string]interface{}{key: value})...)
}

// AddSpanFields adds multiple fields to a span, using the appropriate method for the type of each value.
func AddSpanFields(span trace.Span, fields map[string]interface{}) {
	span.SetAttributes(Attributes(fields)...)
}

// Attributes converts a map of fields to a slice of attribute.KeyValue, setting types appropriately.
func Attributes(fields map[string]interface{}) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(fields))
	for k, v := range fields {
		kv := attribute.KeyValue{Key: attribute.Key(k)}
		switch val := v.(type) {
		case string:
			kv.Value = attribute.StringValue(val)
		case int:
			kv.Value = attribute.IntValue(val)
		case int64:
			kv.Value = attribute.Int64Value(val)
		case float64:
			kv.Value = attribute.Float64Value(val)
		case bool:
			kv.Value = attribute.BoolValue(val)
		default:
			kv.Value = attribute.StringValue(fmt.Sprintf("%v", val))
		}
		attrs = append(attrs, kv)
	}
	return attrs
}

// Starts a span with no extra fields.
func StartSpan(ctx context.Context, tracer trace.Tracer, name string) (context.Context, trace.Span) {
	return tracer.Start(ctx, name)
}

// Starts a span with a single field.
func StartSpanWith(ctx context.Context, tracer trace.Tracer, name string, field string, value interface{}) (context.Context, trace.Span) {
	return tracer.Start(ctx, name, trace.WithAttributes(Attributes(map[string]interface{}{field: value})...))
}

// Starts a span with multiple fields.
func StartSpanMulti(ctx context.Context, tracer trace.Tracer, name string, fields map[string]interface{}) (context.Context, trace.Span) {
	return tracer.Start(ctx, name, trace.WithAttributes(Attributes(fields)...))
}

func SetupTracing(cfg config.OTelTracingConfig, resourceLibrary string, resourceVersion string) (tracer trace.Tracer, shutdown func()) {
	if !cfg.Enabled {
		pr := noop.NewTracerProvider()
		return pr.Tracer(resourceLibrary, trace.WithInstrumentationVersion(resourceVersion)), func() {}
	}

	cfg.APIHost = strings.TrimSuffix(cfg.APIHost, "/")
	apihost, err := url.Parse(fmt.Sprintf("%s:443", cfg.APIHost))
	if err != nil {
		log.Fatalf("failed to parse otel API host: %v", err)
	}

	sampleRate := cfg.SampleRate
	if sampleRate < 1 {
		sampleRate = 1
	}

	var sampleRatio float64 = 1.0 / float64(sampleRate)

	// set up honeycomb specific headers if an API key is provided
	headers := make(map[string]string)
	if cfg.APIKey != "" {
		headers = map[string]string{
			types.APIKeyHeader: cfg.APIKey,
		}

		if types.IsLegacyAPIKey(cfg.APIKey) {
			headers[types.DatasetHeader] = cfg.Dataset
		}
	}

	tlsconfig := &tls.Config{}
	secureOption := otlptracehttp.WithTLSClientConfig(tlsconfig)
	exporter, err := otlptrace.New(
		context.Background(),
		otlptracehttp.NewClient(
			secureOption,
			otlptracehttp.WithEndpoint(apihost.Host),
			otlptracehttp.WithHeaders(headers),
			otlptracehttp.WithCompression(otlptracehttp.GzipCompression),
		),
	)
	if err != nil {
		log.Fatalf("failure configuring otel trace exporter: %v", err)
	}

	bsp := sdktrace.NewBatchSpanProcessor(exporter)
	otel.SetTracerProvider(sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(bsp),
		sdktrace.WithSampler(samplers.TraceIDRatioBased(sampleRatio)),
		sdktrace.WithResource(resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceNameKey.String(cfg.Dataset))),
	))

	return otel.Tracer(resourceLibrary, trace.WithInstrumentationVersion(resourceVersion)), func() {
		bsp.Shutdown(context.Background())
		exporter.Shutdown(context.Background())
	}
}

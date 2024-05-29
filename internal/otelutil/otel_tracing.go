package otelutil

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/honeycombio/otel-config-go/otelconfig"
	"github.com/honeycombio/refinery/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	samplers "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

var classicKeyRegex = regexp.MustCompile(`^[a-f0-9]*$`)
var classicIngestKeyRegex = regexp.MustCompile(`^hc[a-z]ic_[a-z0-9]*$`)

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
	if cfg.APIKey != "" {
		var protocol otelconfig.Protocol = otelconfig.ProtocolHTTPProto

		cfg.APIHost = strings.TrimSuffix(cfg.APIHost, "/")
		apihost := fmt.Sprintf("%s:443", cfg.APIHost)

		sampleRate := cfg.SampleRate
		if sampleRate < 1 {
			sampleRate = 1
		}

		var sampleRatio float64 = 1.0 / float64(sampleRate)

		headers := map[string]string{
			"x-honeycomb-team": cfg.APIKey,
		}

		if isClassicKey(cfg.APIKey) {
			headers["x-honeycomb-dataset"] = cfg.Dataset
		}

		otelshutdown, err := otelconfig.ConfigureOpenTelemetry(
			otelconfig.WithExporterProtocol(protocol),
			otelconfig.WithServiceName(cfg.Dataset),
			otelconfig.WithTracesExporterEndpoint(apihost),
			otelconfig.WithMetricsEnabled(false),
			otelconfig.WithTracesEnabled(true),
			otelconfig.WithSampler(samplers.TraceIDRatioBased(sampleRatio)),
			otelconfig.WithHeaders(headers),
		)
		if err != nil {
			log.Fatalf("failure configuring otel: %v", err)
		}
		return otel.Tracer(resourceLibrary, trace.WithInstrumentationVersion(resourceVersion)), otelshutdown
	}
	pr := noop.NewTracerProvider()
	return pr.Tracer(resourceLibrary, trace.WithInstrumentationVersion(resourceVersion)), func() {}
}

func isClassicKey(key string) bool {
	if len(key) == 32 {
		return classicKeyRegex.MatchString(key)
	} else if len(key) == 64 {
		return classicIngestKeyRegex.MatchString(key)
	}
	return false
}

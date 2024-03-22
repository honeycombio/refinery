package otelutil

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/honeycombio/otel-config-go/otelconfig"
	"github.com/honeycombio/refinery/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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
	k := attribute.Key(key)
	switch v := value.(type) {
	case string:
		span.SetAttributes(attribute.KeyValue{Key: k, Value: attribute.StringValue(v)})
	case int:
		span.SetAttributes(attribute.KeyValue{Key: k, Value: attribute.IntValue(v)})
	case bool:
		span.SetAttributes(attribute.KeyValue{Key: k, Value: attribute.BoolValue(v)})
	case int64:
		span.SetAttributes(attribute.KeyValue{Key: k, Value: attribute.Int64Value(v)})
	case float64:
		span.SetAttributes(attribute.KeyValue{Key: k, Value: attribute.Float64Value(v)})
	case time.Duration:
		span.SetAttributes(attribute.KeyValue{Key: k, Value: attribute.Int64Value(int64(v))})
	default:
		// fallback if we don't have anything better, render it to a string
		span.SetAttributes(attribute.KeyValue{Key: k, Value: attribute.StringValue(fmt.Sprintf("%v", value))})
	}
}

func AddSpanFields(span trace.Span, fields map[string]interface{}) {
	for k, v := range fields {
		AddSpanField(span, k, v)
	}
}

func SetupTracing(cfg config.OTelTracingConfig, resourceLibrary string, resourceVersion string) (tracer trace.Tracer, shutdown func()) {
	if cfg.APIKey != "" {
		var protocol otelconfig.Protocol = otelconfig.ProtocolHTTPProto

		cfg.APIHost = strings.TrimSuffix(cfg.APIHost, "/")
		apihost := fmt.Sprintf("%s:443", cfg.APIHost)

		otelshutdown, err := otelconfig.ConfigureOpenTelemetry(
			otelconfig.WithExporterProtocol(protocol),
			otelconfig.WithServiceName(cfg.Dataset),
			otelconfig.WithTracesExporterEndpoint(apihost),
			otelconfig.WithMetricsEnabled(false),
			otelconfig.WithTracesEnabled(true),
			otelconfig.WithHeaders(map[string]string{
				"x-honeycomb-team": cfg.APIKey,
			}),
		)
		if err != nil {
			log.Fatalf("failure configuring otel: %v", err)
		}
		return otel.Tracer(resourceLibrary, trace.WithInstrumentationVersion(resourceVersion)), otelshutdown
	}
	pr := noop.NewTracerProvider()
	return pr.Tracer(resourceLibrary, trace.WithInstrumentationVersion(resourceVersion)), func() {}
}

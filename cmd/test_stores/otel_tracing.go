package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/honeycombio/otel-config-go/otelconfig"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// This file contains a few helper functions for setting up tracing in the test_stores binary;
// they're a lot less wordy than using OTel explicitly.

var ResourceLibrary = "test_stores"
var ResourceVersion = "dev"

// telemetry helpers

func addException(span trace.Span, err error) {
	span.AddEvent("exception", trace.WithAttributes(
		attribute.KeyValue{Key: "exception.type", Value: attribute.StringValue("error")},
		attribute.KeyValue{Key: "exception.message", Value: attribute.StringValue(err.Error())},
		attribute.KeyValue{Key: "exception.stacktrace", Value: attribute.StringValue("stacktrace")},
		attribute.KeyValue{Key: "exception.escaped", Value: attribute.BoolValue(false)},
	))
}

// addSpanField adds a field to a span, using the appropriate method for the type of the value.
func addSpanField(span trace.Span, key string, value interface{}) {
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

func addSpanFields(span trace.Span, fields map[string]interface{}) {
	for k, v := range fields {
		addSpanField(span, k, v)
	}
}

func setupTracing(opts CmdLineOptions) (tracer trace.Tracer, shutdown func()) {
	if opts.HnyAPIKey != "" {
		var protocol otelconfig.Protocol = otelconfig.ProtocolHTTPProto

		opts.HnyEndpoint = strings.TrimSuffix(opts.HnyEndpoint, "/")
		endpoint := fmt.Sprintf("%s:443", opts.HnyEndpoint)

		otelshutdown, err := otelconfig.ConfigureOpenTelemetry(
			otelconfig.WithExporterProtocol(protocol),
			otelconfig.WithServiceName(opts.HnyDataset),
			otelconfig.WithTracesExporterEndpoint(endpoint),
			otelconfig.WithMetricsEnabled(false),
			otelconfig.WithTracesEnabled(true),
			otelconfig.WithHeaders(map[string]string{
				"x-honeycomb-team": opts.HnyAPIKey,
			}),
		)
		if err != nil {
			log.Fatalf("failure configuring otel: %v", err)
		}
		return otel.Tracer(ResourceLibrary, trace.WithInstrumentationVersion(ResourceVersion)), otelshutdown
	}
	pr := noop.NewTracerProvider()
	return pr.Tracer(ResourceLibrary, trace.WithInstrumentationVersion(ResourceVersion)), func() {}
}

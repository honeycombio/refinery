package collect

// collector_instrumentation.go
// Contains instrumentation methods for the InMemCollector

import (
	"context"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/internal/otelutil"
	"github.com/honeycombio/refinery/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// CollectorInstrumentation contains helpers for tracing collector operations
type CollectorInstrumentation struct {
	tracer trace.Tracer
}

// NewCollectorInstrumentation creates a new instrumentation helper
func NewCollectorInstrumentation(tracer trace.Tracer) *CollectorInstrumentation {
	return &CollectorInstrumentation{
		tracer: tracer,
	}
}

// AddTraceInstrumentation adds trace metadata to the current span
func (ci *CollectorInstrumentation) AddTraceInstrumentation(span trace.Span, tr *types.Trace) {
	if span == nil {
		return
	}

	otelutil.AddSpanFields(span, map[string]interface{}{
		"trace_id":               tr.TraceID,
		"trace.span_count":       tr.SpanCount(),
		"trace.span_event_count": tr.SpanEventCount(),
		"trace.span_link_count":  tr.SpanLinkCount(),
		"trace.dataset":          tr.Dataset,
		"trace.api_key":          tr.APIKey,
	})
}

// AddSpanMetadata adds metadata fields to a span's Data map
func (ci *CollectorInstrumentation) AddSpanMetadata(span *types.Span, tr cache.TraceSentRecord, reason string) {
	if reason != "" {
		span.Data["meta.refinery.kept_reason"] = reason
	}
	span.Data["meta.refinery.sample_rate"] = tr.Rate()
	span.Data["meta.refinery.kept"] = tr.Kept()
}

// StartSpanWithSample starts a new instrumented span for sampling operations
func (ci *CollectorInstrumentation) StartSpanWithSample(ctx context.Context, name string, tr cache.TraceSentRecord, reason string) (context.Context, trace.Span) {
	ctx, span := otelutil.StartSpanMulti(ctx, ci.tracer, name, map[string]interface{}{
		"sample.keep":   tr.Kept(),
		"sample.rate":   tr.Rate(),
		"sample.reason": reason,
	})
	return ctx, span
}

func (i *InMemCollector) addInstrumentation(ctx context.Context, tr cache.TraceSentRecord, keptReason string, sp *types.Span) {
	ctx, span := otelutil.StartSpanMulti(ctx, i.Tracer, "collector.dealWithSentTrace", map[string]interface{}{
		"trace_id":           sp.TraceID,
		"sample.keep":        tr.Kept(),
		"sample.rate":        tr.Rate(),
		"sample.kept_reason": keptReason,
	})
	defer span.End()

	if tr.Kept() {
		if i.Config.GetAddHostMetadataToTrace() {
			sp.AddField("meta.refinery.local_hostname", i.hostname)
		}
		sp.SampleRate = tr.Rate()
		err := i.Transmission.EnqueueSpan(sp)
		if err != nil {
			span.SetAttributes(
				attribute.Bool("error", true),
				attribute.String("error.message", err.Error()),
			)
			i.Logger.Error().WithField("error", err).Logf("failed to enqueue late span")
		}
		i.Metrics.Increment("trace_send_late_span")
	}
}

func (i *InMemCollector) addSamplingInstrumentation(ctx context.Context, trace *types.Trace, rate uint, reason string) {
	if span := otelutil.SpanFromContext(ctx); span != nil {
		span.SetAttributes(
			attribute.String("sampler.key", trace.Dataset),
			attribute.Int("sample.rate", int(rate)),
			attribute.String("sample.reason", reason),
		)
	}
}

func addSpanInstrumentation(ctx context.Context, sp *types.Span, tr *types.Trace) {
	if span := trace.SpanFromContext(ctx); span != nil {
		otelutil.AddSpanFields(span, map[string]interface{}{
			"trace_id":         sp.TraceID,
			"span.dataset":     sp.Dataset,
			"span.data_size":   sp.DataSize,
			"trace.span_count": tr.SpanCount(),
		})
	}
}

// addSpanMetadata adds metadata fields to a span's Data map
func addSpanMetadata(sp *types.Span, hostname string, tr cache.TraceSentRecord) {
	sp.Data["meta.refinery.local_hostname"] = hostname
	sp.Data["meta.refinery.sample_rate"] = tr.Rate()
	if reason := tr.GetKeptReason(); reason != "" {
		sp.Data["meta.refinery.kept_reason"] = reason
	}
}

// addTraceInstrumentation adds trace-level attributes to the span
func addTraceInstrumentation(span trace.Span, tr *types.Trace) {
	if span == nil {
		return
	}

	otelutil.AddSpanFields(span, map[string]interface{}{
		"trace_id":          tr.TraceID,
		"trace.span_count":  tr.SpanCount(),
		"trace.event_count": tr.SpanEventCount(),
		"trace.link_count":  tr.SpanLinkCount(),
		"trace.dataset":     tr.Dataset,
	})
}

// addSampleInstrumentation adds sampling-related attributes to the span
func addSampleInstrumentation(span trace.Span, tr cache.TraceSentRecord, keptReason string) {
	if span == nil {
		return
	}

	otelutil.AddSpanFields(span, map[string]interface{}{
		"sample.keep":   tr.Kept(),
		"sample.rate":   tr.Rate(),
		"sample.reason": keptReason,
	})
}

func addStressInstrumentation(span trace.Span, stressLevel uint, reason string) {
	otelutil.AddSpanFields(span, map[string]interface{}{
		"stress.level":  stressLevel,
		"stress.reason": reason,
	})
}

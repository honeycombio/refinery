package refinerytrace

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type Tracer interface {
	Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span)
}

type RefineryTracer struct {
	t trace.Tracer
}

// ensure RefineryTracer implements Tracer
var _ Tracer = (*RefineryTracer)(nil)

func NewTracer(t trace.Tracer) *RefineryTracer {
	if t == nil {
		t = noop.Tracer{}
	}
	return &RefineryTracer{t: t}
}

func (r *RefineryTracer) Start(ctx context.Context, _ string, _ ...trace.SpanStartOption) (context.Context, trace.Span) {
	return r.t.Start(ctx, "", trace.WithSpanKind(trace.SpanKindServer))
}

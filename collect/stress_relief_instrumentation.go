package collect

import (
	"context"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/otelutil"
	"github.com/honeycombio/refinery/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// StressReliefInstrumentation contains helpers for tracing stress relief operations
type StressReliefInstrumentation struct {
	metrics metrics.Metrics
	config  config.StressReliefConfig
}

// NewStressReliefInstrumentation creates a new instrumentation helper
func NewStressReliefInstrumentation(m metrics.Metrics, cfg config.StressReliefConfig) *StressReliefInstrumentation {
	return &StressReliefInstrumentation{
		metrics: m,
		config:  cfg,
	}
}

// addStressLevelInstrumentation adds stress level information to span and metrics
func (sri *StressReliefInstrumentation) addStressLevelInstrumentation(span trace.Span, level uint) {
	if span == nil {
		return
	}

	otelutil.AddSpanFields(span, map[string]interface{}{
		"stress.level":     level,
		"stress.type":      "cluster",
		"stress.max_level": sri.config.MaxLevel,
	})

	if sri.metrics != nil {
		sri.metrics.Gauge("cluster_stress_level", float64(level))
	}
}

// addStressDecisionInstrumentation adds sampling decision information to span
func (sri *StressReliefInstrumentation) addStressDecisionInstrumentation(span trace.Span, traceID string, keep bool, rate uint, reason string) {
	if span == nil {
		return
	}

	otelutil.AddSpanFields(span, map[string]interface{}{
		"trace_id":      traceID,
		"sample.keep":   keep,
		"sample.rate":   rate,
		"sample.reason": reason,
		"sample.source": "stress_relief",
	})
}

func (s *StressRelief) calculateClusterStressWithInstrumentation(ctx context.Context) uint {
	var span trace.Span
	if s.Tracer != nil {
		_, span = otelutil.StartSpanWith(ctx, s.Tracer, "stressRelief.calculateClusterStress", "stress.type", "cluster")
		defer span.End()
	}

	now := s.Clock.Now()
	expirationTime := now.Add(-s.Config.StressReliefReportExpiration)
	var totalStressLevel uint
	var validReports uint

	for _, report := range s.stressLevels {
		if report.timestamp.After(expirationTime) {
			totalStressLevel += report.level
			validReports++
		}
	}

	var clusterStress uint
	if validReports > 0 {
		clusterStress = totalStressLevel / validReports
	}

	if span != nil {
		span.SetAttributes(
			attribute.Int("stress.valid_reports", int(validReports)),
			attribute.Int("stress.total_level", int(totalStressLevel)),
			attribute.Int("stress.cluster_level", int(clusterStress)),
		)
	}

	s.Metrics.Gauge("cluster_stress_level", float64(clusterStress))
	return clusterStress
}

func (s *StressRelief) addSamplingDecisionInstrumentation(traceID string, keep bool, stressLevel uint, span trace.Span) {
	if span == nil {
		return
	}

	span.SetAttributes(
		attribute.String("trace_id", traceID),
		attribute.Bool("sample.keep", keep),
		attribute.Int("stress.level", int(stressLevel)),
	)
}

// updateStressLevel adds stress level instrumentation to a span
func (s *StressRelief) updateStressLevel(ctx context.Context, level uint) {
	if span := trace.SpanFromContext(ctx); span != nil {
		otelutil.AddSpanFields(span, map[string]interface{}{
			"stress.level":     level,
			"stress.timestamp": s.Clock.Now().String(),
		})
	}

	s.Metrics.Gauge("cluster_stress_level", float64(level))
	s.Logger.Debug().WithField("stress_level", level).Logf("updated stress level")
}

// addStressDecisionInstrumentation adds sampling decision instrumentation
func (s *StressRelief) addStressDecisionInstrumentation(span trace.Span, traceID string, keep bool, rate uint, reason string) {
	if span == nil {
		return
	}

	otelutil.AddSpanFields(span, map[string]interface{}{
		"trace_id":      traceID,
		"sample.keep":   keep,
		"sample.rate":   rate,
		"sample.reason": reason,
	})
}

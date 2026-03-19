package agent

import (
	"fmt"
	"math"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type metricMapping struct {
	metricName string
	signal     string
}

var signalToMetric = map[usageSignal]metricMapping{
	signal_traces:                 {metricName: "bytes_received", signal: "traces"},
	signal_logs:                   {metricName: "bytes_received", signal: "logs"},
	signal_events_received_traces: {metricName: "events_received", signal: "traces"},
	signal_events_received_logs:   {metricName: "events_received", signal: "logs"},
	signal_events_dropped_traces:  {metricName: "events_dropped", signal: "traces"},
	signal_events_dropped_logs:    {metricName: "events_dropped", signal: "logs"},
}

type otlpMetrics struct {
	metrics pmetric.Metrics
	sums    map[string]pmetric.Sum
	sm      pmetric.ScopeMetrics
}

func newOTLPMetrics(serviceName, version, hostname string) *otlpMetrics {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	resourceAttrs := rm.Resource().Attributes()
	resourceAttrs.PutStr("service.name", serviceName)
	resourceAttrs.PutStr("service.version", version)
	resourceAttrs.PutStr("host.name", hostname)
	sm := rm.ScopeMetrics().AppendEmpty()
	return &otlpMetrics{
		metrics: metrics,
		sums:    make(map[string]pmetric.Sum),
		sm:      sm,
	}
}

func (om *otlpMetrics) getOrCreateSum(metricName string) pmetric.Sum {
	if sum, ok := om.sums[metricName]; ok {
		return sum
	}
	ms := om.sm.Metrics().AppendEmpty()
	ms.SetName(metricName)
	sum := ms.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	om.sums[metricName] = sum
	return sum
}

func (om *otlpMetrics) addOTLPSum(timestamp time.Time, value float64, signal usageSignal) error {
	mapping, ok := signalToMetric[signal]
	if !ok {
		return fmt.Errorf("unknown usage signal: %s", signal)
	}

	intVal, err := convertFloat64ToInt64(value)
	if err != nil {
		return err
	}
	sum := om.getOrCreateSum(mapping.metricName)
	d := sum.DataPoints().AppendEmpty()
	d.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	d.SetIntValue(intVal)
	d.Attributes().PutStr("signal", mapping.signal)
	return nil
}

func convertFloat64ToInt64(value float64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("value %f is too large to convert to int64", value)
	}
	if value < 0 {
		return 0, fmt.Errorf("invalid negative value %f", value)
	}
	return int64(value), nil
}

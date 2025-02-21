package agent

import (
	"fmt"
	"math"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type otlpMetrics struct {
	metrics pmetric.Metrics
	ms      pmetric.Sum
}

func newOTLPMetrics() *otlpMetrics {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	resourceAttrs := rm.Resource().Attributes()
	resourceAttrs.PutStr("service.name", "-service")
	resourceAttrs.PutStr("service.version", "1.0.0")
	resourceAttrs.PutStr("host.name", "example-host")
	sm := rm.ScopeMetrics().AppendEmpty()
	ms := sm.Metrics().AppendEmpty()
	ms.SetName("bytes_received")
	sum := ms.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	return &otlpMetrics{
		metrics: metrics,
		ms:      sum,
	}
}

func (om *otlpMetrics) addOTLPSum(timestamp time.Time, value float64, signal usageSignal) error {
	intVal, err := convertFloat64ToInt64(value)
	if err != nil {
		return err
	}
	d := om.ms.DataPoints().AppendEmpty()
	d.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	d.SetIntValue(intVal)
	d.Attributes().PutStr("signal", string(signal))
	return nil
}

func convertFloat64ToInt64(value float64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("value %f is too large to convert to int64", value)
	}
	if value < math.MinInt64 {
		return 0, fmt.Errorf("value %f is too small to convert to int64", value)
	}
	return int64(value), nil
}

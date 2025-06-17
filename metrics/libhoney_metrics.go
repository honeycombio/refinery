package metrics

import (
	"github.com/honeycombio/libhoney-go/transmission"
)

var _ transmission.Metrics = (*LibhoneyMetricsWrapper)(nil)

// LibhoneyMetricsWrapper is a wrapper around Refinery's Metrics and implements the
// libhoney-go transmission.Metrics interface.
type LibhoneyMetricsWrapper struct {
	*MetricsPrefixer
}

func (l *LibhoneyMetricsWrapper) Register(metadata Metadata) {
	l.Metrics.Register(metadata)
}

func (l *LibhoneyMetricsWrapper) Increment(name string) {
	l.Metrics.Increment(name)
}

func (l *LibhoneyMetricsWrapper) Gauge(name string, val interface{}) {
	f := ConvertNumeric(val)
	l.Metrics.Gauge(name, f)
}

func (l *LibhoneyMetricsWrapper) Count(name string, val interface{}) {
	f := ConvertNumeric(val)
	l.Metrics.Count(name, int64(f))
}

func (l *LibhoneyMetricsWrapper) Histogram(name string, obs interface{}) {
	f := ConvertNumeric(obs)
	l.Metrics.Histogram(name, f)
}

// ConvertNumeric converts various numeric types to float64.
// This is useful for converting values from libhoney-go since it expects
// the Gauge, Count, and Histogram methods to accept values with any type.
func ConvertNumeric(val interface{}) float64 {
	switch n := val.(type) {
	case int:
		return float64(n)
	case uint:
		return float64(n)
	case int64:
		return float64(n)
	case uint64:
		return float64(n)
	case int32:
		return float64(n)
	case uint32:
		return float64(n)
	case int16:
		return float64(n)
	case uint16:
		return float64(n)
	case int8:
		return float64(n)
	case uint8:
		return float64(n)
	case float64:
		return n
	case float32:
		return float64(n)
	case bool:
		if n {
			return 1
		}
	}
	return 0
}

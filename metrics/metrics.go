package metrics

import (
	"fmt"

	"github.com/honeycombio/refinery/config"
)

// All of the bits in the metrics package are designed to be pluggable. The
// Metrics interface defines the methods that are needed to support it, and all
// of the objects implement them. The general structure is that a MultiMetrics
// object is created, and then one or more of the child objects are added to it.
// The MultiMetrics object will then call the methods on all of the child
// objects. This allows us to support multiple metrics backends at once.
//
// The user can configure which metrics backends are enabled, and which are not.
// The StoreMetrics object is always enabled, and is the first child of the
// MultiMetrics object. This is because we always want StressRelief to be able
// to retrieve metrics data even if the user has not configured any other
// metrics backends.
//
// The MetricsNamer is a helper object that can be used to generate named
// metrics without creating multiple objects. This is so we don't have multiple
// sources writing to the backend at once.
//
// In operation, there is a single MultiMetrics object that is created and then
// named with multiple MetricsNamers. The user's configuration controls how many
// children the MultiMetrics object has, and which ones they are.
//
// The Metrics object supports "constants", which are just float values that can
// be attached to the metrics system. They do not need to be (and should not) be
// registered in advance; they are just a bucket of key-float pairs that can be
// used in combination with other metrics. This is mainly to support
// StressRelief.
type Metrics interface {
	// Register declares a metric; metricType should be one of counter, gauge, histogram, updown
	Register(name string, metricType string)
	Increment(name string)                  // for counters
	Gauge(name string, val interface{})     // for gauges
	Count(name string, n interface{})       // for counters
	Histogram(name string, obs interface{}) // for histogram
	Up(name string)                         // for updown
	Down(name string)                       // for updown
	Get(name string) (float64, bool)        // for reading back a counter or a gauge
	Store(name string, val float64)         // for storing a rarely-changing value not sent as a metric
}

func GetMetricsImplementation(c config.Config) *MultiMetrics {
	return NewMultiMetrics()
}

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
	default:
		return 0
	}
}

func PrefixMetricName(prefix string, name string) string {
	if prefix != "" {
		return fmt.Sprintf(`%s_%s`, prefix, name)
	}
	return name
}

package metrics

import (
	"fmt"
	"os"

	"github.com/honeycombio/refinery/config"
)

// The Metrics object supports "constants", which are just float values that can be attached to the
// metrics system. They do not need to be (and should not) be registered in advance; they are just
// a bucket of key-float pairs that can be used in combination with other metrics.
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

func GetMetricsImplementation(c config.Config) Metrics {
	var metricsr Metrics
	metricsType, err := c.GetMetricsType()
	if err != nil {
		fmt.Printf("unable to get metrics type from config: %v\n", err)
		os.Exit(1)
	}
	switch metricsType {
	case "honeycomb":
		metricsr = &HoneycombMetrics{}
	case "prometheus":
		metricsr = &PromMetrics{}
	default:
		fmt.Printf("unknown metrics type %s. Exiting.\n", metricsType)
		os.Exit(1)
	}
	return metricsr
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

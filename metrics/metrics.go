package metrics

import (
	"fmt"
	"os"

	"github.com/honeycombio/malinois/config"
)

type Metrics interface {
	Register(name string, metricType string)
	IncrementCounter(name string)
	Gauge(name string, val float64)
	Histogram(name string, obs float64)
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

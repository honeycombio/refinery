package agent

import (
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

var errNoData = errors.New("no data to report")

// usageTracker is a store for usage data. It keeps track of the last usage data and the delta between each update.
type usageTracker struct {
	// lastUsageData is a map of the last cumulative usage data for each signal.
	lastUsageData map[usageSignal]usage

	mut        sync.Mutex
	datapoints []usage
}

func newUsageTracker() *usageTracker {
	return &usageTracker{
		lastUsageData: make(map[usageSignal]usage),
		datapoints:    make([]usage, 0),
	}
}

// Add records the current cumulative usage and calculates the delta between the last update.
func (ur *usageTracker) Add(data usage) {
	ur.mut.Lock()
	defer ur.mut.Unlock()

	if data.val == 0 {
		return
	}

	deltaTraceUsage := data.val - ur.lastUsageData[data.signal].val
	ur.datapoints = append(ur.datapoints, usage{signal: data.signal, val: deltaTraceUsage, timestamp: data.timestamp})
	ur.lastUsageData[data.signal] = usage{signal: data.signal, val: data.val}
}

// NewReport creates a new usage report with the current delta usage data.
func (ur *usageTracker) NewReport(serviceName, version, hostname string) ([]byte, error) {
	ur.mut.Lock()
	defer ur.mut.Unlock()

	if len(ur.datapoints) == 0 {
		return nil, errNoData
	}

	otlpMetrics := newOTLPMetrics(serviceName, version, hostname)
	for _, usage := range ur.datapoints {
		err := otlpMetrics.addOTLPSum(usage.timestamp, usage.val, usage.signal)
		if err != nil {
			return nil, err
		}
	}

	jsonMarshaler := &pmetric.JSONMarshaler{}
	data, err := jsonMarshaler.MarshalMetrics(otlpMetrics.metrics)
	if err != nil {
		return nil, err
	}
	// clear datapoints after reporting
	ur.datapoints = ur.datapoints[:0]
	return data, nil
}

type usageSignal string

var (
	signal_traces usageSignal = "traces"
	signal_logs   usageSignal = "logs"
)

type usage struct {
	signal    usageSignal
	val       float64
	timestamp time.Time
}

func newTraceCumulativeUsage(value float64, timestamp time.Time) usage {
	return usage{
		signal:    signal_traces,
		val:       value,
		timestamp: timestamp,
	}
}

func newLogCumulativeUsage(value float64, timestamp time.Time) usage {
	return usage{
		signal:    signal_logs,
		val:       value,
		timestamp: timestamp,
	}
}

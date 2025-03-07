package agent

import (
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

var (
	errNoData     = errors.New("no data to report")
	jsonMarshaler = &pmetric.JSONMarshaler{}
)

// usageTracker is a store for usage data. It keeps track of the last usage data and the delta between each update.
type usageTracker struct {
	// lastUsageData is a map of the last cumulative usage data for each signal.
	lastUsageData map[usageSignal]float64

	mut               sync.Mutex
	lastDataPoints    map[usageSignal]float64
	currentDataPoints map[usageSignal]float64
}

func newUsageTracker() *usageTracker {
	return &usageTracker{
		lastUsageData:     make(map[usageSignal]float64),
		currentDataPoints: make(map[usageSignal]float64),
		lastDataPoints:    make(map[usageSignal]float64),
	}
}

// Add records the current cumulative usage and calculates the delta between the last update.
func (ur *usageTracker) Add(signal usageSignal, data float64) {
	ur.mut.Lock()
	defer ur.mut.Unlock()

	if data == 0 {
		return
	}

	deltaTraceUsage := data - ur.lastUsageData[signal]
	ur.currentDataPoints[signal] += deltaTraceUsage
	ur.lastUsageData[signal] = data
}

// NewReport creates a new usage report with the current delta usage data.
func (ur *usageTracker) NewReport(serviceName, version, hostname string, now time.Time) ([]byte, error) {
	ur.mut.Lock()
	defer ur.mut.Unlock()

	if len(ur.currentDataPoints) == 0 && len(ur.lastDataPoints) == 0 {
		return nil, errNoData
	}

	otlpMetrics := newOTLPMetrics(serviceName, version, hostname)
	for signal, usage := range ur.currentDataPoints {
		err := otlpMetrics.addOTLPSum(now, usage, signal)
		if err != nil {
			return nil, err
		}
	}

	for signal, usage := range ur.lastDataPoints {
		err := otlpMetrics.addOTLPSum(now, usage, signal)
		if err != nil {
			return nil, err
		}
	}

	data, err := jsonMarshaler.MarshalMetrics(otlpMetrics.metrics)
	if err != nil {
		return nil, err
	}
	// clear the current data points and keep the last data points until we know the report was sent
	ur.lastDataPoints = ur.currentDataPoints
	ur.currentDataPoints = make(map[usageSignal]float64)
	return data, nil
}

// completeSend clears the last data points after the report is sent.
func (ur *usageTracker) completeSend() {
	ur.mut.Lock()
	defer ur.mut.Unlock()
	ur.lastDataPoints = make(map[usageSignal]float64)
}

type usageSignal string

var (
	signal_traces usageSignal = "traces"
	signal_logs   usageSignal = "logs"
)

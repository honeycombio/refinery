package agent

import (
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

var errNoData = errors.New("no data to report")

type usageStore struct {
	lastUsageData totalUsage

	mut        sync.Mutex
	datapoints []usage
}

func newUsageStore() *usageStore {
	return &usageStore{
		lastUsageData: make(totalUsage),
		datapoints:    make([]usage, 0),
	}
}

func (ur *usageStore) Add(traceUsage, logUsage float64) {
	ur.mut.Lock()
	defer ur.mut.Unlock()

	if traceUsage != 0 {
		deltaTraceUsage := traceUsage - ur.lastUsageData[signal_trace].val
		ur.datapoints = append(ur.datapoints, usage{signal: signal_trace, val: deltaTraceUsage, timestamp: time.Now()})
		ur.lastUsageData[signal_trace] = usage{signal: signal_trace, val: traceUsage}
	}
	if logUsage != 0 {
		deltaLogUsage := logUsage - ur.lastUsageData[signal_log].val
		ur.datapoints = append(ur.datapoints, usage{signal: signal_log, val: deltaLogUsage, timestamp: time.Now()})
		ur.lastUsageData[signal_log] = usage{signal: signal_log, val: logUsage}
	}

}

func (ur *usageStore) NewReport() ([]byte, error) {
	ur.mut.Lock()
	defer ur.mut.Unlock()

	if len(ur.datapoints) == 0 {
		return nil, errNoData
	}

	otlpMetrics := newOTLPMetrics()
	for _, usage := range ur.datapoints {
		otlpMetrics.addOTLPSum(usage.timestamp, usage.val, usage.signal)
	}

	jsonMarshaler := &pmetric.JSONMarshaler{}
	data, err := jsonMarshaler.MarshalMetrics(otlpMetrics.metrics)
	if err != nil {
		return nil, err
	}
	ur.datapoints = ur.datapoints[:0]
	return data, nil
}

type usageSignal string

var (
	signal_trace usageSignal = "trace"
	signal_log   usageSignal = "log"
)

type totalUsage map[usageSignal]usage

type usage struct {
	signal    usageSignal
	val       float64
	timestamp time.Time
}
